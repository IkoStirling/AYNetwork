// Lobby, matchmaking and dedicated-server reference backend coverage.

#include <AYNetwork/Session/HttpOnlineServices.h>
#include <AYNetwork/Session/HttpSessionService.h>
#include <AYNetwork/Session/InMemoryOnlineServices.h>
#include <AYNetwork/Session/InMemorySessionService.h>
#include <AYTest.h>

using namespace ayt::net;

namespace
{

std::shared_ptr<InMemoryP2PSessionService> p2pBackend(uint64_t& now) {
    InMemoryP2PSessionServiceConfig config;
    config.publicSignalingAddress = "signal.example.test";
    config.signalingPort = 28080;
    config.nowUnixSeconds = [&now] { return now; };
    return std::make_shared<InMemoryP2PSessionService>(std::move(config));
}

CreateLobbyRequest lobbyRequest() {
    CreateLobbyRequest request;
    request.ownerPeerId = PeerId{"owner"};
    request.name = "Public Lobby";
    request.region = "asia";
    request.buildId = "build-1";
    request.content = {"maps/test", "content-1", 42};
    request.capacity = 3;
    return request;
}

MatchmakingRequest matchRequest(std::vector<PeerId> party,
                                MatchTopology topology = MatchTopology::P2P) {
    MatchmakingRequest request;
    request.partyMembers = std::move(party);
    request.queue = "ranked";
    request.region = "asia";
    request.buildId = "build-1";
    request.content = {"maps/test", "content-1", 42};
    request.topology = topology;
    request.targetPlayers = 3;
    request.virtualPort = 7350;
    return request;
}

} // namespace

TEST_SUITE(OnlineServices)

TEST_CASE(LobbyLifecycleUsesRevisionCASAndLaunchesP2PSession) {
    ayt::test::setCurrentCase("LobbyLifecycleUsesRevisionCASAndLaunchesP2PSession");
    uint64_t now = 1000;
    auto sessions = p2pBackend(now);
    InMemoryOnlineServices online({}, sessions);

    const auto created = online.createLobby(lobbyRequest());
    CHECK(created);
    CHECK(created.value.isValid());
    const auto joined = online.joinLobby(created.value.lobbyId, PeerId{"guest"});
    CHECK(joined);
    CHECK_INT_EQ(joined.value.members.size(), 2);

    UpdateLobbyRequest stale;
    stale.lobbyId = created.value.lobbyId;
    stale.actorPeerId = PeerId{"owner"};
    stale.expectedRevision = created.value.revision;
    stale.name = "stale";
    CHECK(online.updateLobby(stale).error == OnlineServiceError::Conflict);

    LaunchLobbyRequest launch;
    launch.lobbyId = created.value.lobbyId;
    launch.actorPeerId = PeerId{"owner"};
    launch.expectedRevision = joined.value.revision;
    launch.virtualPort = 7350;
    const auto launched = online.launchLobbyP2P(launch);
    CHECK(launched);
    CHECK(launched.value.lobby.state == LobbyState::InSession);
    CHECK(launched.value.lobby.sessionId != 0);
    CHECK_INT_EQ(launched.value.memberGrants.size(), 2);
    CHECK(launched.value.memberGrants[0].session.sessionId ==
          launched.value.memberGrants[1].session.sessionId);
    CHECK(online.joinLobby(created.value.lobbyId, PeerId{"late"}).error ==
          OnlineServiceError::Closed);
}

TEST_CASE(LobbyOwnerTransfersAndFiltersAreDeterministic) {
    ayt::test::setCurrentCase("LobbyOwnerTransfersAndFiltersAreDeterministic");
    InMemoryOnlineServices online({});
    const auto created = online.createLobby(lobbyRequest());
    CHECK(created);
    CHECK(online.joinLobby(created.value.lobbyId, PeerId{"next-owner"}));
    const auto left = online.leaveLobby(created.value.lobbyId, PeerId{"owner"});
    CHECK(left);
    CHECK(left.value.ownerPeerId == PeerId{"next-owner"});

    ListLobbiesRequest filter;
    filter.region = "asia";
    filter.buildId = "build-1";
    const auto listed = online.listLobbies(filter);
    CHECK(listed);
    CHECK_INT_EQ(listed.value.size(), 1);
    filter.region = "europe";
    CHECK(online.listLobbies(filter).value.empty());
}

TEST_CASE(DedicatedServerLeaseDrainAndAllocationCapacity) {
    ayt::test::setCurrentCase("DedicatedServerLeaseDrainAndAllocationCapacity");
    uint64_t now = 2000;
    InMemoryOnlineServicesConfig config;
    config.dedicatedLeaseSeconds = 10;
    config.allocationLifetimeSeconds = 20;
    config.nowUnixSeconds = [&now] { return now; };
    InMemoryOnlineServices online(config);

    DedicatedServerRegistration registration;
    registration.instanceName = "server-a";
    registration.region = "asia";
    registration.buildId = "build-1";
    registration.address = "203.0.113.10";
    registration.port = 7000;
    registration.capacity = 4;
    const auto server = online.registerServer(registration);
    CHECK(server);
    const auto allocation = online.allocateServer({"asia", "build-1", 3});
    CHECK(allocation);
    CHECK(allocation.value.isValid());
    CHECK(online.allocateServer({"asia", "build-1", 2}).error ==
          OnlineServiceError::NoCapacity);

    CHECK(online.setServerDraining(server.value.credential, true));
    CHECK(online.allocateServer({"asia", "build-1", 1}).error ==
          OnlineServiceError::NoCapacity);
    CHECK(online.unregisterServer(server.value.credential).error ==
          OnlineServiceError::Conflict);
    CHECK(online.releaseAllocation(allocation.value.allocationId,
                                   allocation.value.reservationToken));
    CHECK(online.unregisterServer(server.value.credential));
    CHECK(online.listServers().value.empty());
}

TEST_CASE(DedicatedServerLeaseExpiryFreesDirectoryEntry) {
    ayt::test::setCurrentCase("DedicatedServerLeaseExpiryFreesDirectoryEntry");
    uint64_t now = 3000;
    InMemoryOnlineServicesConfig config;
    config.dedicatedLeaseSeconds = 5;
    config.nowUnixSeconds = [&now] { return now; };
    InMemoryOnlineServices online(config);
    const auto server = online.registerServer(
        {"server-a", "asia", "build-1", "203.0.113.10", 7000, 4});
    CHECK(server);
    now = 3005;
    CHECK(online.listServers().value.empty());
    CHECK(online.heartbeatServer(server.value.credential).error ==
          OnlineServiceError::NotFound);
}

TEST_CASE(MatchmakingGroupsFIFOAndReturnsOnlyPartyP2PGrants) {
    ayt::test::setCurrentCase("MatchmakingGroupsFIFOAndReturnsOnlyPartyP2PGrants");
    uint64_t now = 4000;
    InMemoryOnlineServices online({}, p2pBackend(now));
    const auto solo = online.enqueueMatch(matchRequest({PeerId{"solo"}}));
    const auto party = online.enqueueMatch(matchRequest(
        {PeerId{"party-a"}, PeerId{"party-b"}}));
    CHECK(solo && party);
    CHECK_INT_EQ(online.runMatchmaking(1), 1);

    const auto soloResult = online.getMatch(solo.value.ticketId, PeerId{"solo"});
    const auto partyResult = online.getMatch(party.value.ticketId, PeerId{"party-a"});
    CHECK(soloResult && partyResult);
    CHECK(soloResult.value.state == MatchTicketState::Matched);
    CHECK_INT_EQ(soloResult.value.assignment.p2pGrants.size(), 1);
    CHECK_INT_EQ(partyResult.value.assignment.p2pGrants.size(), 2);
    CHECK(soloResult.value.assignment.p2pGrants[0].session.sessionId ==
          partyResult.value.assignment.p2pGrants[0].session.sessionId);
    CHECK(online.getMatch(solo.value.ticketId, PeerId{"stranger"}).error ==
          OnlineServiceError::Unauthorized);
}

TEST_CASE(MatchmakingDoesNotMixDifferentContentDescriptors) {
    ayt::test::setCurrentCase(
        "MatchmakingDoesNotMixDifferentContentDescriptors");
    uint64_t now = 4050;
    InMemoryOnlineServices online({}, p2pBackend(now));
    auto firstRequest = matchRequest({PeerId{"first"}});
    firstRequest.targetPlayers = 2;
    auto incompatibleRequest = matchRequest({PeerId{"incompatible"}});
    incompatibleRequest.targetPlayers = 2;
    incompatibleRequest.content.contentSeed = 43;
    const auto first = online.enqueueMatch(firstRequest);
    const auto incompatible = online.enqueueMatch(incompatibleRequest);
    CHECK(first && incompatible);
    CHECK_INT_EQ(online.runMatchmaking(1), 0);

    auto compatibleRequest = firstRequest;
    compatibleRequest.partyMembers = {PeerId{"compatible"}};
    const auto compatible = online.enqueueMatch(compatibleRequest);
    CHECK(compatible);
    CHECK_INT_EQ(online.runMatchmaking(1), 1);
    const auto result = online.getMatch(
        first.value.ticketId, PeerId{"first"});
    CHECK(result);
    CHECK(result.value.assignment.content == firstRequest.content);
    CHECK(online.getMatch(
        incompatible.value.ticketId, PeerId{"incompatible"}).value.state ==
        MatchTicketState::Queued);
}

TEST_CASE(MatchmakingAnyPrefersDedicatedAndDedicatedOnlyWaitsForCapacity) {
    ayt::test::setCurrentCase(
        "MatchmakingAnyPrefersDedicatedAndDedicatedOnlyWaitsForCapacity");
    InMemoryOnlineServices online({});
    auto firstRequest = matchRequest({PeerId{"a"}}, MatchTopology::Dedicated);
    firstRequest.targetPlayers = 2;
    auto secondRequest = firstRequest;
    secondRequest.partyMembers = {PeerId{"b"}};
    const auto first = online.enqueueMatch(firstRequest);
    const auto second = online.enqueueMatch(secondRequest);
    CHECK(first && second);
    CHECK_INT_EQ(online.runMatchmaking(1), 0);
    CHECK(online.getMatch(first.value.ticketId, PeerId{"a"}).value.state ==
          MatchTicketState::Queued);

    CHECK(online.registerServer(
        {"server-a", "asia", "build-1", "203.0.113.10", 7000, 8}));
    CHECK_INT_EQ(online.runMatchmaking(1), 1);
    const auto matched = online.getMatch(first.value.ticketId, PeerId{"a"});
    CHECK(matched.value.state == MatchTicketState::Matched);
    CHECK(matched.value.assignment.topology == MatchTopology::Dedicated);
    CHECK(matched.value.assignment.dedicated.isValid());
}

TEST_CASE(OnlineServicesEnforceResourceCapsAndRateLimitConfiguration) {
    ayt::test::setCurrentCase(
        "OnlineServicesEnforceResourceCapsAndRateLimitConfiguration");
    InMemoryOnlineServicesConfig limits;
    limits.maxLobbyCapacity = 2;
    limits.maxMatchPlayers = 2;
    limits.maxDedicatedServerCapacity = 4;
    InMemoryOnlineServices online(limits);

    auto oversizedLobby = lobbyRequest();
    oversizedLobby.capacity = 3;
    CHECK(online.createLobby(oversizedLobby).error ==
          OnlineServiceError::InvalidRequest);

    auto oversizedMatch = matchRequest({PeerId{"owner"}});
    oversizedMatch.targetPlayers = 3;
    CHECK(online.enqueueMatch(oversizedMatch).error ==
          OnlineServiceError::InvalidRequest);

    CHECK(online.registerServer(
        {"server-a", "asia", "build-1", "203.0.113.10", 7000, 5}).error ==
          OnlineServiceError::InvalidRequest);
    CHECK(online.allocateServer({"asia", "build-1", 3}).error ==
          OnlineServiceError::InvalidRequest);

    uint64_t now = 4500;
    HttpP2PSessionServerConfig serverConfig;
    serverConfig.bindAddress = "127.0.0.1";
    serverConfig.rateLimitRequestsPerMinute = 1;
    serverConfig.rateLimitBurst = 1;
    serverConfig.rateLimitTrackedSources = 0;
    HttpP2PSessionServer server(std::move(serverConfig), p2pBackend(now));
    CHECK(!server.start());
}

TEST_CASE(HttpOnlineServicesDerivesIdentityAndCoversAllRouteFamilies) {
    ayt::test::setCurrentCase(
        "HttpOnlineServicesDerivesIdentityAndCoversAllRouteFamilies");
    uint64_t now = 5000;
    auto sessions = p2pBackend(now);
    auto online = std::make_shared<InMemoryOnlineServices>(
        InMemoryOnlineServicesConfig{}, sessions);

    HttpP2PSessionServerConfig serverConfig;
    serverConfig.bindAddress = "127.0.0.1";
    serverConfig.playerAuthenticator = [](std::string_view token, PeerId& peer) {
        if (token == "owner-access-token") peer = PeerId{"owner"};
        else if (token == "guest-access-token") peer = PeerId{"guest"};
        else return false;
        return true;
    };
    serverConfig.dedicatedControlAuthenticator = [](std::string_view token) {
        return token == "fleet-control-token";
    };
    HttpP2PSessionServer server(
        std::move(serverConfig), sessions, online, online, online);
    CHECK(server.start());

    auto clientFor = [&](const char* peer, const char* token) {
        HttpOnlineServicesClientConfig config;
        config.serverPort = server.getBoundPort();
        config.localPeerId = PeerId{peer};
        config.playerAccessToken = token;
        config.dedicatedControlToken = "fleet-control-token";
        return std::make_unique<HttpOnlineServices>(std::move(config));
    };
    auto owner = clientFor("owner", "owner-access-token");
    auto guest = clientFor("guest", "guest-access-token");
    auto denied = clientFor("owner", "wrong-token");

    CHECK(denied->createLobby(lobbyRequest()).error ==
          OnlineServiceError::Unauthorized);
    const auto created = owner->createLobby(lobbyRequest());
    CHECK(created);
    const auto joined = guest->joinLobby(created.value.lobbyId, PeerId{"guest"});
    CHECK(joined);
    CHECK_INT_EQ(owner->listLobbies({"asia", "build-1", 1, 10}).value.size(), 1);

    MatchmakingRequest forged = matchRequest(
        {PeerId{"owner"}, PeerId{"guest"}});
    forged.targetPlayers = 2;
    CHECK(owner->enqueueMatch(forged).error == OnlineServiceError::Unauthorized);

    LaunchLobbyRequest launch;
    launch.lobbyId = created.value.lobbyId;
    launch.actorPeerId = PeerId{"owner"};
    launch.expectedRevision = joined.value.revision;
    launch.virtualPort = 7350;
    const auto launched = owner->launchLobbyP2P(launch);
    CHECK(launched);
    CHECK_INT_EQ(launched.value.memberGrants.size(), 2);

    auto firstRequest = matchRequest({PeerId{"owner"}});
    firstRequest.targetPlayers = 2;
    auto secondRequest = matchRequest({PeerId{"guest"}});
    secondRequest.targetPlayers = 2;
    const auto first = owner->enqueueMatch(firstRequest);
    const auto second = guest->enqueueMatch(secondRequest);
    CHECK(first && second);
    CHECK_INT_EQ(owner->runMatchmaking(1), 1);
    CHECK(owner->getMatch(first.value.ticketId, PeerId{"owner"}).value.state ==
          MatchTicketState::Matched);
    CHECK(guest->getMatch(second.value.ticketId, PeerId{"guest"}).value.state ==
          MatchTicketState::Matched);

    DedicatedServerRegistration registration;
    registration.instanceName = "http-server-a";
    registration.region = "asia";
    registration.buildId = "build-1";
    registration.address = "203.0.113.20";
    registration.port = 7100;
    registration.capacity = 8;
    const auto dedicated = owner->registerServer(registration);
    CHECK(dedicated);
    CHECK_INT_EQ(owner->listServers().value.size(), 1);
    CHECK(owner->heartbeatServer(dedicated.value.credential));
    const auto allocation = owner->allocateServer({"asia", "build-1", 3});
    CHECK(allocation);
    CHECK(owner->releaseAllocation(allocation.value.allocationId,
                                   allocation.value.reservationToken));
    CHECK(owner->setServerDraining(dedicated.value.credential, true));
    CHECK(owner->unregisterServer(dedicated.value.credential));
    server.stop();
}

TEST_SUITE_END
