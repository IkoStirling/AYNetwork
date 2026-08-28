// Lobby, matchmaking and dedicated-server reference backend coverage.

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

TEST_SUITE_END
