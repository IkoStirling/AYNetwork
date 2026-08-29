// Durable Online Services restart, encryption, concurrency and account-token tests.

#include <AYNetwork/Session/InMemorySessionService.h>
#include <AYNetwork/Session/PlayerAccessToken.h>
#include <AYNetwork/Session/SqliteOnlineServices.h>
#include <AYTest.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <future>
#include <iterator>
#include <memory>
#include <string>
#include <vector>

using namespace ayt::net;

namespace
{

struct DurableOnlineFixture {
    DurableOnlineFixture() {
        const auto unique = std::chrono::steady_clock::now()
            .time_since_epoch().count();
        directory = std::filesystem::temp_directory_path() /
            ("aynetwork-online-db-" + std::to_string(unique));
        std::filesystem::create_directories(directory);
        database = directory / "online.sqlite3";
        for (size_t i = 0; i < storageKey.size(); ++i) {
            storageKey[i] = static_cast<uint8_t>(0x51u + i);
            accessKey[i] = static_cast<uint8_t>(0xa1u - i);
        }
    }

    ~DurableOnlineFixture() {
        std::error_code ignored;
        std::filesystem::remove_all(directory, ignored);
    }

    SqliteOnlineServicesConfig config(std::atomic<uint64_t>& now) const {
        SqliteOnlineServicesConfig value;
        value.databasePath = database.string();
        value.storageKey = storageKey;
        value.operationClaimSeconds = 3;
        value.matchTicketRetentionSeconds = 3;
        value.dedicatedLeaseSeconds = 5;
        value.allocationLifetimeSeconds = 10;
        value.nowUnixSeconds = [&now] { return now.load(); };
        return value;
    }

    std::shared_ptr<InMemoryP2PSessionService> p2p(
        std::atomic<uint64_t>& now) const {
        InMemoryP2PSessionServiceConfig value;
        value.publicSignalingAddress = "signal.example.test";
        value.signalingPort = 28080;
        value.nowUnixSeconds = [&now] { return now.load(); };
        return std::make_shared<InMemoryP2PSessionService>(std::move(value));
    }

    std::vector<uint8_t> storedBytes() const {
        std::vector<uint8_t> result;
        for (const auto& entry : std::filesystem::directory_iterator(directory)) {
            if (!entry.is_regular_file()) continue;
            std::ifstream input(entry.path(), std::ios::binary);
            result.insert(result.end(), std::istreambuf_iterator<char>(input), {});
        }
        return result;
    }

    std::filesystem::path directory;
    std::filesystem::path database;
    std::array<uint8_t, 32> storageKey{};
    std::array<uint8_t, 32> accessKey{};
};

CreateLobbyRequest durableLobbyRequest() {
    CreateLobbyRequest request;
    request.ownerPeerId = PeerId{"durable-owner"};
    request.name = "Durable Lobby";
    request.region = "asia";
    request.buildId = "build-1";
    request.content = {"maps/durable", "content-1", 99};
    request.capacity = 4;
    return request;
}

MatchmakingRequest durableMatchRequest(const char* peer) {
    MatchmakingRequest request;
    request.partyMembers = {PeerId{peer}};
    request.queue = "ranked";
    request.region = "asia";
    request.buildId = "build-1";
    request.content = {"maps/durable", "content-1", 99};
    request.topology = MatchTopology::P2P;
    request.targetPlayers = 2;
    request.virtualPort = 7350;
    return request;
}

DedicatedServerRegistration durableServerRequest() {
    DedicatedServerRegistration request;
    request.instanceName = "durable-server";
    request.region = "asia";
    request.buildId = "build-1";
    request.address = "203.0.113.42";
    request.port = 7200;
    request.capacity = 8;
    return request;
}

bool containsText(const std::vector<uint8_t>& bytes, const std::string& text) {
    return !text.empty() && std::search(
        bytes.begin(), bytes.end(), text.begin(), text.end()) != bytes.end();
}

} // namespace

TEST_SUITE(DurableOnlineServices)

TEST_CASE(RestartPreservesLobbyServerAllocationAndQueuedTicket) {
    ayt::test::setCurrentCase(
        "RestartPreservesLobbyServerAllocationAndQueuedTicket");
    DurableOnlineFixture fixture;
    std::atomic<uint64_t> now{30000};
    LobbyInfo joined;
    DedicatedServerGrant server;
    DedicatedAllocation allocation;
    MatchTicketInfo ticket;
    {
        SqliteOnlineServices service(fixture.config(now), fixture.p2p(now));
        CHECK(service.isReady());
        const auto created = service.createLobby(durableLobbyRequest());
        CHECK(created);
        const auto result = service.joinLobby(
            created.value.lobbyId, PeerId{"durable-guest"});
        CHECK(result);
        joined = result.value;
        const auto registered = service.registerServer(durableServerRequest());
        CHECK(registered);
        server = registered.value;
        const auto reserved = service.allocateServer({"asia", "build-1", 3});
        CHECK(reserved);
        allocation = reserved.value;
        const auto queued = service.enqueueMatch(
            durableMatchRequest("durable-owner"));
        CHECK(queued);
        ticket = queued.value;
    }

    now.store(30001);
    SqliteOnlineServices restarted(fixture.config(now), fixture.p2p(now));
    CHECK(restarted.isReady());
    const auto lobby = restarted.getLobby(joined.lobbyId);
    CHECK(lobby);
    CHECK_INT_EQ(lobby.value.revision, joined.revision);
    CHECK(lobby.value.members == joined.members);
    CHECK(lobby.value.content == durableLobbyRequest().content);
    CHECK(restarted.heartbeatServer(server.credential));
    CHECK(restarted.releaseAllocation(
        allocation.allocationId, allocation.reservationToken));
    const auto observed = restarted.getMatch(
        ticket.ticketId, PeerId{"durable-owner"});
    CHECK(observed);
    CHECK(observed.value.state == MatchTicketState::Queued);
    CHECK(observed.value.request.content ==
          durableMatchRequest("durable-owner").content);
}

TEST_CASE(MatchedAssignmentSurvivesRestartAndCredentialsStayEncrypted) {
    ayt::test::setCurrentCase(
        "MatchedAssignmentSurvivesRestartAndCredentialsStayEncrypted");
    DurableOnlineFixture fixture;
    std::atomic<uint64_t> now{31000};
    MatchTicketInfo firstResult;
    MatchTicketInfo secondResult;
    DedicatedServerGrant server;
    DedicatedAllocation allocation;
    const auto sessions = fixture.p2p(now);
    {
        SqliteOnlineServices service(fixture.config(now), sessions);
        CHECK(service.isReady());
        const auto first = service.enqueueMatch(
            durableMatchRequest("durable-a"));
        const auto second = service.enqueueMatch(
            durableMatchRequest("durable-b"));
        CHECK(first && second);
        const size_t matched = service.runMatchmaking(1);
        if (matched != 1) {
            std::fprintf(stderr, "durable matchmaking failed: %s\n",
                         service.getLastError().c_str());
        }
        CHECK_INT_EQ(matched, 1);
        const auto firstMatch = service.getMatch(
            first.value.ticketId, PeerId{"durable-a"});
        const auto secondMatch = service.getMatch(
            second.value.ticketId, PeerId{"durable-b"});
        CHECK(firstMatch && secondMatch);
        CHECK(firstMatch.value.assignment.content ==
              durableMatchRequest("durable-a").content);
        firstResult = firstMatch.value;
        secondResult = secondMatch.value;
        const auto registered = service.registerServer(durableServerRequest());
        CHECK(registered);
        server = registered.value;
        const auto reserved = service.allocateServer({"asia", "build-1", 2});
        CHECK(reserved);
        allocation = reserved.value;
    }

    CHECK(firstResult.state == MatchTicketState::Matched);
    CHECK(secondResult.state == MatchTicketState::Matched);
    CHECK_INT_EQ(firstResult.assignment.p2pGrants.size(), 1);
    CHECK_INT_EQ(secondResult.assignment.p2pGrants.size(), 1);
    if (firstResult.assignment.p2pGrants.size() != 1 ||
        secondResult.assignment.p2pGrants.size() != 1) return;
    CHECK(firstResult.assignment.p2pGrants.front().member.peerId ==
          PeerId{"durable-a"});
    CHECK(secondResult.assignment.p2pGrants.front().member.peerId ==
          PeerId{"durable-b"});
    CHECK(firstResult.assignment.p2pGrants.front().session.sessionId ==
          secondResult.assignment.p2pGrants.front().session.sessionId);

    now.store(31001);
    {
        SqliteOnlineServices restarted(fixture.config(now), sessions);
        CHECK(restarted.isReady());
        const auto first = restarted.getMatch(
            firstResult.ticketId, PeerId{"durable-a"});
        const auto second = restarted.getMatch(
            secondResult.ticketId, PeerId{"durable-b"});
        CHECK(first && second);
        CHECK(first.value.assignment.p2pGrants.front().member.token ==
              firstResult.assignment.p2pGrants.front().member.token);
        CHECK(second.value.assignment.p2pGrants.front().signalingToken ==
              secondResult.assignment.p2pGrants.front().signalingToken);
    }

    const auto bytes = fixture.storedBytes();
    CHECK(!containsText(bytes, server.credential.token));
    CHECK(!containsText(bytes, allocation.reservationToken));
    CHECK(!containsText(bytes,
        firstResult.assignment.p2pGrants.front().member.token));
    CHECK(!containsText(bytes,
        secondResult.assignment.p2pGrants.front().signalingToken));
}

TEST_CASE(WrongStorageKeyRefusesExistingOnlineDatabase) {
    ayt::test::setCurrentCase(
        "WrongStorageKeyRefusesExistingOnlineDatabase");
    DurableOnlineFixture fixture;
    std::atomic<uint64_t> now{32000};
    {
        SqliteOnlineServices service(fixture.config(now));
        CHECK(service.isReady());
        CHECK(service.createLobby(durableLobbyRequest()));
    }
    auto wrong = fixture.config(now);
    wrong.storageKey[0] ^= 0xffu;
    SqliteOnlineServices rejected(std::move(wrong));
    CHECK(!rejected.isReady());
    CHECK(rejected.getLastError().find("key mismatch") != std::string::npos);
}

TEST_CASE(RestartRecoversExpiredServerAndCompletedTicketRetention) {
    ayt::test::setCurrentCase(
        "RestartRecoversExpiredServerAndCompletedTicketRetention");
    DurableOnlineFixture fixture;
    std::atomic<uint64_t> now{33000};
    MatchTicketId cancelledId = 0;
    {
        SqliteOnlineServices service(fixture.config(now));
        CHECK(service.isReady());
        CHECK(service.registerServer(durableServerRequest()));
        const auto queued = service.enqueueMatch(
            durableMatchRequest("durable-owner"));
        CHECK(queued);
        cancelledId = queued.value.ticketId;
        CHECK(service.cancelMatch(cancelledId, PeerId{"durable-owner"}));
    }

    now.store(33006);
    SqliteOnlineServices restarted(fixture.config(now));
    CHECK(restarted.isReady());
    const auto servers = restarted.listServers();
    CHECK(servers);
    CHECK(servers.value.empty());
    CHECK(restarted.getMatch(cancelledId, PeerId{"durable-owner"}).error ==
          OnlineServiceError::NotFound);
}

TEST_CASE(TwoInstancesCommitExactlyOneMatchBatch) {
    ayt::test::setCurrentCase("TwoInstancesCommitExactlyOneMatchBatch");
    DurableOnlineFixture fixture;
    std::atomic<uint64_t> now{34000};
    const auto sessions = fixture.p2p(now);
    SqliteOnlineServices first(fixture.config(now), sessions);
    SqliteOnlineServices second(fixture.config(now), sessions);
    CHECK(first.isReady() && second.isReady());
    const auto ticketA = first.enqueueMatch(durableMatchRequest("durable-a"));
    const auto ticketB = first.enqueueMatch(durableMatchRequest("durable-b"));
    CHECK(ticketA && ticketB);

    auto futureA = std::async(std::launch::async,
        [&] { return first.runMatchmaking(1); });
    auto futureB = std::async(std::launch::async,
        [&] { return second.runMatchmaking(1); });
    const size_t processed = futureA.get() + futureB.get();
    if (processed != 1) {
        std::fprintf(stderr, "concurrent durable matchmaking failed: A=%s B=%s\n",
                     first.getLastError().c_str(),
                     second.getLastError().c_str());
    }
    CHECK_INT_EQ(processed, 1);
    const auto resultA = first.getMatch(
        ticketA.value.ticketId, PeerId{"durable-a"});
    const auto resultB = second.getMatch(
        ticketB.value.ticketId, PeerId{"durable-b"});
    CHECK(resultA && resultB);
    CHECK(resultA.value.state == MatchTicketState::Matched);
    CHECK(resultB.value.state == MatchTicketState::Matched);
    if (resultA.value.assignment.p2pGrants.empty() ||
        resultB.value.assignment.p2pGrants.empty()) return;
    CHECK(resultA.value.assignment.p2pGrants.front().session.sessionId ==
          resultB.value.assignment.p2pGrants.front().session.sessionId);
}

TEST_CASE(TwoInstancesCommitExactlyOneLobbyLaunch) {
    ayt::test::setCurrentCase("TwoInstancesCommitExactlyOneLobbyLaunch");
    DurableOnlineFixture fixture;
    std::atomic<uint64_t> now{34500};
    const auto sessions = fixture.p2p(now);
    SqliteOnlineServices first(fixture.config(now), sessions);
    SqliteOnlineServices second(fixture.config(now), sessions);
    CHECK(first.isReady() && second.isReady());
    const auto lobby = first.createLobby(durableLobbyRequest());
    CHECK(lobby);
    LaunchLobbyRequest launch;
    launch.lobbyId = lobby.value.lobbyId;
    launch.actorPeerId = lobby.value.ownerPeerId;
    launch.expectedRevision = lobby.value.revision;
    launch.virtualPort = 7350;

    auto futureA = std::async(std::launch::async,
        [&] { return first.launchLobbyP2P(launch); });
    auto futureB = std::async(std::launch::async,
        [&] { return second.launchLobbyP2P(launch); });
    const auto resultA = futureA.get();
    const auto resultB = futureB.get();
    if (static_cast<bool>(resultA) == static_cast<bool>(resultB)) {
        std::fprintf(stderr,
            "concurrent lobby launch: A=%u/%s B=%u/%s dbA=%s dbB=%s\n",
            static_cast<unsigned>(resultA.error), resultA.message.c_str(),
            static_cast<unsigned>(resultB.error), resultB.message.c_str(),
            first.getLastError().c_str(), second.getLastError().c_str());
    }
    CHECK(static_cast<bool>(resultA) != static_cast<bool>(resultB));
    CHECK((resultA.error == OnlineServiceError::Conflict) !=
          (resultB.error == OnlineServiceError::Conflict));
    const auto canonical = first.getLobby(lobby.value.lobbyId);
    CHECK(canonical);
    CHECK(canonical.value.state == LobbyState::InSession);
    CHECK(canonical.value.sessionId != 0);
}

TEST_CASE(TwoInstancesCannotOverbookDedicatedCapacity) {
    ayt::test::setCurrentCase("TwoInstancesCannotOverbookDedicatedCapacity");
    DurableOnlineFixture fixture;
    std::atomic<uint64_t> now{34700};
    SqliteOnlineServices first(fixture.config(now));
    SqliteOnlineServices second(fixture.config(now));
    CHECK(first.isReady() && second.isReady());
    auto registration = durableServerRequest();
    registration.capacity = 4;
    CHECK(first.registerServer(registration));

    auto futureA = std::async(std::launch::async,
        [&] { return first.allocateServer({"asia", "build-1", 3}); });
    auto futureB = std::async(std::launch::async,
        [&] { return second.allocateServer({"asia", "build-1", 3}); });
    const auto resultA = futureA.get();
    const auto resultB = futureB.get();
    CHECK(static_cast<bool>(resultA) != static_cast<bool>(resultB));
    CHECK((resultA.error == OnlineServiceError::NoCapacity) !=
          (resultB.error == OnlineServiceError::NoCapacity));
    const auto servers = first.listServers();
    CHECK(servers);
    CHECK_INT_EQ(servers.value.size(), 1);
    CHECK_INT_EQ(servers.value.front().reservedPlayers, 3);
}

TEST_CASE(PlayerAccessTokensVerifyProviderNeutralIdentityAndTimeBounds) {
    ayt::test::setCurrentCase(
        "PlayerAccessTokensVerifyProviderNeutralIdentityAndTimeBounds");
    DurableOnlineFixture fixture;
    std::atomic<uint64_t> now{35000};
    PlayerAccessTokenVerifierConfig config;
    config.signingKey = fixture.accessKey;
    config.maximumLifetimeSeconds = 60;
    config.clockSkewSeconds = 0;
    config.nowUnixSeconds = [&now] { return now.load(); };
    PlayerAccessTokenVerifier verifier(config);
    CHECK(verifier.isReady());

    std::string token;
    CHECK(issuePlayerAccessToken(
        PeerId{"account:independent-player"}, 35000, 35030,
        fixture.accessKey, token));
    PeerId peer;
    CHECK(verifier.verify(token, peer) == PlayerAccessTokenError::None);
    CHECK(peer == PeerId{"account:independent-player"});

    std::string tampered = token;
    const size_t signatureStart = tampered.rfind('.') + 1;
    tampered[signatureStart] = tampered[signatureStart] == 'A' ? 'B' : 'A';
    CHECK(verifier.verify(tampered, peer) ==
          PlayerAccessTokenError::InvalidSignature);
    CHECK(!peer.isValid());

    now.store(35031);
    CHECK(verifier.verify(token, peer) == PlayerAccessTokenError::Expired);
    std::string excessiveLifetime;
    CHECK(issuePlayerAccessToken(
        PeerId{"account:independent-player"}, 35000, 35100,
        fixture.accessKey, excessiveLifetime));
    CHECK(verifier.verify(excessiveLifetime, peer) ==
          PlayerAccessTokenError::LifetimeExceeded);

    auto wrongConfig = config;
    wrongConfig.signingKey[0] ^= 0xffu;
    PlayerAccessTokenVerifier wrongKey(std::move(wrongConfig));
    CHECK(wrongKey.verify(token, peer) ==
          PlayerAccessTokenError::InvalidSignature);
}

TEST_SUITE_END
