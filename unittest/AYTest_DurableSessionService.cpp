// Durable SQLite authority, encrypted credential and multi-instance CAS tests.

#include <AYNetwork/Session/SqliteSessionService.h>
#include <AYTest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <future>
#include <iterator>
#include <string>
#include <vector>

using namespace ayt::net;

namespace
{

struct DurableFixture {
    DurableFixture() {
        const auto unique = std::chrono::steady_clock::now()
            .time_since_epoch().count();
        directory = std::filesystem::temp_directory_path() /
            ("aynetwork-session-db-" + std::to_string(unique));
        std::filesystem::create_directories(directory);
        database = directory / "sessions.sqlite3";
        CHECK(generateSessionTicketKeyPair(keys));
        for (size_t i = 0; i < storageKey.size(); ++i) {
            storageKey[i] = static_cast<uint8_t>(0x31u + i);
        }
    }

    ~DurableFixture() {
        std::error_code ignored;
        std::filesystem::remove_all(directory, ignored);
    }

    SqliteP2PSessionServiceConfig config(std::atomic<uint64_t>& now) const {
        SqliteP2PSessionServiceConfig value;
        value.databasePath = database.string();
        value.publicSignalingAddress = "127.0.0.1";
        value.signalingPort = 28080;
        value.hostLeaseSeconds = 10;
        value.joinTicketLifetimeSeconds = 60;
        value.signalingTokenLifetimeSeconds = 600;
        value.storageKey = storageKey;
        value.nowUnixSeconds = [&now] { return now.load(); };
        return value;
    }

    std::filesystem::path directory;
    std::filesystem::path database;
    SessionTicketKeyPair keys{};
    std::array<uint8_t, 32> storageKey{};
};

P2PSessionCreateRequest durableHostRequest() {
    P2PSessionCreateRequest request;
    request.hostPeerId = PeerId{"durable-host"};
    request.virtualPort = 7350;
    request.capacity = 4;
    return request;
}

int hexValue(char ch) {
    if (ch >= '0' && ch <= '9') return ch - '0';
    if (ch >= 'a' && ch <= 'f') return ch - 'a' + 10;
    if (ch >= 'A' && ch <= 'F') return ch - 'A' + 10;
    return -1;
}

std::vector<uint8_t> decodeHex(const std::string& text) {
    if ((text.size() & 1u) != 0) return {};
    std::vector<uint8_t> out(text.size() / 2);
    for (size_t i = 0; i < out.size(); ++i) {
        const int hi = hexValue(text[i * 2]);
        const int lo = hexValue(text[i * 2 + 1]);
        if (hi < 0 || lo < 0) return {};
        out[i] = static_cast<uint8_t>((hi << 4) | lo);
    }
    return out;
}

} // namespace

TEST_SUITE(DurableSessionService)

TEST_CASE(RestartPreservesSessionCredentialsAndTicketAuthority) {
    ayt::test::setCurrentCase(
        "RestartPreservesSessionCredentialsAndTicketAuthority");
    DurableFixture fixture;
    std::atomic<uint64_t> now{20000};
    P2PSessionGrant host;
    {
        SqliteP2PSessionService service(fixture.config(now), fixture.keys);
        CHECK(service.isReady());
        const auto created = service.createSession(durableHostRequest());
        CHECK(created);
        host = created.value;
    }

    now.store(20001);
    SqliteP2PSessionService restarted(fixture.config(now), fixture.keys);
    CHECK(restarted.isReady());
    const auto observed = restarted.getSession(host.session.sessionId);
    CHECK(observed);
    CHECK_INT_EQ(observed.value.epoch, 1);
    CHECK(observed.value.hostPeerId == host.member.peerId);

    P2PSessionHeartbeatRequest heartbeat;
    heartbeat.member = host.member;
    heartbeat.expectedEpoch = 1;
    CHECK(restarted.heartbeat(heartbeat));

    std::array<uint8_t, 32> signaling{};
    uint64_t signalingExpiry = 0;
    CHECK(restarted.resolveSignalingCredential(
        host.member.peerId, host.session.signalingRoom,
        signaling, signalingExpiry));
    CHECK(std::vector<uint8_t>(signaling.begin(), signaling.end()) ==
          decodeHex(host.signalingToken));

    SessionJoinTicketClaims claims;
    CHECK(verifySessionJoinTicket(
        host.joinTicket.data(), host.joinTicket.size(),
        restarted.getTicketPublicKey(), claims, now.load()) ==
        SessionTicketError::None);
}

TEST_CASE(CredentialPlaintextIsNotStoredInDatabase) {
    ayt::test::setCurrentCase("CredentialPlaintextIsNotStoredInDatabase");
    DurableFixture fixture;
    std::atomic<uint64_t> now{21000};
    P2PSessionGrant host;
    {
        SqliteP2PSessionService service(fixture.config(now), fixture.keys);
        CHECK(service.isReady());
        const auto created = service.createSession(durableHostRequest());
        CHECK(created);
        host = created.value;
    }
    std::ifstream input(fixture.database, std::ios::binary);
    const std::vector<uint8_t> fileBytes(
        std::istreambuf_iterator<char>(input), {});
    const auto memberToken = decodeHex(host.member.token);
    const auto signalingToken = decodeHex(host.signalingToken);
    CHECK(memberToken.size() == 32);
    CHECK(signalingToken.size() == 32);
    CHECK(std::search(fileBytes.begin(), fileBytes.end(),
                      memberToken.begin(), memberToken.end()) == fileBytes.end());
    CHECK(std::search(fileBytes.begin(), fileBytes.end(),
                      signalingToken.begin(), signalingToken.end()) ==
          fileBytes.end());
}

TEST_CASE(TwoServiceInstancesCommitExactlyOneExpiredLeaseClaim) {
    ayt::test::setCurrentCase(
        "TwoServiceInstancesCommitExactlyOneExpiredLeaseClaim");
    DurableFixture fixture;
    std::atomic<uint64_t> now{22000};
    SqliteP2PSessionService serviceA(fixture.config(now), fixture.keys);
    CHECK(serviceA.isReady());
    const auto host = serviceA.createSession(durableHostRequest());
    CHECK(host);
    const auto memberA = serviceA.joinSession(
        {host.value.session.sessionId, PeerId{"candidate-a"}});
    const auto memberB = serviceA.joinSession(
        {host.value.session.sessionId, PeerId{"candidate-b"}});
    CHECK(memberA && memberB);
    SqliteP2PSessionService serviceB(fixture.config(now), fixture.keys);
    CHECK(serviceB.isReady());
    now.store(22010);

    P2PSessionClaimHostRequest claimA;
    claimA.member = memberA.value.member;
    claimA.expectedEpoch = 1;
    claimA.newHostPeerId = memberA.value.member.peerId;
    P2PSessionClaimHostRequest claimB;
    claimB.member = memberB.value.member;
    claimB.expectedEpoch = 1;
    claimB.newHostPeerId = memberB.value.member.peerId;
    auto futureA = std::async(std::launch::async,
        [&] { return serviceA.claimHost(claimA); });
    auto futureB = std::async(std::launch::async,
        [&] { return serviceB.claimHost(claimB); });
    const auto resultA = futureA.get();
    const auto resultB = futureB.get();
    CHECK(static_cast<bool>(resultA) != static_cast<bool>(resultB));
    CHECK((resultA.error == SessionServiceError::EpochConflict) !=
          (resultB.error == SessionServiceError::EpochConflict));
    const auto canonical = serviceA.getSession(host.value.session.sessionId);
    CHECK(canonical);
    CHECK_INT_EQ(canonical.value.epoch, 2);
}

TEST_CASE(WrongStorageOrTicketKeyRefusesExistingDatabase) {
    ayt::test::setCurrentCase(
        "WrongStorageOrTicketKeyRefusesExistingDatabase");
    DurableFixture fixture;
    std::atomic<uint64_t> now{23000};
    {
        SqliteP2PSessionService service(fixture.config(now), fixture.keys);
        CHECK(service.isReady());
        CHECK(service.createSession(durableHostRequest()));
    }

    auto wrongStorage = fixture.config(now);
    wrongStorage.storageKey[0] ^= 0xffu;
    SqliteP2PSessionService wrongStorageService(wrongStorage, fixture.keys);
    CHECK(!wrongStorageService.isReady());

    SessionTicketKeyPair wrongTicket{};
    CHECK(generateSessionTicketKeyPair(wrongTicket));
    SqliteP2PSessionService wrongTicketService(fixture.config(now), wrongTicket);
    CHECK(!wrongTicketService.isReady());
}

TEST_SUITE_END
