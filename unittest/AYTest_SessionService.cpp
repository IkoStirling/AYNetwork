// Authority-session service, ticket, lease and backend bootstrap coverage.

#include <AYNetwork/Session/HttpSessionService.h>
#include <AYNetwork/Session/InMemorySessionService.h>
#include <AYNetwork/Session/P2PSessionBackend.h>
#include <AYNetwork/Session/P2PSessionLeaseKeeper.h>
#include <AYNetwork/Session/SessionTicket.h>
#include <AYNetwork/Signaling/SecureUdpSignaling.h>
#include <AYTest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <thread>
#include <vector>

using namespace ayt::net;

namespace
{

InMemoryP2PSessionServiceConfig configFor(uint64_t& now) {
    InMemoryP2PSessionServiceConfig config;
    config.publicSignalingAddress = "session.example.test";
    config.signalingPort = 28080;
    config.hostLeaseSeconds = 10;
    config.joinTicketLifetimeSeconds = 60;
    config.signalingTokenLifetimeSeconds = 600;
    config.nowUnixSeconds = [&now] { return now; };
    return config;
}

P2PSessionCreateRequest hostRequest(const char* peer = "host-a") {
    P2PSessionCreateRequest request;
    request.hostPeerId = PeerId{peer};
    request.virtualPort = 7350;
    request.capacity = 3;
    return request;
}

} // namespace

TEST_SUITE(P2PSessionService)

TEST_CASE(Ed25519JoinTicketRoundTripAndTamperRejection) {
    ayt::test::setCurrentCase("Ed25519JoinTicketRoundTripAndTamperRejection");
    SessionTicketKeyPair keys;
    CHECK(generateSessionTicketKeyPair(keys));
    CHECK(validateSessionTicketKeyPair(keys));
    SessionJoinTicketClaims claims;
    claims.sessionId = 0x1122334455667788ull;
    claims.epoch = 7;
    claims.peerId = PeerId{"peer-a"};
    claims.issuedAtUnixSeconds = 100;
    claims.expiresAtUnixSeconds = 160;
    for (size_t i = 0; i < claims.nonce.size(); ++i) {
        claims.nonce[i] = static_cast<uint8_t>(i + 1);
    }
    std::vector<uint8_t> ticket;
    CHECK(issueSessionJoinTicket(claims, keys.secretKey, ticket));
    CHECK(ticket.size() <= kP2PMaxJoinTicketBytes);

    SessionJoinTicketClaims decoded;
    CHECK(verifySessionJoinTicket(ticket.data(), ticket.size(), keys.publicKey,
                                  decoded, 120) == SessionTicketError::None);
    CHECK(decoded.sessionId == claims.sessionId);
    CHECK_INT_EQ(decoded.epoch, claims.epoch);
    CHECK(decoded.peerId == claims.peerId);
    CHECK(validateP2PSessionJoinTicket(
        keys.publicKey, claims.sessionId, claims.epoch, claims.peerId,
        ticket.data(), ticket.size(), 120).accepted);
    CHECK(!validateP2PSessionJoinTicket(
        keys.publicKey, claims.sessionId, claims.epoch + 1, claims.peerId,
        ticket.data(), ticket.size(), 120).accepted);
    CHECK(verifySessionJoinTicket(ticket.data(), ticket.size(), keys.publicKey,
                                  decoded, 160) == SessionTicketError::Expired);

    ticket[10] ^= 0x40;
    CHECK(verifySessionJoinTicket(ticket.data(), ticket.size(), keys.publicKey,
                                  decoded, 120) ==
          SessionTicketError::InvalidSignature);
}

TEST_CASE(MismatchedSuppliedTicketKeyPairIsRejected) {
    ayt::test::setCurrentCase("MismatchedSuppliedTicketKeyPairIsRejected");
    SessionTicketKeyPair first;
    SessionTicketKeyPair second;
    CHECK(generateSessionTicketKeyPair(first));
    CHECK(generateSessionTicketKeyPair(second));
    first.secretKey = second.secretKey;
    CHECK(!validateSessionTicketKeyPair(first));

    uint64_t now = 500;
    InMemoryP2PSessionService service(configFor(now), &first);
    CHECK(service.createSession(hostRequest()).error ==
          SessionServiceError::InvalidRequest);
}

TEST_CASE(TicketSigningIdentityPersistsAndRejectsCorruption) {
    ayt::test::setCurrentCase("TicketSigningIdentityPersistsAndRejectsCorruption");
    const auto path = std::filesystem::temp_directory_path() /
        ("aynetwork-ticket-" + std::to_string(
            std::chrono::steady_clock::now().time_since_epoch().count()) +
         ".key");
    SessionTicketKeyPair created;
    CHECK(loadOrCreateSessionTicketKeyPair(path.string(), created) ==
          SessionTicketKeyFileError::None);
    SessionTicketKeyPair loaded;
    CHECK(loadSessionTicketKeyPair(path.string(), loaded) ==
          SessionTicketKeyFileError::None);
    CHECK(created.publicKey == loaded.publicKey);
    CHECK(created.secretKey == loaded.secretKey);

    {
        std::ofstream corrupt(path, std::ios::binary | std::ios::app);
        corrupt.put('x');
    }
    CHECK(loadSessionTicketKeyPair(path.string(), loaded) ==
          SessionTicketKeyFileError::InvalidFormat);
    std::error_code ignored;
    std::filesystem::remove(path, ignored);
}

TEST_CASE(HttpAdmissionRateLimitAndSecretFreeAudit) {
    ayt::test::setCurrentCase("HttpAdmissionRateLimitAndSecretFreeAudit");
    uint64_t now = 7000;
    auto service = std::make_shared<InMemoryP2PSessionService>(configFor(now));
    std::mutex auditMutex;
    std::vector<HttpP2PSessionServerConfig::AuditEvent> audit;

    HttpP2PSessionServerConfig serverConfig;
    serverConfig.bindAddress = "127.0.0.1";
    serverConfig.requireAdmissionAuthentication = true;
    serverConfig.admissionAuthenticator = [](std::string_view token,
                                              const PeerId&) {
        return token == "test-admission-secret";
    };
    serverConfig.rateLimitRequestsPerMinute = 1;
    serverConfig.rateLimitBurst = 2;
    serverConfig.auditSink = [&](const auto& event) {
        std::lock_guard<std::mutex> lock(auditMutex);
        audit.push_back(event);
    };
    HttpP2PSessionServer server(std::move(serverConfig), service);
    CHECK(server.start());

    HttpP2PSessionClientConfig deniedConfig;
    deniedConfig.serverPort = server.getBoundPort();
    HttpP2PSessionService denied(std::move(deniedConfig));
    CHECK(denied.createSession(hostRequest()).error ==
          SessionServiceError::Unauthorized);

    HttpP2PSessionClientConfig admittedConfig;
    admittedConfig.serverPort = server.getBoundPort();
    admittedConfig.admissionToken = "test-admission-secret";
    HttpP2PSessionService admitted(std::move(admittedConfig));
    const auto created = admitted.createSession(hostRequest());
    CHECK(created);
    CHECK(admitted.getSession(created.value.session.sessionId).error ==
          SessionServiceError::RateLimited);
    server.stop();

    std::lock_guard<std::mutex> lock(auditMutex);
    CHECK(audit.size() >= 3);
    bool sawUnauthorized = false;
    bool sawRateLimit = false;
    for (const auto& event : audit) {
        sawUnauthorized = sawUnauthorized || event.status == 401;
        sawRateLimit = sawRateLimit || event.status == 429;
        CHECK(event.path.find("test-admission-secret") == std::string::npos);
    }
    CHECK(sawUnauthorized);
    CHECK(sawRateLimit);
}

TEST_CASE(CreateJoinHeartbeatAndSignalingCredentials) {
    ayt::test::setCurrentCase("CreateJoinHeartbeatAndSignalingCredentials");
    uint64_t now = 1000;
    InMemoryP2PSessionService service(configFor(now));
    const auto host = service.createSession(hostRequest());
    CHECK(host);
    CHECK(host.value.isValid());
    CHECK_INT_EQ(host.value.session.epoch, 1);
    CHECK(host.value.session.hostPeerId == PeerId{"host-a"});

    P2PSessionJoinRequest join;
    join.sessionId = host.value.session.sessionId;
    join.peerId = PeerId{"join-a"};
    const auto member = service.joinSession(join);
    CHECK(member);
    CHECK_INT_EQ(member.value.session.memberCount, 2);
    CHECK(member.value.member.token != host.value.member.token);
    CHECK(member.value.signalingToken != host.value.signalingToken);

    SignalingToken parsed;
    CHECK(parseSignalingTokenHex(member.value.signalingToken, parsed));
    std::array<uint8_t, 32> resolved{};
    uint64_t expires = 0;
    CHECK(service.resolveSignalingCredential(
        join.peerId, member.value.session.signalingRoom, resolved, expires));
    CHECK(resolved == parsed.bytes);
    CHECK(expires > now);

    now = 1005;
    P2PSessionHeartbeatRequest heartbeat;
    heartbeat.member = host.value.member;
    heartbeat.expectedEpoch = 1;
    const auto renewed = service.heartbeat(heartbeat);
    CHECK(renewed);
    CHECK(renewed.value.hostLeaseExpiresAtUnixSeconds == 1015);

    heartbeat.member = member.value.member;
    CHECK(service.heartbeat(heartbeat).error == SessionServiceError::Unauthorized);
}

TEST_CASE(HostTransferUsesAtomicEpochCompare) {
    ayt::test::setCurrentCase("HostTransferUsesAtomicEpochCompare");
    uint64_t now = 2000;
    InMemoryP2PSessionService service(configFor(now));
    const auto host = service.createSession(hostRequest());
    P2PSessionJoinRequest join{host.value.session.sessionId, PeerId{"join-a"}};
    const auto member = service.joinSession(join);
    CHECK(host && member);

    P2PSessionClaimHostRequest claim;
    claim.member = host.value.member;
    claim.expectedEpoch = 1;
    claim.newHostPeerId = member.value.member.peerId;
    const auto promoted = service.claimHost(claim);
    CHECK(promoted);
    CHECK_INT_EQ(promoted.value.epoch, 2);
    CHECK(promoted.value.hostPeerId == PeerId{"join-a"});

    const auto stale = service.claimHost(claim);
    CHECK(stale.error == SessionServiceError::EpochConflict);

    P2PSessionHeartbeatRequest oldHostHeartbeat;
    oldHostHeartbeat.member = host.value.member;
    oldHostHeartbeat.expectedEpoch = 2;
    CHECK(service.heartbeat(oldHostHeartbeat).error ==
          SessionServiceError::Unauthorized);
    oldHostHeartbeat.member = member.value.member;
    CHECK(service.heartbeat(oldHostHeartbeat));
}

TEST_CASE(ExpiredLeaseAllowsOnlyCandidateSelfClaim) {
    ayt::test::setCurrentCase("ExpiredLeaseAllowsOnlyCandidateSelfClaim");
    uint64_t now = 3000;
    InMemoryP2PSessionService service(configFor(now));
    const auto host = service.createSession(hostRequest());
    P2PSessionJoinRequest join{host.value.session.sessionId, PeerId{"join-a"}};
    const auto member = service.joinSession(join);
    CHECK(host && member);
    now = 3010;

    P2PSessionClaimHostRequest forbidden;
    forbidden.member = member.value.member;
    forbidden.expectedEpoch = 1;
    forbidden.newHostPeerId = host.value.member.peerId;
    CHECK(service.claimHost(forbidden).error == SessionServiceError::InvalidRequest);

    // Once the lease expires, the stale Host has no authority to select its
    // successor or tear down the room.
    forbidden.member = host.value.member;
    forbidden.newHostPeerId = member.value.member.peerId;
    CHECK(service.claimHost(forbidden).error == SessionServiceError::Unauthorized);
    P2PSessionLeaveRequest staleLeave;
    staleLeave.member = host.value.member;
    CHECK(service.leaveSession(staleLeave).error ==
          SessionServiceError::HostLeaseExpired);

    P2PSessionClaimHostRequest claim;
    claim.member = member.value.member;
    claim.expectedEpoch = 1;
    claim.newHostPeerId = member.value.member.peerId;
    const auto promoted = service.claimHost(claim);
    CHECK(promoted);
    CHECK_INT_EQ(promoted.value.epoch, 2);
}

TEST_CASE(GrantBootstrapsP2PAndAdmissionValidator) {
    ayt::test::setCurrentCase("GrantBootstrapsP2PAndAdmissionValidator");
    uint64_t now = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::system_clock::now().time_since_epoch()).count());
    InMemoryP2PSessionService service(configFor(now));
    const auto host = service.createSession(hostRequest());
    P2PSessionJoinRequest join{host.value.session.sessionId, PeerId{"join-a"}};
    const auto member = service.joinSession(join);
    CHECK(host && member);

    P2PConfig config;
    config.localPeerId = member.value.member.peerId;
    config.icePolicy = P2PIcePolicy::DirectOnly;
    SecureUdpSignalingClientConfig signaling;
    CHECK(applyP2PSessionGrant(member.value, config, signaling));
    CHECK(config.sessionId == host.value.session.sessionId);
    CHECK_INT_EQ(config.sessionEpoch, 1);
    CHECK_INT_EQ(config.virtualPort, 7350);
    CHECK(signaling.roomId.value == host.value.session.signalingRoom);

    // A member installs the verifier before it is ever promoted to Host.
    const auto validator = makeP2PSessionJoinValidator(member.value);
    CHECK(static_cast<bool>(validator));
    CHECK(validator(host.value.session.sessionId, host.value.session.epoch,
                    member.value.member.peerId,
                    member.value.joinTicket.data(),
                    member.value.joinTicket.size()).accepted);
    CHECK(!validator(host.value.session.sessionId, host.value.session.epoch,
                     PeerId{"other-peer"},
                     member.value.joinTicket.data(),
                     member.value.joinTicket.size()).accepted);
}

TEST_CASE(BackendSessionTupleMustBeAllZeroOrAllNonZero) {
    ayt::test::setCurrentCase("BackendSessionTupleMustBeAllZeroOrAllNonZero");
    P2PConfig config;
    config.localPeerId = PeerId{"peer-a"};
    config.icePolicy = P2PIcePolicy::DirectOnly;
    CHECK(config.isValid());
    config.sessionId = 42;
    CHECK(!config.isValid());
    config.sessionId = 0;
    config.sessionEpoch = 7;
    CHECK(!config.isValid());
    config.sessionId = 42;
    CHECK(config.isValid());
}

TEST_CASE(LeaseKeeperRenewsAndStopsAfterGracefulTransfer) {
    ayt::test::setCurrentCase("LeaseKeeperRenewsAndStopsAfterGracefulTransfer");
    std::atomic<uint64_t> now{4000};
    InMemoryP2PSessionServiceConfig serviceConfig;
    serviceConfig.publicSignalingAddress = "session.example.test";
    serviceConfig.signalingPort = 28080;
    serviceConfig.hostLeaseSeconds = 10;
    serviceConfig.nowUnixSeconds = [&now] { return now.load(); };
    auto service = std::make_shared<InMemoryP2PSessionService>(serviceConfig);
    const auto host = service->createSession(hostRequest());
    P2PSessionJoinRequest join{host.value.session.sessionId, PeerId{"join-a"}};
    const auto member = service->joinSession(join);
    CHECK(host && member);

    P2PSessionLeaseKeeperConfig keeperConfig;
    keeperConfig.heartbeatIntervalMs = 20;
    keeperConfig.transientFailureRetryMs = 10;
    P2PSessionLeaseKeeper keeper(service, host.value.member, keeperConfig);
    CHECK(keeper.start(1));

    // The first renewal is immediate; the second observes the advanced clock.
    now.store(4005);
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(500);
    while (keeper.getLastSession().hostLeaseExpiresAtUnixSeconds != 4015 &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    CHECK(keeper.getLastSession().hostLeaseExpiresAtUnixSeconds == 4015);
    CHECK(keeper.isRunning());

    const auto promoted = keeper.claimHost(1, member.value.member.peerId);
    CHECK(promoted);
    CHECK_INT_EQ(promoted.value.epoch, 2);
    CHECK(!keeper.isRunning());

    // The old Host credential cannot resume renewal in the new epoch.
    CHECK(keeper.start(2));
    const auto stopDeadline = std::chrono::steady_clock::now() +
                              std::chrono::milliseconds(500);
    while (keeper.isRunning() &&
           std::chrono::steady_clock::now() < stopDeadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    CHECK(!keeper.isRunning());
    CHECK(keeper.getLastError() == SessionServiceError::Unauthorized);
}

TEST_SUITE_END
