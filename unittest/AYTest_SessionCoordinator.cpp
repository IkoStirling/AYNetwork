// Async P2P session orchestration and backend authority-gate coverage.

#include <AYNetwork/Session/InMemorySessionService.h>
#include <AYNetwork/Session/P2PSessionCoordinator.h>
#include <AYTest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <thread>
#include <vector>

using namespace ayt::net;

namespace
{

class CoordinatorNetwork final : public INetworkSubSystem {
public:
    const char* getName() const override { return "CoordinatorNetwork"; }
    const ::ayt::game::SubSystemDescriptor& getDescriptor() const override {
        static ::ayt::game::SubSystemDescriptor descriptor{
            "CoordinatorNetwork", {}, 0};
        return descriptor;
    }
    bool initialize() override { initialized = true; return true; }
    void shutdown() override { initialized = false; }
    void update(float) override {}
    void fixedUpdate(float) override {}
    void connect(const char*, uint16_t) override {}
    void listen(uint16_t) override {}
    void disconnect() override {
        connected = false;
        listening = false;
        session.role = P2PSessionRole::None;
        session.state = P2PSessionState::Idle;
    }
    bool isConnected() const override { return connected; }
    bool isListening() const override { return listening; }
    ConnectionMode getMode() const override {
        return listening ? ConnectionMode::ListenServer
                         : (connected ? ConnectionMode::Client
                                      : ConnectionMode::Disconnected);
    }

    bool configureP2P(const P2PConfig& input,
                      std::shared_ptr<ISignalingTransport> transport) override {
        if (rejectConfigure || !initialized || !input.isValid() || !transport) {
            return false;
        }
        config = input;
        signaling = std::move(transport);
        configured = true;
        session = {};
        session.state = P2PSessionState::Idle;
        session.localPeerId = config.localPeerId;
        session.virtualPort = config.virtualPort;
        session.sessionId = config.sessionId;
        session.epoch = config.sessionEpoch;
        session.migration = migrationEnabled
            ? P2PHostMigrationState::Stable
            : P2PHostMigrationState::Disabled;
        return true;
    }
    bool listenP2P() override {
        if (!configured) return false;
        listening = true;
        session.role = P2PSessionRole::Host;
        session.state = P2PSessionState::Hosting;
        session.hostPeerId = config.localPeerId;
        session.localSeatId = 1;
        return true;
    }
    bool connectP2P(const PeerId& remote) override {
        if (!configured || !remote.isValid()) return false;
        connected = true;
        session.role = P2PSessionRole::Client;
        session.state = P2PSessionState::Connecting;
        session.hostPeerId = remote;
        session.localSeatId = 2;
        return true;
    }
    bool isP2PConfigured() const override { return configured; }
    PeerId getLocalPeerId() const override { return config.localPeerId; }
    P2PSessionInfo getP2PSessionInfo() const override { return session; }
    bool setP2PJoinTicket(const void* bytes, size_t size) override {
        if (!bytes || size == 0 || size > kP2PMaxJoinTicketBytes) return false;
        const auto* first = static_cast<const uint8_t*>(bytes);
        ticket.assign(first, first + size);
        return true;
    }
    void setP2PSessionJoinValidator(
        P2PSessionJoinValidator input) override {
        validator = std::move(input);
    }
    void setP2PHostMigrationEnabled(bool enabled) override {
        migrationEnabled = enabled;
        session.migration = enabled ? P2PHostMigrationState::Stable
                                    : P2PHostMigrationState::Disabled;
    }
    bool requestP2PHostMigration() override {
        if (!migrationEnabled || session.role != P2PSessionRole::Host) {
            return false;
        }
        session.migration = P2PHostMigrationState::Preparing;
        return true;
    }
    void setP2PAuthorityTransitionGate(
        P2PAuthorityTransitionGate input) override {
        gate = std::move(input);
    }
    bool setP2PAuthorityTransitionTimeoutMs(uint32_t input) override {
        if (input == 0 || input > 60000) return false;
        authorityTimeoutMs = input;
        return true;
    }

    P2PAuthorityTransitionDecision authorize(
        const P2PMigrationContext& context) {
        return gate ? gate(context)
                    : P2PAuthorityTransitionDecision::Approved;
    }
    void makeActive() { session.state = P2PSessionState::Active; }

    void send(uint8_t, const void*, size_t) override {}
    void sendTo(NetConnection*, uint8_t, const void*, size_t) override {}
    void broadcast(uint8_t, const void*, size_t) override {}
    void broadcastExcept(NetConnection*, uint8_t, const void*, size_t) override {}
    void onMessage(uint8_t, MessageHandler) override {}
    void onConnectionChange(ConnectionHandler) override {}
    void setAcceptCallback(AcceptCallback) override {}
    void kickConnection(NetConnection*, const char*) override {}
    const std::vector<NetConnection*>& getConnections() override {
        return connections;
    }
    NetConnection* getConnection() const override { return nullptr; }
    uint32_t getHostId() const override { return 0; }
    void setExtension(INetworkExtension*) override {}
    ReplicationManager* getReplicationManager() override { return nullptr; }
    RpcHandler* getRpcHandler() override { return nullptr; }
    void setReplayRecorder(ayt::replay::IReplayRecorder*) override {}
    ayt::replay::IReplayRecorder* getReplayRecorder() const override {
        return nullptr;
    }

    bool initialized = true;
    bool rejectConfigure = false;
    bool configured = false;
    bool connected = false;
    bool listening = false;
    bool migrationEnabled = false;
    uint32_t authorityTimeoutMs = 0;
    P2PConfig config;
    P2PSessionInfo session;
    std::vector<uint8_t> ticket;
    P2PSessionJoinValidator validator;
    P2PAuthorityTransitionGate gate;
    std::shared_ptr<ISignalingTransport> signaling;
    std::vector<NetConnection*> connections;
};

class FaultSessionService final : public IP2PSessionService {
public:
    explicit FaultSessionService(std::shared_ptr<IP2PSessionService> inner)
        : _inner(std::move(inner)) {}

    SessionServiceResult<P2PSessionGrant> createSession(
        const P2PSessionCreateRequest& request) override {
        if (partitioned.load()) return transportFailure<P2PSessionGrant>();
        return _inner->createSession(request);
    }

    SessionServiceResult<P2PSessionGrant> joinSession(
        const P2PSessionJoinRequest& request) override {
        if (partitioned.load()) return transportFailure<P2PSessionGrant>();
        return _inner->joinSession(request);
    }

    SessionServiceResult<P2PBackendSessionInfo> heartbeat(
        const P2PSessionHeartbeatRequest& request) override {
        if (partitioned.load()) {
            return transportFailure<P2PBackendSessionInfo>();
        }
        return _inner->heartbeat(request);
    }

    SessionServiceResult<P2PBackendSessionInfo> claimHost(
        const P2PSessionClaimHostRequest& request) override {
        if (partitioned.load()) {
            return transportFailure<P2PBackendSessionInfo>();
        }
        auto result = _inner->claimHost(request);
        if (result && consume(loseClaimResponseAfterCommit)) {
            return transportFailure<P2PBackendSessionInfo>();
        }
        return result;
    }

    SessionServiceResult<SessionServiceEmpty> leaveSession(
        const P2PSessionLeaveRequest& request) override {
        if (partitioned.load() || consume(dropLeaveBeforeCommit)) {
            return transportFailure<SessionServiceEmpty>();
        }
        return _inner->leaveSession(request);
    }

    SessionServiceResult<P2PBackendSessionInfo> getSession(
        uint64_t sessionId) override {
        const uint32_t delay = getDelayMs.load();
        if (delay != 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(delay));
        }
        if (partitioned.load()) {
            return transportFailure<P2PBackendSessionInfo>();
        }
        return _inner->getSession(sessionId);
    }

    std::atomic<bool> partitioned{false};
    std::atomic<uint32_t> getDelayMs{0};
    std::atomic<uint32_t> loseClaimResponseAfterCommit{0};
    std::atomic<uint32_t> dropLeaveBeforeCommit{0};

private:
    static bool consume(std::atomic<uint32_t>& counter) {
        uint32_t current = counter.load();
        while (current != 0) {
            if (counter.compare_exchange_weak(current, current - 1)) return true;
        }
        return false;
    }

    template <typename T>
    static SessionServiceResult<T> transportFailure() {
        return SessionServiceResult<T>::failure(
            SessionServiceError::TransportError,
            "injected backend partition");
    }

    std::shared_ptr<IP2PSessionService> _inner;
};

std::shared_ptr<InMemoryP2PSessionService> makeService(
    std::atomic<uint64_t>& now) {
    InMemoryP2PSessionServiceConfig config;
    config.publicSignalingAddress = "127.0.0.1";
    config.signalingPort = 28080;
    config.hostLeaseSeconds = 10;
    config.joinTicketLifetimeSeconds = 60;
    config.signalingTokenLifetimeSeconds = 600;
    config.nowUnixSeconds = [&now] { return now.load(); };
    return std::make_shared<InMemoryP2PSessionService>(config);
}

P2PSessionCoordinatorConfig coordinatorConfig(
    const char* peer, uint16_t virtualPort = 7350,
    uint32_t authorityTimeoutMs = 1000) {
    P2PSessionCoordinatorConfig config;
    config.p2p.localPeerId = PeerId{peer};
    config.p2p.virtualPort = virtualPort;
    config.p2p.icePolicy = P2PIcePolicy::DirectOnly;
    config.p2p.allowPrivateCandidates = true;
    config.capacity = 4;
    config.authorityRetryMs = 10;
    config.authorityTimeoutMs = authorityTimeoutMs;
    config.lease.heartbeatIntervalMs = 20;
    config.lease.transientFailureRetryMs = 10;
    return config;
}

bool waitForState(P2PSessionCoordinator& coordinator,
                  P2PSessionCoordinatorState wanted,
                  uint32_t timeoutMs = 1000) {
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(timeoutMs);
    while (std::chrono::steady_clock::now() < deadline) {
        coordinator.update();
        if (coordinator.getStatus().state == wanted) return true;
        if (coordinator.getStatus().state ==
            P2PSessionCoordinatorState::Failed) return false;
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    return false;
}

P2PAuthorityTransitionDecision waitForAuthority(
    CoordinatorNetwork& network, P2PSessionCoordinator& coordinator,
    const P2PMigrationContext& context, uint32_t timeoutMs = 1500) {
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(timeoutMs);
    P2PAuthorityTransitionDecision decision =
        P2PAuthorityTransitionDecision::Pending;
    while (decision == P2PAuthorityTransitionDecision::Pending &&
           std::chrono::steady_clock::now() < deadline) {
        decision = network.authorize(context);
        coordinator.update();
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    return decision;
}

P2PSessionCreateRequest hostRequest(const char* peer = "host-a") {
    P2PSessionCreateRequest request;
    request.hostPeerId = PeerId{peer};
    request.virtualPort = 7350;
    request.capacity = 4;
    return request;
}

} // namespace

TEST_SUITE(P2PSessionCoordinator)

TEST_CASE(AsyncCreateConfiguresNetworkLeaseAndLeave) {
    ayt::test::setCurrentCase("AsyncCreateConfiguresNetworkLeaseAndLeave");
    std::atomic<uint64_t> now{1000};
    auto service = makeService(now);
    CoordinatorNetwork network;
    P2PSessionCoordinator coordinator(
        network, service, coordinatorConfig("host-a"));

    CHECK(coordinator.createSession());
    CHECK(waitForState(coordinator, P2PSessionCoordinatorState::Hosting));
    const auto grant = coordinator.getGrant();
    CHECK(grant.isValid());
    CHECK(network.configured);
    CHECK(network.listening);
    CHECK(network.config.sessionId == grant.session.sessionId);
    CHECK_INT_EQ(network.config.sessionEpoch, grant.session.epoch);
    CHECK(network.ticket == grant.joinTicket);
    CHECK(static_cast<bool>(network.validator));
    CHECK_INT_EQ(network.authorityTimeoutMs, 1000);

    CHECK(coordinator.leaveSession());
    CHECK(waitForState(coordinator, P2PSessionCoordinatorState::Idle));
    CHECK(service->getSession(grant.session.sessionId).error ==
          SessionServiceError::SessionNotFound);
}

TEST_CASE(AsyncJoinInstallsGrantAndTracksActiveState) {
    ayt::test::setCurrentCase("AsyncJoinInstallsGrantAndTracksActiveState");
    std::atomic<uint64_t> now{2000};
    auto service = makeService(now);
    const auto host = service->createSession(hostRequest());
    CHECK(host);
    CoordinatorNetwork network;
    P2PSessionCoordinator coordinator(
        network, service, coordinatorConfig("join-a", 0));

    CHECK(coordinator.joinSession(host.value.session.sessionId));
    CHECK(waitForState(coordinator, P2PSessionCoordinatorState::Connecting));
    CHECK(network.connected);
    CHECK(network.session.hostPeerId == host.value.member.peerId);
    CHECK(coordinator.getGrant().member.peerId == PeerId{"join-a"});
    network.makeActive();
    coordinator.update();
    CHECK(coordinator.getStatus().state ==
          P2PSessionCoordinatorState::Active);
}

TEST_CASE(BootstrapFailureRollsBackBackendMembership) {
    ayt::test::setCurrentCase("BootstrapFailureRollsBackBackendMembership");
    std::atomic<uint64_t> now{2500};
    auto service = makeService(now);
    CoordinatorNetwork network;
    network.rejectConfigure = true;
    P2PSessionCoordinator coordinator(
        network, service, coordinatorConfig("host-a"));

    CHECK(coordinator.createSession());
    uint64_t sessionId = 0;
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(1000);
    while (std::chrono::steady_clock::now() < deadline) {
        coordinator.update();
        const auto status = coordinator.getStatus();
        if (status.backendSession.sessionId != 0) {
            sessionId = status.backendSession.sessionId;
        }
        if (status.state == P2PSessionCoordinatorState::Failed) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    CHECK(coordinator.getStatus().state ==
          P2PSessionCoordinatorState::Failed);
    CHECK(coordinator.getStatus().error ==
          P2PSessionCoordinatorError::NetworkConfigurationFailed);

    // The failed Host grant is cleaned asynchronously; no unusable room is
    // left behind for discovery/join.
    const auto cleanupDeadline = std::chrono::steady_clock::now() +
                                 std::chrono::milliseconds(1000);
    while (sessionId != 0 && std::chrono::steady_clock::now() < cleanupDeadline &&
           service->getSession(sessionId).error !=
               SessionServiceError::SessionNotFound) {
        coordinator.update();
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    CHECK(sessionId != 0);
    CHECK(service->getSession(sessionId).error ==
          SessionServiceError::SessionNotFound);
}

TEST_CASE(GracefulAuthorityGateCommitsBackendBeforeApproval) {
    ayt::test::setCurrentCase("GracefulAuthorityGateCommitsBackendBeforeApproval");
    std::atomic<uint64_t> now{3000};
    auto service = makeService(now);
    CoordinatorNetwork network;
    P2PSessionCoordinator coordinator(
        network, service, coordinatorConfig("host-a"));
    CHECK(coordinator.createSession());
    CHECK(waitForState(coordinator, P2PSessionCoordinatorState::Hosting));
    const auto host = coordinator.getGrant();
    const auto member = service->joinSession(
        {host.session.sessionId, PeerId{"join-a"}});
    CHECK(member);

    P2PMigrationContext context;
    context.sessionId = host.session.sessionId;
    context.currentEpoch = 1;
    context.nextEpoch = 2;
    context.electedHostPeerId = member.value.member.peerId;
    context.graceful = true;
    CHECK(waitForAuthority(network, coordinator, context) ==
          P2PAuthorityTransitionDecision::Approved);
    const auto backend = service->getSession(context.sessionId);
    CHECK(backend);
    CHECK_INT_EQ(backend.value.epoch, 2);
    CHECK(backend.value.hostPeerId == context.electedHostPeerId);

    P2PSessionHeartbeatRequest stale;
    stale.member = host.member;
    stale.expectedEpoch = 2;
    CHECK(service->heartbeat(stale).error ==
          SessionServiceError::Unauthorized);
}

TEST_CASE(CrashCandidateWaitsForLeaseThenSelfClaims) {
    ayt::test::setCurrentCase("CrashCandidateWaitsForLeaseThenSelfClaims");
    std::atomic<uint64_t> now{4000};
    auto service = makeService(now);
    const auto host = service->createSession(hostRequest());
    CHECK(host);
    CoordinatorNetwork network;
    P2PSessionCoordinator coordinator(
        network, service, coordinatorConfig("join-a", 0, 1500));
    CHECK(coordinator.joinSession(host.value.session.sessionId));
    CHECK(waitForState(coordinator, P2PSessionCoordinatorState::Connecting));

    P2PMigrationContext context;
    context.sessionId = host.value.session.sessionId;
    context.currentEpoch = 1;
    context.nextEpoch = 2;
    context.electedHostPeerId = PeerId{"join-a"};
    context.graceful = false;
    CHECK(network.authorize(context) ==
          P2PAuthorityTransitionDecision::Pending);
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    CHECK(network.authorize(context) ==
          P2PAuthorityTransitionDecision::Pending);
    now.store(4010);
    CHECK(waitForAuthority(network, coordinator, context) ==
          P2PAuthorityTransitionDecision::Approved);
    const auto backend = service->getSession(context.sessionId);
    CHECK(backend && backend.value.hostPeerId == PeerId{"join-a"});
    CHECK_INT_EQ(backend.value.epoch, 2);
}

TEST_CASE(ConcurrentCrashCandidatesOnlyOneEpochCasWins) {
    ayt::test::setCurrentCase("ConcurrentCrashCandidatesOnlyOneEpochCasWins");
    std::atomic<uint64_t> now{5000};
    auto service = makeService(now);
    const auto host = service->createSession(hostRequest());
    CHECK(host);
    CoordinatorNetwork networkA;
    CoordinatorNetwork networkB;
    P2PSessionCoordinator coordinatorA(
        networkA, service, coordinatorConfig("join-a", 0));
    P2PSessionCoordinator coordinatorB(
        networkB, service, coordinatorConfig("join-b", 0));
    CHECK(coordinatorA.joinSession(host.value.session.sessionId));
    CHECK(coordinatorB.joinSession(host.value.session.sessionId));
    CHECK(waitForState(coordinatorA, P2PSessionCoordinatorState::Connecting));
    CHECK(waitForState(coordinatorB, P2PSessionCoordinatorState::Connecting));
    now.store(5010);

    P2PMigrationContext contextA{
        host.value.session.sessionId, 1, 2, PeerId{"join-a"}, false};
    P2PMigrationContext contextB{
        host.value.session.sessionId, 1, 2, PeerId{"join-b"}, false};
    auto decisionA = P2PAuthorityTransitionDecision::Pending;
    auto decisionB = P2PAuthorityTransitionDecision::Pending;
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(1500);
    while ((decisionA == P2PAuthorityTransitionDecision::Pending ||
            decisionB == P2PAuthorityTransitionDecision::Pending) &&
           std::chrono::steady_clock::now() < deadline) {
        if (decisionA == P2PAuthorityTransitionDecision::Pending) {
            decisionA = networkA.authorize(contextA);
        }
        if (decisionB == P2PAuthorityTransitionDecision::Pending) {
            decisionB = networkB.authorize(contextB);
        }
        coordinatorA.update();
        coordinatorB.update();
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    CHECK((decisionA == P2PAuthorityTransitionDecision::Approved) !=
          (decisionB == P2PAuthorityTransitionDecision::Approved));
    CHECK((decisionA == P2PAuthorityTransitionDecision::Rejected) !=
          (decisionB == P2PAuthorityTransitionDecision::Rejected));
    const auto backend = service->getSession(host.value.session.sessionId);
    CHECK(backend);
    CHECK_INT_EQ(backend.value.epoch, 2);
}

TEST_CASE(AuthorityTimeoutRemainsPendingForNetworkFailClosedDeadline) {
    ayt::test::setCurrentCase(
        "AuthorityTimeoutRemainsPendingForNetworkFailClosedDeadline");
    std::atomic<uint64_t> now{6000};
    auto service = makeService(now);
    const auto host = service->createSession(hostRequest());
    CHECK(host);
    CoordinatorNetwork network;
    P2PSessionCoordinator coordinator(
        network, service, coordinatorConfig("join-a", 0, 80));
    CHECK(coordinator.joinSession(host.value.session.sessionId));
    CHECK(waitForState(coordinator, P2PSessionCoordinatorState::Connecting));

    P2PMigrationContext context{
        host.value.session.sessionId, 1, 2, PeerId{"join-a"}, false};
    CHECK(network.authorize(context) ==
          P2PAuthorityTransitionDecision::Pending);
    std::this_thread::sleep_for(std::chrono::milliseconds(120));
    CHECK(network.authorize(context) ==
          P2PAuthorityTransitionDecision::Pending);
    CHECK(coordinator.getStatus().error ==
          P2PSessionCoordinatorError::AuthorityTimeout);
}

TEST_CASE(CrashMigrationFailureBecomesTerminalCoordinatorFailure) {
    ayt::test::setCurrentCase(
        "CrashMigrationFailureBecomesTerminalCoordinatorFailure");
    std::atomic<uint64_t> now{7000};
    auto service = makeService(now);
    const auto host = service->createSession(hostRequest());
    CHECK(host);
    CoordinatorNetwork network;
    P2PSessionCoordinator coordinator(
        network, service, coordinatorConfig("join-a", 0));
    CHECK(coordinator.joinSession(host.value.session.sessionId));
    CHECK(waitForState(coordinator, P2PSessionCoordinatorState::Connecting));

    network.session.migration = P2PHostMigrationState::Failed;
    network.session.migrationFailure =
        P2PMigrationFailureReason::ReconnectFailed;
    coordinator.update();
    CHECK(coordinator.getStatus().state ==
          P2PSessionCoordinatorState::Failed);
    CHECK(coordinator.getStatus().error ==
          P2PSessionCoordinatorError::MigrationFailed);
}

TEST_CASE(LostClaimResponseReconcilesCommittedBackendEpoch) {
    ayt::test::setCurrentCase(
        "LostClaimResponseReconcilesCommittedBackendEpoch");
    std::atomic<uint64_t> now{8000};
    auto core = makeService(now);
    const auto host = core->createSession(hostRequest());
    CHECK(host);
    auto faults = std::make_shared<FaultSessionService>(core);
    CoordinatorNetwork network;
    P2PSessionCoordinator coordinator(
        network, faults, coordinatorConfig("join-a", 0));
    CHECK(coordinator.joinSession(host.value.session.sessionId));
    CHECK(waitForState(coordinator, P2PSessionCoordinatorState::Connecting));

    now.store(8010);
    faults->loseClaimResponseAfterCommit.store(1);
    P2PMigrationContext context{
        host.value.session.sessionId, 1, 2, PeerId{"join-a"}, false};
    CHECK(waitForAuthority(network, coordinator, context) ==
          P2PAuthorityTransitionDecision::Approved);
    const auto canonical = core->getSession(context.sessionId);
    CHECK(canonical);
    CHECK_INT_EQ(canonical.value.epoch, 2);
    CHECK(canonical.value.hostPeerId == PeerId{"join-a"});
}

TEST_CASE(BackendPartitionTimesOutWithoutAdvancingAuthority) {
    ayt::test::setCurrentCase(
        "BackendPartitionTimesOutWithoutAdvancingAuthority");
    std::atomic<uint64_t> now{9000};
    auto core = makeService(now);
    const auto host = core->createSession(hostRequest());
    CHECK(host);
    auto faults = std::make_shared<FaultSessionService>(core);
    CoordinatorNetwork network;
    P2PSessionCoordinator coordinator(
        network, faults, coordinatorConfig("join-a", 0, 80));
    CHECK(coordinator.joinSession(host.value.session.sessionId));
    CHECK(waitForState(coordinator, P2PSessionCoordinatorState::Connecting));

    now.store(9010);
    faults->partitioned.store(true);
    P2PMigrationContext context{
        host.value.session.sessionId, 1, 2, PeerId{"join-a"}, false};
    CHECK(network.authorize(context) ==
          P2PAuthorityTransitionDecision::Pending);
    std::this_thread::sleep_for(std::chrono::milliseconds(120));
    CHECK(network.authorize(context) ==
          P2PAuthorityTransitionDecision::Pending);
    CHECK(coordinator.getStatus().error ==
          P2PSessionCoordinatorError::AuthorityTimeout);
    faults->partitioned.store(false);
    const auto canonical = core->getSession(context.sessionId);
    CHECK(canonical);
    CHECK_INT_EQ(canonical.value.epoch, 1);
    CHECK(canonical.value.hostPeerId == PeerId{"host-a"});
}

TEST_CASE(SlowBackendAuthorityDoesNotBlockNetworkThread) {
    ayt::test::setCurrentCase("SlowBackendAuthorityDoesNotBlockNetworkThread");
    std::atomic<uint64_t> now{10000};
    auto core = makeService(now);
    const auto host = core->createSession(hostRequest());
    CHECK(host);
    auto faults = std::make_shared<FaultSessionService>(core);
    CoordinatorNetwork network;
    P2PSessionCoordinator coordinator(
        network, faults, coordinatorConfig("join-a", 0, 1000));
    CHECK(coordinator.joinSession(host.value.session.sessionId));
    CHECK(waitForState(coordinator, P2PSessionCoordinatorState::Connecting));

    now.store(10010);
    faults->getDelayMs.store(200);
    P2PMigrationContext context{
        host.value.session.sessionId, 1, 2, PeerId{"join-a"}, false};
    const auto started = std::chrono::steady_clock::now();
    CHECK(network.authorize(context) ==
          P2PAuthorityTransitionDecision::Pending);
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - started).count();
    CHECK(elapsed < 50);
    CHECK(waitForAuthority(network, coordinator, context) ==
          P2PAuthorityTransitionDecision::Approved);
}

TEST_CASE(LeaveTransportFailureRetainsCredentialForRetry) {
    ayt::test::setCurrentCase("LeaveTransportFailureRetainsCredentialForRetry");
    std::atomic<uint64_t> now{11000};
    auto core = makeService(now);
    auto faults = std::make_shared<FaultSessionService>(core);
    CoordinatorNetwork network;
    P2PSessionCoordinator coordinator(
        network, faults, coordinatorConfig("host-a"));
    CHECK(coordinator.createSession());
    CHECK(waitForState(coordinator, P2PSessionCoordinatorState::Hosting));
    const uint64_t sessionId = coordinator.getGrant().session.sessionId;

    faults->dropLeaveBeforeCommit.store(1);
    CHECK(coordinator.leaveSession());
    CHECK(!waitForState(
        coordinator, P2PSessionCoordinatorState::Idle, 500));
    CHECK(coordinator.getStatus().state ==
          P2PSessionCoordinatorState::Failed);
    CHECK(coordinator.getGrant().member.isValid());
    CHECK(coordinator.leaveSession());
    CHECK(waitForState(coordinator, P2PSessionCoordinatorState::Idle));
    CHECK(core->getSession(sessionId).error ==
          SessionServiceError::SessionNotFound);
}

TEST_CASE(HealedOldHostRemainsFencedAfterPartitionElection) {
    ayt::test::setCurrentCase(
        "HealedOldHostRemainsFencedAfterPartitionElection");
    std::atomic<uint64_t> now{12000};
    auto core = makeService(now);
    const auto host = core->createSession(hostRequest());
    CHECK(host);
    const auto member = core->joinSession(
        {host.value.session.sessionId, PeerId{"join-a"}});
    CHECK(member);
    now.store(12010);

    P2PSessionClaimHostRequest claim;
    claim.member = member.value.member;
    claim.expectedEpoch = 1;
    claim.newHostPeerId = member.value.member.peerId;
    const auto promoted = core->claimHost(claim);
    CHECK(promoted);
    CHECK_INT_EQ(promoted.value.epoch, 2);

    P2PSessionHeartbeatRequest staleHeartbeat;
    staleHeartbeat.member = host.value.member;
    staleHeartbeat.expectedEpoch = 1;
    CHECK(core->heartbeat(staleHeartbeat).error ==
          SessionServiceError::Unauthorized);
    staleHeartbeat.expectedEpoch = 2;
    CHECK(core->heartbeat(staleHeartbeat).error ==
          SessionServiceError::Unauthorized);

    P2PSessionClaimHostRequest staleClaim;
    staleClaim.member = host.value.member;
    staleClaim.expectedEpoch = 2;
    staleClaim.newHostPeerId = host.value.member.peerId;
    CHECK(core->claimHost(staleClaim).error ==
          SessionServiceError::Unauthorized);
}

TEST_SUITE_END
