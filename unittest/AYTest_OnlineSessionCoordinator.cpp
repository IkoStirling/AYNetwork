// Game-facing Lobby / Matchmaking assignment orchestration coverage.

#include <AYEventSystem/EventBus.h>
#include <AYNetwork/Session/HttpSessionService.h>
#include <AYNetwork/Session/InMemoryOnlineServices.h>
#include <AYNetwork/Session/InMemorySessionService.h>
#include <AYNetwork/Session/OnlineFlowCoordinator.h>
#include <AYNetwork/Session/OnlineSessionCoordinator.h>
#include <AYNetwork/Session/OnlineSessionEvents.h>
#include <AYNetwork/Session/OnlineSubSystem.h>
#include <AYTest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <string_view>
#include <thread>
#include <vector>

using namespace ayt::net;

namespace
{

class OnlineCoordinatorNetwork final : public INetworkSubSystem {
public:
    const char* getName() const override { return "OnlineCoordinatorNetwork"; }
    const ::ayt::game::SubSystemDescriptor& getDescriptor() const override {
        static ::ayt::game::SubSystemDescriptor descriptor{
            "OnlineCoordinatorNetwork", {}, 0};
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
        if (!initialized || !input.isValid() || !transport) return false;
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
        if (!bytes && size == 0) {
            ticket.clear();
            return true;
        }
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
    void setP2PAuthorityTransitionGate(
        P2PAuthorityTransitionGate input) override {
        authorityGate = std::move(input);
    }
    bool setP2PAuthorityTransitionTimeoutMs(uint32_t input) override {
        authorityTimeoutMs = input;
        return input != 0 && input <= 60000;
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
    bool configured = false;
    bool connected = false;
    bool listening = false;
    bool migrationEnabled = false;
    uint32_t authorityTimeoutMs = 0;
    P2PConfig config;
    P2PSessionInfo session;
    std::vector<uint8_t> ticket;
    P2PSessionJoinValidator validator;
    P2PAuthorityTransitionGate authorityGate;
    std::shared_ptr<ISignalingTransport> signaling;
    std::vector<NetConnection*> connections;
};

class FakeDedicatedConnector final : public IDedicatedSessionConnector {
public:
    bool connect(const DedicatedAllocation& input) override {
        if (!acceptConnect || !input.isValid() ||
            status.state != DedicatedSessionConnectionState::Idle) return false;
        allocation = input;
        status.state = DedicatedSessionConnectionState::Connecting;
        return true;
    }
    void disconnect() override {
        ++disconnectCount;
        status = {};
    }
    void update() override { ++updateCount; }
    DedicatedSessionConnectionStatus getStatus() const override {
        return status;
    }
    void activate() {
        status.state = DedicatedSessionConnectionState::Active;
    }

    bool acceptConnect = true;
    uint32_t disconnectCount = 0;
    uint32_t updateCount = 0;
    DedicatedAllocation allocation;
    DedicatedSessionConnectionStatus status;
};

class InvalidAssignmentMatchmaking final : public IMatchmakingService {
public:
    OnlineServiceResult<MatchTicketInfo> enqueueMatch(
        const MatchmakingRequest& request) override {
        MatchTicketInfo info;
        info.ticketId = 77;
        info.request = request;
        return OnlineServiceResult<MatchTicketInfo>::success(std::move(info));
    }
    OnlineServiceResult<MatchTicketInfo> getMatch(
        MatchTicketId ticketId, const PeerId&) override {
        MatchTicketInfo info;
        info.ticketId = ticketId;
        info.state = MatchTicketState::Matched;
        info.assignment.topology = MatchTopology::P2P;
        return OnlineServiceResult<MatchTicketInfo>::success(std::move(info));
    }
    OnlineServiceResult<MatchTicketInfo> cancelMatch(
        MatchTicketId, const PeerId&) override {
        return OnlineServiceResult<MatchTicketInfo>::failure(
            OnlineServiceError::Conflict);
    }
    size_t runMatchmaking(size_t) override { return 0; }
};

class RecoverableCancellationMatchmaking final : public IMatchmakingService {
public:
    OnlineServiceResult<MatchTicketInfo> enqueueMatch(
        const MatchmakingRequest& request) override {
        MatchTicketInfo info;
        info.ticketId = 88;
        info.state = MatchTicketState::Queued;
        info.request = request;
        return OnlineServiceResult<MatchTicketInfo>::success(std::move(info));
    }
    OnlineServiceResult<MatchTicketInfo> getMatch(
        MatchTicketId ticketId, const PeerId&) override {
        MatchTicketInfo info;
        info.ticketId = ticketId;
        info.state = MatchTicketState::Queued;
        return OnlineServiceResult<MatchTicketInfo>::success(std::move(info));
    }
    OnlineServiceResult<MatchTicketInfo> cancelMatch(
        MatchTicketId ticketId, const PeerId&) override {
        const uint32_t attempt = ++cancelCalls;
        if (attempt <= failuresBeforeSuccess) {
            return OnlineServiceResult<MatchTicketInfo>::failure(
                OnlineServiceError::BackendUnavailable,
                "temporary cancellation outage");
        }
        MatchTicketInfo info;
        info.ticketId = ticketId;
        info.state = MatchTicketState::Cancelled;
        return OnlineServiceResult<MatchTicketInfo>::success(std::move(info));
    }
    size_t runMatchmaking(size_t) override { return 0; }

    uint32_t failuresBeforeSuccess = 4;
    std::atomic<uint32_t> cancelCalls{0};
};

class AssignmentWinsCancellationMatchmaking final
    : public IMatchmakingService {
public:
    explicit AssignmentWinsCancellationMatchmaking(P2PSessionGrant input)
        : grant(std::move(input)) {}

    OnlineServiceResult<MatchTicketInfo> enqueueMatch(
        const MatchmakingRequest& request) override {
        MatchTicketInfo info;
        info.ticketId = 99;
        info.state = MatchTicketState::Queued;
        info.request = request;
        return OnlineServiceResult<MatchTicketInfo>::success(std::move(info));
    }
    OnlineServiceResult<MatchTicketInfo> getMatch(
        MatchTicketId ticketId, const PeerId&) override {
        ++getCalls;
        MatchTicketInfo info;
        info.ticketId = ticketId;
        info.state = MatchTicketState::Matched;
        info.assignment.topology = MatchTopology::P2P;
        info.assignment.p2pGrants.push_back(grant);
        return OnlineServiceResult<MatchTicketInfo>::success(std::move(info));
    }
    OnlineServiceResult<MatchTicketInfo> cancelMatch(
        MatchTicketId, const PeerId&) override {
        ++cancelCalls;
        return OnlineServiceResult<MatchTicketInfo>::failure(
            OnlineServiceError::Conflict,
            "assignment was committed first");
    }
    size_t runMatchmaking(size_t) override { return 0; }

    P2PSessionGrant grant;
    std::atomic<uint32_t> getCalls{0};
    std::atomic<uint32_t> cancelCalls{0};
};

std::shared_ptr<InMemoryP2PSessionService> makeSessionService(
    std::atomic<uint64_t>& now) {
    InMemoryP2PSessionServiceConfig config;
    config.publicSignalingAddress = "127.0.0.1";
    config.signalingPort = 28080;
    config.hostLeaseSeconds = 10;
    config.joinTicketLifetimeSeconds = 60;
    config.signalingTokenLifetimeSeconds = 600;
    config.nowUnixSeconds = [&now] { return now.load(); };
    return std::make_shared<InMemoryP2PSessionService>(std::move(config));
}

P2PSessionCoordinatorConfig p2pConfig(const char* peer) {
    P2PSessionCoordinatorConfig config;
    config.p2p.localPeerId = PeerId{peer};
    config.p2p.icePolicy = P2PIcePolicy::DirectOnly;
    config.p2p.allowPrivateCandidates = true;
    config.capacity = 4;
    config.authorityRetryMs = 10;
    config.authorityTimeoutMs = 1000;
    config.lease.heartbeatIntervalMs = 20;
    config.lease.transientFailureRetryMs = 10;
    return config;
}

OnlineSessionCoordinatorConfig onlineConfig(const char* peer) {
    OnlineSessionCoordinatorConfig config;
    config.localPeerId = PeerId{peer};
    config.matchmakingPollIntervalMs = 1;
    config.maxConsecutivePollFailures = 3;
    return config;
}

OnlineSubSystemConfig subsystemConfig(const char* peer) {
    OnlineSubSystemConfig config;
    config.localPeerId = PeerId{peer};
    config.p2p = p2pConfig(peer);
    config.sessions = onlineConfig(peer);
    config.gracefulShutdownTimeoutMs = 500;
    return config;
}

OnlineSubSystemDependencies subsystemDependencies(
    const std::shared_ptr<InMemoryP2PSessionService>& sessions,
    const std::shared_ptr<InMemoryOnlineServices>& online) {
    OnlineSubSystemDependencies dependencies;
    dependencies.sessionService = sessions;
    dependencies.lobbyService = online;
    dependencies.matchmakingService = online;
    return dependencies;
}

CreateLobbyRequest lobbyRequest(const char* owner = "owner") {
    CreateLobbyRequest request;
    request.ownerPeerId = PeerId{owner};
    request.name = "Coordinator Lobby";
    request.region = "asia";
    request.buildId = "build-1";
    request.capacity = 2;
    return request;
}

MatchmakingRequest matchRequest(const char* peer, MatchTopology topology) {
    MatchmakingRequest request;
    request.partyMembers = {PeerId{peer}};
    request.queue = "coordinator";
    request.region = "asia";
    request.buildId = "build-1";
    request.topology = topology;
    request.targetPlayers = 2;
    request.virtualPort = 7350;
    return request;
}

template <typename Predicate>
bool waitFor(OnlineSessionCoordinator& coordinator,
             Predicate&& predicate,
             uint32_t timeoutMs = 1500) {
    const auto deadline = std::chrono::steady_clock::now() +
        std::chrono::milliseconds(timeoutMs);
    while (std::chrono::steady_clock::now() < deadline) {
        coordinator.update();
        const auto status = coordinator.getStatus();
        if (predicate(status)) return true;
        if (status.state == OnlineSessionCoordinatorState::Failed) return false;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return false;
}

bool waitForState(OnlineSessionCoordinator& coordinator,
                  OnlineSessionCoordinatorState state,
                  uint32_t timeoutMs = 1500) {
    return waitFor(coordinator, [state](const auto& status) {
        return status.state == state;
    }, timeoutMs);
}

template <typename Predicate>
bool waitForSubSystem(IOnlineSubSystem& subsystem,
                      ayt::event::EventBus& eventBus,
                      Predicate&& predicate,
                      uint32_t timeoutMs = 2000) {
    const auto deadline = std::chrono::steady_clock::now() +
        std::chrono::milliseconds(timeoutMs);
    while (std::chrono::steady_clock::now() < deadline) {
        subsystem.update(0.0f);
        eventBus.pump();
        const auto status = subsystem.getOnlineStatus();
        if (predicate(status)) return true;
        if (status.state == OnlineSessionCoordinatorState::Failed) return false;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return false;
}

bool waitForSubSystemState(IOnlineSubSystem& subsystem,
                           ayt::event::EventBus& eventBus,
                           OnlineSessionCoordinatorState state,
                           uint32_t timeoutMs = 2000) {
    return waitForSubSystem(subsystem, eventBus, [state](const auto& status) {
        return status.state == state;
    }, timeoutMs);
}

template <typename Predicate>
bool waitForFlow(IOnlineSubSystem& subsystem,
                 OnlineFlowCoordinator& flow,
                 ayt::event::EventBus& eventBus,
                 Predicate&& predicate,
                 uint32_t timeoutMs = 2000) {
    const auto deadline = std::chrono::steady_clock::now() +
        std::chrono::milliseconds(timeoutMs);
    while (std::chrono::steady_clock::now() < deadline) {
        subsystem.update(0.0f);
        flow.update();
        eventBus.pump();
        const auto status = flow.getStatus();
        if (predicate(status)) return true;
        if (status.state == OnlineFlowState::Failed) return false;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return false;
}

} // namespace

TEST_SUITE(OnlineSessionCoordinator)

TEST_CASE(LobbyLaunchConsumesAssignedGrantWithoutSecondBackendJoin) {
    ayt::test::setCurrentCase(
        "LobbyLaunchConsumesAssignedGrantWithoutSecondBackendJoin");
    std::atomic<uint64_t> now{1000};
    auto sessions = makeSessionService(now);
    auto online = std::make_shared<InMemoryOnlineServices>(
        InMemoryOnlineServicesConfig{}, sessions);
    OnlineCoordinatorNetwork network;
    P2PSessionCoordinator p2p(network, sessions, p2pConfig("owner"));
    OnlineSessionCoordinator coordinator(
        online, online, p2p, onlineConfig("owner"));

    CHECK(coordinator.createLobby(lobbyRequest("forged-owner")));
    CHECK(waitForState(coordinator, OnlineSessionCoordinatorState::InLobby));
    CHECK(coordinator.getStatus().lobby.ownerPeerId == PeerId{"owner"});
    CHECK(coordinator.updateLobbyName("Updated Lobby"));
    CHECK(waitForState(coordinator, OnlineSessionCoordinatorState::InLobby));
    CHECK(coordinator.getStatus().lobby.name == "Updated Lobby");

    CHECK(coordinator.launchLobbyP2P(7350));
    CHECK(waitForState(coordinator, OnlineSessionCoordinatorState::InSession));
    const auto status = coordinator.getStatus();
    CHECK(status.topology == OnlineSessionTopology::P2P);
    CHECK(network.listening);
    CHECK(p2p.getGrant().member.peerId == PeerId{"owner"});
    const auto backend = sessions->getSession(
        p2p.getGrant().session.sessionId);
    CHECK(backend);
    CHECK_INT_EQ(backend.value.memberCount, 1);

    CHECK(coordinator.leaveSession());
    CHECK(waitForState(coordinator, OnlineSessionCoordinatorState::Idle));
    CHECK(sessions->getSession(backend.value.sessionId).error ==
          SessionServiceError::SessionNotFound);
}

TEST_CASE(LobbyDiscoveryJoinRefreshAndLeaveStayNonBlocking) {
    ayt::test::setCurrentCase("LobbyDiscoveryJoinRefreshAndLeaveStayNonBlocking");
    std::atomic<uint64_t> now{2000};
    auto sessions = makeSessionService(now);
    auto online = std::make_shared<InMemoryOnlineServices>(
        InMemoryOnlineServicesConfig{}, sessions);
    CHECK(online->createLobby(lobbyRequest("owner")));

    OnlineCoordinatorNetwork network;
    P2PSessionCoordinator p2p(network, sessions, p2pConfig("guest"));
    OnlineSessionCoordinator coordinator(
        online, online, p2p, onlineConfig("guest"));
    CHECK(coordinator.listLobbies({"asia", "build-1", 1, 10}));
    CHECK(waitForState(coordinator, OnlineSessionCoordinatorState::Idle));
    const auto listed = coordinator.getLobbyResults();
    CHECK_INT_EQ(listed.size(), 1);
    CHECK(coordinator.joinLobby(listed.front().lobbyId));
    CHECK(waitForState(coordinator, OnlineSessionCoordinatorState::InLobby));
    CHECK(coordinator.refreshLobby());
    CHECK(waitForState(coordinator, OnlineSessionCoordinatorState::InLobby));
    CHECK_INT_EQ(coordinator.getStatus().lobby.members.size(), 2);
    CHECK(coordinator.leaveLobby());
    CHECK(waitForState(coordinator, OnlineSessionCoordinatorState::Idle));
}

TEST_CASE(MatchmakingP2PAssignmentStartsLocalGrantAutomatically) {
    ayt::test::setCurrentCase(
        "MatchmakingP2PAssignmentStartsLocalGrantAutomatically");
    std::atomic<uint64_t> now{3000};
    auto sessions = makeSessionService(now);
    auto online = std::make_shared<InMemoryOnlineServices>(
        InMemoryOnlineServicesConfig{}, sessions);
    OnlineCoordinatorNetwork network;
    P2PSessionCoordinator p2p(network, sessions, p2pConfig("local"));
    OnlineSessionCoordinator coordinator(
        online, online, p2p, onlineConfig("local"));

    CHECK(coordinator.startMatchmaking(
        matchRequest("local", MatchTopology::P2P)));
    CHECK(waitFor(coordinator, [](const auto& status) {
        return status.state == OnlineSessionCoordinatorState::Queueing &&
               status.matchTicket.ticketId != 0;
    }));
    CHECK(online->enqueueMatch(matchRequest("guest", MatchTopology::P2P)));
    CHECK_INT_EQ(online->runMatchmaking(1), 1);
    CHECK(waitForState(coordinator, OnlineSessionCoordinatorState::InSession));
    const auto status = coordinator.getStatus();
    CHECK(status.matchTicket.state == MatchTicketState::Matched);
    CHECK(status.topology == OnlineSessionTopology::P2P);
    CHECK(p2p.getGrant().member.peerId == PeerId{"local"});
    CHECK(network.listening);
    const auto backend = sessions->getSession(
        p2p.getGrant().session.sessionId);
    CHECK(backend);
    CHECK_INT_EQ(backend.value.memberCount, 2);
}

TEST_CASE(MatchmakingCancellationStopsQueuedTicket) {
    ayt::test::setCurrentCase("MatchmakingCancellationStopsQueuedTicket");
    std::atomic<uint64_t> now{4000};
    auto sessions = makeSessionService(now);
    auto online = std::make_shared<InMemoryOnlineServices>(
        InMemoryOnlineServicesConfig{}, sessions);
    OnlineCoordinatorNetwork network;
    P2PSessionCoordinator p2p(network, sessions, p2pConfig("local"));
    OnlineSessionCoordinator coordinator(
        online, online, p2p, onlineConfig("local"));

    CHECK(coordinator.startMatchmaking(
        matchRequest("local", MatchTopology::P2P)));
    // Cancellation is accepted even while enqueueMatch is still in flight;
    // the coordinator preserves intent until the ticket id arrives.
    CHECK(coordinator.cancelMatchmaking());
    CHECK(waitFor(coordinator, [](const auto& status) {
        return status.state == OnlineSessionCoordinatorState::CancellingMatch &&
               status.matchTicket.ticketId != 0;
    }));
    const MatchTicketId ticketId =
        coordinator.getStatus().matchTicket.ticketId;
    CHECK(waitForState(coordinator, OnlineSessionCoordinatorState::Idle));
    const auto cancelled = online->getMatch(ticketId, PeerId{"local"});
    CHECK(cancelled);
    CHECK(cancelled.value.state == MatchTicketState::Cancelled);
}

TEST_CASE(DedicatedAssignmentInstallsReservationAndTracksConnector) {
    ayt::test::setCurrentCase(
        "DedicatedAssignmentInstallsReservationAndTracksConnector");
    std::atomic<uint64_t> now{5000};
    auto sessions = makeSessionService(now);
    InMemoryOnlineServicesConfig onlineSettings;
    onlineSettings.nowUnixSeconds = [&now] { return now.load(); };
    auto online = std::make_shared<InMemoryOnlineServices>(
        onlineSettings, sessions);
    CHECK(online->registerServer(
        {"server-a", "asia", "build-1", "203.0.113.10", 7200, 8}));
    auto dedicated = std::make_shared<FakeDedicatedConnector>();
    OnlineCoordinatorNetwork network;
    P2PSessionCoordinator p2p(network, sessions, p2pConfig("local"));
    OnlineSessionCoordinator coordinator(
        online, online, p2p, onlineConfig("local"), dedicated);

    CHECK(coordinator.startMatchmaking(
        matchRequest("local", MatchTopology::Dedicated)));
    CHECK(waitFor(coordinator, [](const auto& status) {
        return status.matchTicket.ticketId != 0;
    }));
    CHECK(online->enqueueMatch(
        matchRequest("guest", MatchTopology::Dedicated)));
    CHECK_INT_EQ(online->runMatchmaking(1), 1);
    CHECK(waitForState(coordinator, OnlineSessionCoordinatorState::Connecting));
    CHECK(dedicated->allocation.isValid());
    CHECK(dedicated->allocation.address == "203.0.113.10");
    CHECK(dedicated->allocation.reservationToken.size() == 64);
    dedicated->activate();
    coordinator.update();
    CHECK(coordinator.getStatus().state ==
          OnlineSessionCoordinatorState::InSession);
    CHECK(coordinator.leaveSession());
    coordinator.update();
    CHECK(coordinator.getStatus().state ==
          OnlineSessionCoordinatorState::Idle);
    CHECK_INT_EQ(dedicated->disconnectCount, 1);
}

TEST_CASE(InvalidAssignmentFailsClosedWithoutStartingTransport) {
    ayt::test::setCurrentCase(
        "InvalidAssignmentFailsClosedWithoutStartingTransport");
    std::atomic<uint64_t> now{6000};
    auto sessions = makeSessionService(now);
    auto lobbies = std::make_shared<InMemoryOnlineServices>(
        InMemoryOnlineServicesConfig{}, sessions);
    auto matchmaking = std::make_shared<InvalidAssignmentMatchmaking>();
    OnlineCoordinatorNetwork network;
    P2PSessionCoordinator p2p(network, sessions, p2pConfig("local"));
    OnlineSessionCoordinator coordinator(
        lobbies, matchmaking, p2p, onlineConfig("local"));

    CHECK(coordinator.startMatchmaking(
        matchRequest("local", MatchTopology::P2P)));
    CHECK(waitFor(coordinator, [](const auto& status) {
        return status.state == OnlineSessionCoordinatorState::Failed;
    }));
    const auto status = coordinator.getStatus();
    CHECK(status.error == OnlineSessionCoordinatorError::InvalidAssignment);
    CHECK(!network.configured);
    CHECK(!p2p.getGrant().isValid());
    CHECK(coordinator.reset());
    CHECK(coordinator.getStatus().state ==
          OnlineSessionCoordinatorState::Idle);
}

TEST_CASE(ExplicitCancelAfterTerminalFailureGetsFreshRetryBudget) {
    ayt::test::setCurrentCase(
        "ExplicitCancelAfterTerminalFailureGetsFreshRetryBudget");
    std::atomic<uint64_t> now{7000};
    auto sessions = makeSessionService(now);
    auto lobbies = std::make_shared<InMemoryOnlineServices>(
        InMemoryOnlineServicesConfig{}, sessions);
    auto matchmaking =
        std::make_shared<RecoverableCancellationMatchmaking>();
    OnlineCoordinatorNetwork network;
    P2PSessionCoordinator p2p(network, sessions, p2pConfig("local"));
    auto config = onlineConfig("local");
    config.maxConsecutivePollFailures = 2;
    OnlineSessionCoordinator coordinator(
        lobbies, matchmaking, p2p, std::move(config));

    CHECK(coordinator.startMatchmaking(
        matchRequest("local", MatchTopology::P2P)));
    CHECK(waitFor(coordinator, [](const auto& status) {
        return status.state == OnlineSessionCoordinatorState::Queueing &&
               status.matchTicket.ticketId != 0;
    }));
    CHECK(coordinator.cancelMatchmaking());
    CHECK(waitFor(coordinator, [](const auto& status) {
        return status.state == OnlineSessionCoordinatorState::Failed;
    }));
    CHECK(coordinator.getStatus().serviceError ==
          OnlineServiceError::BackendUnavailable);
    CHECK_INT_EQ(matchmaking->cancelCalls.load(), 3);

    CHECK(coordinator.cancelMatchmaking());
    CHECK(waitForState(coordinator, OnlineSessionCoordinatorState::Idle));
    CHECK_INT_EQ(matchmaking->cancelCalls.load(), 5);
}

TEST_CASE(CommittedAssignmentWinsCancellationConflict) {
    ayt::test::setCurrentCase("CommittedAssignmentWinsCancellationConflict");
    std::atomic<uint64_t> now{8000};
    auto sessions = makeSessionService(now);
    P2PSessionCreateRequest create;
    create.hostPeerId = PeerId{"local"};
    create.virtualPort = 7350;
    create.capacity = 2;
    const auto created = sessions->createSession(create);
    CHECK(created);
    if (!created) return;
    auto matchmaking =
        std::make_shared<AssignmentWinsCancellationMatchmaking>(created.value);
    auto lobbies = std::make_shared<InMemoryOnlineServices>(
        InMemoryOnlineServicesConfig{}, sessions);
    OnlineCoordinatorNetwork network;
    P2PSessionCoordinator p2p(network, sessions, p2pConfig("local"));
    OnlineSessionCoordinator coordinator(
        lobbies, matchmaking, p2p, onlineConfig("local"));

    CHECK(coordinator.startMatchmaking(
        matchRequest("local", MatchTopology::P2P)));
    CHECK(waitFor(coordinator, [](const auto& status) {
        return status.state == OnlineSessionCoordinatorState::Queueing &&
               status.matchTicket.ticketId != 0;
    }));
    CHECK(coordinator.cancelMatchmaking());
    CHECK(waitForState(coordinator, OnlineSessionCoordinatorState::InSession));
    CHECK_INT_EQ(matchmaking->cancelCalls.load(), 1);
    CHECK(matchmaking->getCalls.load() >= 1);
    CHECK(coordinator.getStatus().matchTicket.state ==
          MatchTicketState::Matched);
    CHECK(network.listening);
}

TEST_CASE(OnlineSubSystemPublishesLifecycleAndLobbyListEvents) {
    ayt::test::setCurrentCase(
        "OnlineSubSystemPublishesLifecycleAndLobbyListEvents");
    std::atomic<uint64_t> now{9000};
    auto sessions = makeSessionService(now);
    auto online = std::make_shared<InMemoryOnlineServices>(
        InMemoryOnlineServicesConfig{}, sessions);
    CHECK(online->createLobby(lobbyRequest("seed-owner")));

    OnlineCoordinatorNetwork network;
    ayt::event::EventBus eventBus;
    OnlineSubSystemDependencies partialDependencies;
    partialDependencies.sessionService = sessions;
    auto partial = createOnlineSubSystem(
        network, subsystemConfig("owner"), partialDependencies, &eventBus);
    CHECK(!partial->initialize());

    uint32_t statusEventCount = 0;
    uint32_t listEventCount = 0;
    OnlineSessionStatusChangedEvent lastStatusEvent;
    OnlineLobbyListChangedEvent lastListEvent;
    const auto statusConnection =
        eventBus.subscribe<OnlineSessionStatusChangedEvent>(
            [&](const OnlineSessionStatusChangedEvent& event) {
                ++statusEventCount;
                lastStatusEvent = event;
            });
    const auto listConnection =
        eventBus.subscribe<OnlineLobbyListChangedEvent>(
            [&](const OnlineLobbyListChangedEvent& event) {
                ++listEventCount;
                lastListEvent = event;
            });

    auto subsystem = createOnlineSubSystem(
        network, subsystemConfig("owner"),
        subsystemDependencies(sessions, online), &eventBus);
    CHECK(subsystem != nullptr);
    const auto& descriptor = subsystem->getDescriptor();
    CHECK(std::string_view(descriptor.name) == "Online");
    CHECK_INT_EQ(descriptor.initializeAfter.size(), 1);
    CHECK(std::string_view(descriptor.initializeAfter.front()) == "Network");
    CHECK_INT_EQ(descriptor.runsAfter.size(), 1);
    CHECK(std::string_view(descriptor.runsAfter.front()) == "Network");
    CHECK(descriptor.phases ==
          ayt::game::phaseBit(ayt::game::FramePhase::Ingress));
    CHECK(subsystem->initialize());
    CHECK(subsystem->isReady());
    CHECK(subsystem->isAuthenticated());

    CHECK(subsystem->listLobbies({"asia", "build-1", 1, 10}));
    CHECK(waitForSubSystem(subsystem.operator*(), eventBus,
        [&](const auto& status) {
            return status.state == OnlineSessionCoordinatorState::Idle &&
                   subsystem->getLobbyListGeneration() == 1;
        }));
    CHECK_INT_EQ(subsystem->getLobbyResults().size(), 1);
    CHECK_INT_EQ(listEventCount, 1);
    CHECK_INT_EQ(lastListEvent.generation, 1);
    CHECK_INT_EQ(lastListEvent.lobbyCount, 1);

    CHECK(subsystem->createLobby(lobbyRequest("forged-owner")));
    CHECK(waitForSubSystemState(
        *subsystem, eventBus, OnlineSessionCoordinatorState::InLobby));
    const auto lobbyId = subsystem->getOnlineStatus().lobby.lobbyId;
    CHECK(lobbyId != 0);
    CHECK(subsystem->getOnlineStatus().lobby.ownerPeerId == PeerId{"owner"});
    CHECK(statusEventCount >= 4);
    CHECK(lastStatusEvent.state == OnlineSessionCoordinatorState::InLobby);
    CHECK_INT_EQ(lastStatusEvent.lobbyId, lobbyId);

    subsystem->shutdown();
    CHECK(!subsystem->isReady());
    CHECK(online->getLobby(lobbyId).error == OnlineServiceError::NotFound);
    eventBus.unsubscribe(statusConnection);
    eventBus.unsubscribe(listConnection);
}

TEST_CASE(OnlineSubSystemBuildsHttpBackendAndRotatesPlayerToken) {
    ayt::test::setCurrentCase(
        "OnlineSubSystemBuildsHttpBackendAndRotatesPlayerToken");
    std::atomic<uint64_t> now{10000};
    auto sessions = makeSessionService(now);
    auto online = std::make_shared<InMemoryOnlineServices>(
        InMemoryOnlineServicesConfig{}, sessions);

    HttpP2PSessionServerConfig serverConfig;
    serverConfig.bindAddress = "127.0.0.1";
    serverConfig.playerAuthenticator = [](std::string_view token,
                                           PeerId& peer) {
        if (token != "owner-access-token") return false;
        peer = PeerId{"owner"};
        return true;
    };
    HttpP2PSessionServer server(
        std::move(serverConfig), sessions, online, online);
    CHECK(server.start());

    auto config = subsystemConfig("owner");
    config.backend.serverAddress = "127.0.0.1";
    config.backend.serverPort = server.getBoundPort();
    config.backend.connectTimeoutMs = 500;
    config.backend.requestTimeoutMs = 1000;
    OnlineCoordinatorNetwork network;
    ayt::event::EventBus eventBus;
    auto subsystem = createOnlineSubSystem(
        network, std::move(config), {}, &eventBus);
    CHECK(subsystem->initialize());
    CHECK(subsystem->isReady());
    CHECK(!subsystem->isAuthenticated());

    CHECK(subsystem->setPlayerAccessToken("owner-access-token"));
    CHECK(subsystem->isAuthenticated());
    CHECK(subsystem->createLobby(lobbyRequest("forged-owner")));
    CHECK(waitForSubSystemState(
        *subsystem, eventBus, OnlineSessionCoordinatorState::InLobby));
    CHECK(subsystem->getOnlineStatus().lobby.ownerPeerId == PeerId{"owner"});
    CHECK(!subsystem->setPlayerAccessToken({}));

    CHECK(subsystem->leaveLobby());
    CHECK(waitForSubSystemState(
        *subsystem, eventBus, OnlineSessionCoordinatorState::Idle));
    CHECK(subsystem->setPlayerAccessToken({}));
    CHECK(!subsystem->isAuthenticated());
    CHECK(subsystem->createLobby(lobbyRequest("owner")));
    CHECK(waitForSubSystem(
        *subsystem, eventBus, [](const auto& status) {
            return status.state == OnlineSessionCoordinatorState::Failed &&
                   status.serviceError == OnlineServiceError::Unauthorized;
        }));

    CHECK(subsystem->reset());
    CHECK(subsystem->getOnlineStatus().state ==
          OnlineSessionCoordinatorState::Idle);
    subsystem->shutdown();
    server.stop();
}

TEST_CASE(OnlineFlowRunsEndToEndOverInjectedSubSystem) {
    ayt::test::setCurrentCase(
        "OnlineFlowRunsEndToEndOverInjectedSubSystem");
    std::atomic<uint64_t> now{11000};
    auto sessions = makeSessionService(now);
    auto online = std::make_shared<InMemoryOnlineServices>(
        InMemoryOnlineServicesConfig{}, sessions);
    OnlineCoordinatorNetwork network;
    ayt::event::EventBus eventBus;
    auto subsystem = createOnlineSubSystem(
        network, subsystemConfig("flow-owner"),
        subsystemDependencies(sessions, online), &eventBus);
    CHECK(subsystem->initialize());
    OnlineFlowCoordinator flow(*subsystem, {}, &eventBus);
    CHECK(flow.getStatus().state == OnlineFlowState::MainMenu);

    CHECK(flow.createLobby(lobbyRequest("forged-owner")));
    CHECK(waitForFlow(*subsystem, flow, eventBus, [](const auto& status) {
        return status.state == OnlineFlowState::InLobby;
    }));
    const LobbyId lobbyId = flow.getStatus().lobbyId;
    CHECK(lobbyId != 0);

    CHECK(flow.startLobbySession(7350));
    CHECK(waitForFlow(*subsystem, flow, eventBus, [](const auto& status) {
        return status.state == OnlineFlowState::LoadingSession;
    }));
    const uint64_t generation = flow.getStatus().loadingGeneration;
    CHECK(generation != 0);
    CHECK(flow.completeLoading(generation));
    CHECK(waitForFlow(*subsystem, flow, eventBus, [](const auto& status) {
        return status.state == OnlineFlowState::InSession;
    }));

    CHECK(flow.leaveSession());
    CHECK(waitForFlow(*subsystem, flow, eventBus, [](const auto& status) {
        return status.state == OnlineFlowState::MainMenu;
    }));
    CHECK(online->getLobby(lobbyId).error == OnlineServiceError::NotFound);
    subsystem->shutdown();
}

TEST_SUITE_END
