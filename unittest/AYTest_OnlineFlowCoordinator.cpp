// Application flow orchestration over IOnlineSubSystem.

#include <AYEventSystem/EventBus.h>
#include <AYNetwork/Session/OnlineFlowCoordinator.h>
#include <AYNetwork/Session/OnlineFlowSubSystem.h>
#include <AYTest.h>

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

using namespace ayt::net;

namespace
{

class FlowOnlineSubSystem final : public IOnlineSubSystem {
public:
    const char* getName() const override { return "FlowOnline"; }
    const ::ayt::game::SubSystemDescriptor& getDescriptor() const override {
        static ::ayt::game::SubSystemDescriptor descriptor{
            "FlowOnline", {}, 0};
        return descriptor;
    }
    bool initialize() override { ready = true; return true; }
    void shutdown() override { ready = false; }
    void update(float) override {}
    void fixedUpdate(float) override {}

    bool isReady() const override { return ready; }
    bool isAuthenticated() const override { return authenticated; }
    PeerId getLocalPeerId() const override { return PeerId{"flow-peer"}; }

    bool setPlayerAccessToken(std::string token) override {
        if (rejectCredentials) return false;
        playerToken = std::move(token);
        authenticated = !playerToken.empty();
        return true;
    }
    bool setP2PAdmissionToken(std::string token) override {
        admissionToken = std::move(token);
        return true;
    }

    bool listLobbies(ListLobbiesRequest) override {
        if (!acceptCommands) return false;
        ++listCalls;
        session.state = OnlineSessionCoordinatorState::ListingLobbies;
        return true;
    }
    bool createLobby(CreateLobbyRequest) override {
        if (!acceptCommands) return false;
        ++createCalls;
        session.state = OnlineSessionCoordinatorState::CreatingLobby;
        return true;
    }
    bool joinLobby(LobbyId lobbyId) override {
        if (!acceptCommands || lobbyId == 0) return false;
        ++joinCalls;
        session.state = OnlineSessionCoordinatorState::JoiningLobby;
        return true;
    }
    bool refreshLobby() override {
        if (!acceptCommands || session.lobby.lobbyId == 0) return false;
        ++refreshCalls;
        session.state = OnlineSessionCoordinatorState::RefreshingLobby;
        return true;
    }
    bool updateLobbyName(std::string name) override {
        if (!acceptCommands || session.lobby.lobbyId == 0 || name.empty()) {
            return false;
        }
        ++updateLobbyCalls;
        session.lobby.name = std::move(name);
        session.state = OnlineSessionCoordinatorState::UpdatingLobby;
        return true;
    }
    bool leaveLobby() override {
        if (!acceptCommands || session.lobby.lobbyId == 0) return false;
        ++leaveLobbyCalls;
        session.lobby = {};
        session.state = OnlineSessionCoordinatorState::Idle;
        return true;
    }
    bool launchLobbyP2P(uint16_t virtualPort) override {
        if (!acceptCommands || session.lobby.lobbyId == 0 || virtualPort == 0) {
            return false;
        }
        ++launchCalls;
        session.state = OnlineSessionCoordinatorState::LaunchingLobby;
        return true;
    }
    bool startMatchmaking(MatchmakingRequest) override {
        if (!acceptCommands) return false;
        ++matchCalls;
        session.matchTicket.ticketId = 41;
        session.matchTicket.state = MatchTicketState::Queued;
        session.state = OnlineSessionCoordinatorState::Queueing;
        return true;
    }
    bool cancelMatchmaking() override {
        if (!acceptCommands) return false;
        ++cancelCalls;
        session.matchTicket = {};
        session.state = session.lobby.lobbyId != 0
            ? OnlineSessionCoordinatorState::InLobby
            : OnlineSessionCoordinatorState::Idle;
        return true;
    }
    bool leaveSession() override {
        if (!acceptCommands) return false;
        ++leaveSessionCalls;
        const auto lastNetworkSession = p2p.networkSession;
        session.topology = OnlineSessionTopology::None;
        session.assignment = {};
        session.matchTicket = {};
        session.lobby = {};
        session.state = OnlineSessionCoordinatorState::Idle;
        p2p = {};
        // The real network adapter keeps its last connection identity for
        // diagnostics after disconnect. Flow cleanup must not treat it as a
        // live backend membership.
        p2p.networkSession = lastNetworkSession;
        return true;
    }
    bool reset() override {
        if (!acceptCommands) return false;
        ++resetCalls;
        session = {};
        p2p = {};
        return true;
    }

    OnlineSessionCoordinatorStatus getOnlineStatus() const override {
        return session;
    }
    P2PSessionCoordinatorStatus getP2PStatus() const override { return p2p; }
    std::vector<LobbyInfo> getLobbyResults() const override { return results; }
    uint64_t getLobbyListGeneration() const override { return listGeneration; }

    void completeList() {
        LobbyInfo info;
        info.lobbyId = 9;
        info.revision = 1;
        info.ownerPeerId = PeerId{"owner"};
        info.name = "Visible lobby";
        info.region = "asia";
        info.buildId = "build-1";
        info.content = {"maps/flow", "content-1", 11};
        info.capacity = 4;
        info.state = LobbyState::Open;
        info.members = {PeerId{"owner"}};
        results = {std::move(info)};
        ++listGeneration;
        session.state = OnlineSessionCoordinatorState::Idle;
    }

    void completeLobby() {
        session.lobby.lobbyId = 11;
        session.lobby.revision = 1;
        session.lobby.ownerPeerId = PeerId{"flow-peer"};
        session.lobby.name = "Flow lobby";
        session.lobby.region = "asia";
        session.lobby.buildId = "build-1";
        session.lobby.content = {"maps/flow", "content-1", 11};
        session.lobby.capacity = 4;
        session.lobby.state = LobbyState::Open;
        session.lobby.members = {PeerId{"flow-peer"}};
        session.state = OnlineSessionCoordinatorState::InLobby;
    }

    void connectP2PSession(bool active) {
        session.topology = OnlineSessionTopology::P2P;
        session.assignment.content = {"maps/flow", "content-1", 11};
        session.state = active ? OnlineSessionCoordinatorState::InSession
                               : OnlineSessionCoordinatorState::Connecting;
        p2p.backendSession.sessionId = 700;
        p2p.backendSession.epoch = 3;
        p2p.networkSession.sessionId = 700;
        p2p.networkSession.epoch = 3;
        p2p.state = active ? P2PSessionCoordinatorState::Active
                           : P2PSessionCoordinatorState::Connecting;
    }

    void failSession(std::string reason) {
        session.state = OnlineSessionCoordinatorState::Failed;
        session.error = OnlineSessionCoordinatorError::P2PFailed;
        session.message = std::move(reason);
    }

    bool ready = true;
    bool authenticated = false;
    bool rejectCredentials = false;
    bool acceptCommands = true;
    std::string playerToken;
    std::string admissionToken;
    OnlineSessionCoordinatorStatus session;
    P2PSessionCoordinatorStatus p2p;
    std::vector<LobbyInfo> results;
    uint64_t listGeneration = 0;
    uint32_t listCalls = 0;
    uint32_t createCalls = 0;
    uint32_t joinCalls = 0;
    uint32_t refreshCalls = 0;
    uint32_t updateLobbyCalls = 0;
    uint32_t leaveLobbyCalls = 0;
    uint32_t launchCalls = 0;
    uint32_t matchCalls = 0;
    uint32_t cancelCalls = 0;
    uint32_t leaveSessionCalls = 0;
    uint32_t resetCalls = 0;
};

MatchmakingRequest flowMatchRequest() {
    MatchmakingRequest request;
    request.queue = "default";
    request.region = "asia";
    request.buildId = "build-1";
    request.content = {"maps/flow", "content-1", 11};
    request.topology = MatchTopology::P2P;
    request.targetPlayers = 2;
    request.virtualPort = 7350;
    return request;
}

CreateLobbyRequest flowLobbyRequest() {
    CreateLobbyRequest request;
    request.name = "Flow lobby";
    request.region = "asia";
    request.buildId = "build-1";
    request.content = {"maps/flow", "content-1", 11};
    request.capacity = 4;
    return request;
}

} // namespace

TEST_SUITE(OnlineFlowCoordinator)

TEST_CASE(SignInBrowseLobbyAndSignOutFlow) {
    ayt::test::setCurrentCase("SignInBrowseLobbyAndSignOutFlow");
    FlowOnlineSubSystem online;
    ayt::event::EventBus eventBus;
    uint32_t statusEvents = 0;
    const auto connection = eventBus.subscribe<OnlineFlowStatusChangedEvent>(
        [&](const OnlineFlowStatusChangedEvent&) { ++statusEvents; });
    OnlineFlowCoordinator flow(online, {}, &eventBus);

    CHECK(flow.getStatus().state == OnlineFlowState::SignedOut);
    CHECK(!flow.browseLobbies());
    CHECK(flow.getStatus().error ==
          OnlineFlowError::AuthenticationRequired);
    CHECK(flow.signIn("player-token", "admission-token"));
    CHECK(flow.getStatus().state == OnlineFlowState::MainMenu);
    CHECK(online.playerToken == "player-token");
    CHECK(online.admissionToken == "admission-token");

    CHECK(flow.browseLobbies({"asia", "build-1", 1, 10}));
    CHECK(flow.getStatus().state == OnlineFlowState::BrowsingLobbies);
    online.completeList();
    flow.update();
    CHECK(flow.getStatus().state == OnlineFlowState::BrowsingLobbies);
    CHECK_INT_EQ(flow.getLobbyResults().size(), 1);

    CHECK(flow.createLobby(flowLobbyRequest()));
    CHECK(flow.getStatus().state == OnlineFlowState::CreatingLobby);
    online.completeLobby();
    flow.update();
    CHECK(flow.getStatus().state == OnlineFlowState::InLobby);
    CHECK_INT_EQ(flow.getStatus().lobbyId, 11);

    CHECK(flow.signOut());
    CHECK(flow.getStatus().state == OnlineFlowState::SigningOut);
    CHECK_INT_EQ(online.leaveLobbyCalls, 1);
    flow.update();
    CHECK(flow.getStatus().state == OnlineFlowState::SignedOut);
    CHECK(online.playerToken.empty());
    CHECK(online.admissionToken.empty());

    eventBus.pump();
    CHECK(statusEvents >= 6);
    eventBus.unsubscribe(connection);
}

TEST_CASE(LoadingWaitsForNetworkAndGenerationFencedWorld) {
    ayt::test::setCurrentCase(
        "LoadingWaitsForNetworkAndGenerationFencedWorld");
    FlowOnlineSubSystem online;
    online.authenticated = true;
    ayt::event::EventBus eventBus;
    OnlineFlowLoadRequestedEvent loadEvent;
    uint32_t loadEvents = 0;
    const auto connection = eventBus.subscribe<OnlineFlowLoadRequestedEvent>(
        [&](const OnlineFlowLoadRequestedEvent& event) {
            ++loadEvents;
            loadEvent = event;
        });
    OnlineFlowCoordinator flow(online, {}, &eventBus);

    CHECK(flow.getStatus().state == OnlineFlowState::MainMenu);
    CHECK(flow.startMatchmaking(flowMatchRequest()));
    CHECK(flow.getStatus().state == OnlineFlowState::Matchmaking);
    online.connectP2PSession(false);
    flow.update();
    eventBus.pump();

    const auto generation = flow.getStatus().loadingGeneration;
    CHECK(generation != 0);
    CHECK(flow.getStatus().state == OnlineFlowState::LoadingSession);
    CHECK_INT_EQ(loadEvents, 1);
    CHECK_INT_EQ(loadEvent.generation, generation);
    CHECK_INT_EQ(loadEvent.sessionId, 700);
    CHECK(!flow.completeLoading(generation + 1));
    CHECK(flow.completeLoading(generation));
    CHECK(flow.getStatus().worldLoaded);
    CHECK(flow.getStatus().state == OnlineFlowState::LoadingSession);

    online.connectP2PSession(true);
    flow.update();
    CHECK(flow.getStatus().state == OnlineFlowState::InSession);
    CHECK(flow.leaveSession());
    CHECK(flow.getStatus().state == OnlineFlowState::Leaving);
    CHECK_INT_EQ(online.leaveSessionCalls, 1);
    flow.update();
    CHECK(flow.getStatus().state == OnlineFlowState::MainMenu);
    eventBus.unsubscribe(connection);
}

TEST_CASE(LoadingTimeoutCleansSessionAndCanRecover) {
    ayt::test::setCurrentCase("LoadingTimeoutCleansSessionAndCanRecover");
    uint64_t now = 100;
    FlowOnlineSubSystem online;
    online.authenticated = true;
    OnlineFlowConfig config;
    config.loadingTimeoutMs = 10;
    config.nowMilliseconds = [&now] { return now; };
    OnlineFlowCoordinator flow(online, std::move(config));

    CHECK(flow.startMatchmaking(flowMatchRequest()));
    online.connectP2PSession(false);
    flow.update();
    CHECK(flow.getStatus().state == OnlineFlowState::LoadingSession);
    now = 111;
    flow.update();
    CHECK(flow.getStatus().state == OnlineFlowState::Leaving);
    CHECK(flow.getStatus().error == OnlineFlowError::LoadingTimedOut);
    CHECK_INT_EQ(online.leaveSessionCalls, 1);
    flow.update();
    CHECK(flow.getStatus().state == OnlineFlowState::Failed);
    CHECK(flow.getStatus().error == OnlineFlowError::LoadingTimedOut);

    CHECK(flow.recover());
    CHECK(flow.getStatus().state == OnlineFlowState::MainMenu);
    CHECK(flow.getStatus().error == OnlineFlowError::None);
}

TEST_CASE(SessionFailureIsSurfacedAfterFailClosedCleanup) {
    ayt::test::setCurrentCase(
        "SessionFailureIsSurfacedAfterFailClosedCleanup");
    FlowOnlineSubSystem online;
    online.authenticated = true;
    OnlineFlowCoordinator flow(online);

    CHECK(flow.startMatchmaking(flowMatchRequest()));
    online.connectP2PSession(false);
    flow.update();
    online.failSession("transport failed");
    flow.update();
    CHECK(flow.getStatus().state == OnlineFlowState::Leaving);
    CHECK(flow.getStatus().error == OnlineFlowError::SessionFailed);
    CHECK_INT_EQ(online.leaveSessionCalls, 1);
    flow.update();
    CHECK(flow.getStatus().state == OnlineFlowState::Failed);
    CHECK(flow.getStatus().message == "transport failed");
}

TEST_CASE(ActiveWorldFailureIsSurfacedAfterFailClosedCleanup) {
    ayt::test::setCurrentCase(
        "ActiveWorldFailureIsSurfacedAfterFailClosedCleanup");
    FlowOnlineSubSystem online;
    online.authenticated = true;
    OnlineFlowCoordinator flow(online);

    online.connectP2PSession(false);
    flow.update();
    const auto generation = flow.getStatus().loadingGeneration;
    CHECK(flow.completeLoading(generation));
    online.connectP2PSession(true);
    flow.update();
    CHECK(flow.getStatus().state == OnlineFlowState::InSession);

    CHECK(flow.failActiveSession("scene recovery failed"));
    CHECK(flow.getStatus().state == OnlineFlowState::Leaving);
    CHECK(flow.getStatus().error == OnlineFlowError::WorldFailed);
    CHECK_INT_EQ(online.leaveSessionCalls, 1);
    flow.update();
    CHECK(flow.getStatus().state == OnlineFlowState::Failed);
    CHECK(flow.getStatus().error == OnlineFlowError::WorldFailed);
    CHECK(flow.getStatus().message == "scene recovery failed");
}

TEST_CASE(CleanupTimeoutFailsClosedAndPreservesCredential) {
    ayt::test::setCurrentCase(
        "CleanupTimeoutFailsClosedAndPreservesCredential");
    uint64_t now = 500;
    FlowOnlineSubSystem online;
    online.authenticated = true;
    online.playerToken = "still-needed";
    online.completeLobby();
    online.acceptCommands = false;
    OnlineFlowConfig config;
    config.cleanupTimeoutMs = 10;
    config.nowMilliseconds = [&now] { return now; };
    OnlineFlowCoordinator flow(online, std::move(config));

    CHECK(flow.getStatus().state == OnlineFlowState::MainMenu);
    flow.update();
    CHECK(flow.getStatus().state == OnlineFlowState::InLobby);
    CHECK(flow.signOut());
    CHECK(flow.getStatus().state == OnlineFlowState::SigningOut);
    now = 511;
    flow.update();
    CHECK(flow.getStatus().state == OnlineFlowState::Failed);
    CHECK(flow.getStatus().error == OnlineFlowError::CleanupFailed);
    CHECK(online.playerToken == "still-needed");
}

TEST_CASE(CredentialsRotateWithoutRecreatingActiveFlow) {
    ayt::test::setCurrentCase(
        "CredentialsRotateWithoutRecreatingActiveFlow");
    FlowOnlineSubSystem online;
    online.authenticated = true;
    online.playerToken = "old-player";
    OnlineFlowCoordinator flow(online);

    CHECK(flow.refreshCredentials("new-player", "new-admission"));
    CHECK(flow.getStatus().state == OnlineFlowState::MainMenu);
    CHECK(online.playerToken == "new-player");
    CHECK(online.admissionToken == "new-admission");
    CHECK(!flow.refreshCredentials({}));
    CHECK(online.playerToken == "new-player");
}

TEST_CASE(CommandsRejectWrongStateWithoutMutatingBackend) {
    ayt::test::setCurrentCase(
        "CommandsRejectWrongStateWithoutMutatingBackend");
    FlowOnlineSubSystem online;
    online.authenticated = true;
    OnlineFlowCoordinator flow(online);

    CHECK(!flow.startLobbySession(7350));
    CHECK(flow.getStatus().error == OnlineFlowError::InvalidState);
    CHECK_INT_EQ(online.launchCalls, 0);
    online.acceptCommands = false;
    CHECK(!flow.startMatchmaking(flowMatchRequest()));
    CHECK(flow.getStatus().error == OnlineFlowError::CommandRejected);
    CHECK_INT_EQ(online.matchCalls, 0);
}

TEST_CASE(FlowSubSystemRunsAfterOnlineAndOwnsCoordinatorLifecycle) {
    ayt::test::setCurrentCase(
        "FlowSubSystemRunsAfterOnlineAndOwnsCoordinatorLifecycle");
    FlowOnlineSubSystem online;
    online.authenticated = true;
    ayt::event::EventBus eventBus;
    auto subsystem = createOnlineFlowSubSystem(online, {}, &eventBus);

    CHECK(subsystem != nullptr);
    CHECK(subsystem->getDescriptor().phases ==
          ayt::game::phaseBit(ayt::game::FramePhase::Ingress));
    CHECK(subsystem->getDescriptor().runsAfter.size() == 1);
    CHECK(std::string{subsystem->getDescriptor().runsAfter.front()} == "Online");
    CHECK(subsystem->initialize());
    CHECK(subsystem->isReady());
    CHECK(subsystem->coordinator() != nullptr);
    CHECK(subsystem->coordinator()->getStatus().state ==
          OnlineFlowState::MainMenu);

    subsystem->update(0.0f);
    subsystem->shutdown();
    CHECK(!subsystem->isReady());
    CHECK(subsystem->coordinator() == nullptr);
}

TEST_SUITE_END
