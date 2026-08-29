#include <AYNetwork/Session/OnlineFlowCoordinator.h>

#include <AYEventSystem/EventBus.h>

#include <algorithm>
#include <chrono>
#include <initializer_list>
#include <utility>

namespace ayt::net
{
namespace
{

constexpr uint32_t kMaximumLoadingTimeoutMs = 30u * 60u * 1000u;
constexpr uint32_t kMaximumCleanupTimeoutMs = 5u * 60u * 1000u;

uint64_t steadyMilliseconds() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count());
}

bool isLiveTicket(const MatchTicketInfo& ticket) {
    return ticket.ticketId != 0 &&
        (ticket.state == MatchTicketState::Queued ||
         ticket.state == MatchTicketState::Matching ||
         ticket.state == MatchTicketState::AwaitingAcceptance);
}

bool containsPeer(const std::vector<PeerId>& peers, const PeerId& peer) {
    return std::find(peers.begin(), peers.end(), peer) != peers.end();
}

bool samePublishedStatus(const OnlineFlowStatus& a,
                         const OnlineFlowStatus& b) {
    return a.state == b.state && a.error == b.error &&
           a.sessionState == b.sessionState &&
           a.sessionError == b.sessionError &&
           a.serviceError == b.serviceError && a.p2pError == b.p2pError &&
           a.topology == b.topology &&
           a.content == b.content &&
           a.loadingGeneration == b.loadingGeneration &&
           a.worldLoaded == b.worldLoaded && a.lobbyId == b.lobbyId &&
           a.matchTicketId == b.matchTicketId &&
           a.matchTicketState == b.matchTicketState &&
           a.acceptedPartyMembers == b.acceptedPartyMembers &&
           a.requiredPartyMembers == b.requiredPartyMembers &&
           a.matchAcceptanceRequired == b.matchAcceptanceRequired &&
           a.localMatchAccepted == b.localMatchAccepted &&
           a.matchAcceptanceExpiresAtUnixSeconds ==
               b.matchAcceptanceExpiresAtUnixSeconds &&
           a.sessionId == b.sessionId && a.sessionEpoch == b.sessionEpoch &&
           a.dedicatedAllocationId == b.dedicatedAllocationId &&
           a.message == b.message;
}

} // namespace

struct OnlineFlowCoordinator::Impl {
    enum class ExitTarget : uint8_t { None = 0, MainMenu, SignedOut, Failed };

    Impl(IOnlineSubSystem& inputOnline,
         OnlineFlowConfig inputConfig,
         ::ayt::event::EventBus* inputEventBus)
        : online(inputOnline),
          config(std::move(inputConfig)),
          eventBus(inputEventBus ? inputEventBus
                                 : &::ayt::event::EventBus::instance()) {
        if (!config.nowMilliseconds) {
            config.nowMilliseconds = steadyMilliseconds;
        }
        if (!config.isValid() || !online.isReady()) {
            state = OnlineFlowState::Failed;
            error = !config.isValid()
                ? OnlineFlowError::InvalidConfiguration
                : OnlineFlowError::SubSystemNotReady;
            message = !config.isValid()
                ? "Online flow configuration is invalid"
                : "Online subsystem is not ready";
        } else {
            state = online.isAuthenticated()
                ? OnlineFlowState::MainMenu : OnlineFlowState::SignedOut;
        }
        lastPublished = snapshot();
    }

    uint64_t now() const { return config.nowMilliseconds(); }

    OnlineFlowStatus snapshot() const {
        const auto session = online.getOnlineStatus();
        const auto p2p = online.getP2PStatus();
        OnlineFlowStatus out;
        out.state = state;
        out.error = error;
        out.message = message;
        out.sessionState = session.state;
        out.sessionError = session.error;
        out.serviceError = session.serviceError;
        out.p2pError = session.p2pError != P2PSessionCoordinatorError::None
            ? session.p2pError : p2p.error;
        out.topology = session.topology;
        out.content = session.assignment.content.isValid()
            ? session.assignment.content : session.lobby.content;
        out.loadingGeneration = loadingGeneration;
        out.worldLoaded = worldLoaded;
        out.lobbyId = session.lobby.lobbyId;
        out.matchTicketId = session.matchTicket.ticketId;
        out.matchTicketState = session.matchTicket.state;
        out.acceptedPartyMembers = static_cast<uint16_t>(
            session.matchTicket.acceptedMembers.size());
        out.requiredPartyMembers = static_cast<uint16_t>(
            session.matchTicket.request.partyMembers.size());
        out.matchAcceptanceRequired = session.matchTicket.state ==
            MatchTicketState::AwaitingAcceptance;
        out.localMatchAccepted = containsPeer(
            session.matchTicket.acceptedMembers, online.getLocalPeerId());
        out.matchAcceptanceExpiresAtUnixSeconds =
            session.matchTicket.acceptanceExpiresAtUnixSeconds;
        out.sessionId = p2p.backendSession.sessionId != 0
            ? p2p.backendSession.sessionId : session.lobby.sessionId;
        out.sessionEpoch = p2p.backendSession.epoch;
        out.dedicatedAllocationId =
            session.assignment.dedicated.allocationId;
        return out;
    }

    void publishIfChanged() {
        const auto current = snapshot();
        if (samePublishedStatus(lastPublished, current)) return;
        OnlineFlowStatusChangedEvent event;
        event.previousState = lastPublished.state;
        event.state = current.state;
        event.error = current.error;
        event.sessionState = current.sessionState;
        event.sessionError = current.sessionError;
        event.serviceError = current.serviceError;
        event.p2pError = current.p2pError;
        event.topology = current.topology;
        event.loadingGeneration = current.loadingGeneration;
        event.lobbyId = current.lobbyId;
        event.matchTicketId = current.matchTicketId;
        event.matchTicketState = current.matchTicketState;
        event.acceptedPartyMembers = current.acceptedPartyMembers;
        event.requiredPartyMembers = current.requiredPartyMembers;
        event.matchAcceptanceRequired = current.matchAcceptanceRequired;
        event.localMatchAccepted = current.localMatchAccepted;
        event.matchAcceptanceExpiresAtUnixSeconds =
            current.matchAcceptanceExpiresAtUnixSeconds;
        event.sessionId = current.sessionId;
        event.sessionEpoch = current.sessionEpoch;
        event.dedicatedAllocationId = current.dedicatedAllocationId;
        event.worldLoaded = current.worldLoaded;
        eventBus->post(event);
        lastPublished = current;
    }

    void clearError() {
        error = OnlineFlowError::None;
        message.clear();
    }

    void setState(OnlineFlowState next) {
        state = next;
        publishIfChanged();
    }

    bool reject(OnlineFlowError value, std::string reason) {
        error = value;
        message = std::move(reason);
        publishIfChanged();
        return false;
    }

    bool canIssueFrom(std::initializer_list<OnlineFlowState> allowed) {
        if (!online.isReady()) {
            return reject(OnlineFlowError::SubSystemNotReady,
                          "Online subsystem is not ready");
        }
        if (!online.isAuthenticated()) {
            return reject(OnlineFlowError::AuthenticationRequired,
                          "A player access token is required");
        }
        if (exitTarget != ExitTarget::None) {
            return reject(OnlineFlowError::InvalidState,
                          "Online cleanup is already in progress");
        }
        for (const auto value : allowed) {
            if (state == value) return true;
        }
        return reject(OnlineFlowError::InvalidState,
                      "Command is not valid in the current online flow state");
    }

    template <typename Command>
    bool issue(std::initializer_list<OnlineFlowState> allowed,
               OnlineFlowState acceptedState,
               Command&& command) {
        if (!canIssueFrom(allowed)) return false;
        if (!command()) {
            return reject(OnlineFlowError::CommandRejected,
                          "Online subsystem rejected the command");
        }
        clearError();
        setState(acceptedState);
        return true;
    }

    void postLoadRequest() {
        const auto status = snapshot();
        OnlineFlowLoadRequestedEvent event;
        event.generation = status.loadingGeneration;
        event.topology = status.topology;
        event.lobbyId = status.lobbyId;
        event.matchTicketId = status.matchTicketId;
        event.sessionId = status.sessionId;
        event.sessionEpoch = status.sessionEpoch;
        event.dedicatedAllocationId = status.dedicatedAllocationId;
        eventBus->post(event);
    }

    void beginLoading() {
        if (state == OnlineFlowState::LoadingSession ||
            state == OnlineFlowState::InSession) {
            // During reconnect/host migration the application flow remains
            // InSession, but consumers still need the changed lower-level
            // sessionState/epoch to suspend and later restore scene bindings.
            publishIfChanged();
            return;
        }
        ++loadingGeneration;
        worldLoaded = false;
        loadingStartedAtMs = now();
        clearError();
        setState(OnlineFlowState::LoadingSession);
        postLoadRequest();
    }

    void finishExit() {
        const ExitTarget target = exitTarget;
        exitTarget = ExitTarget::None;
        exitStartedAtMs = 0;
        worldLoaded = false;
        loadingStartedAtMs = 0;

        if (target == ExitTarget::SignedOut) {
            if (!online.setPlayerAccessToken({})) {
                error = OnlineFlowError::CleanupFailed;
                message = "Online resources prevented credential clearing";
                setState(OnlineFlowState::Failed);
                return;
            }
            (void)online.setP2PAdmissionToken({});
            clearError();
            setState(OnlineFlowState::SignedOut);
            return;
        }
        if (target == ExitTarget::Failed) {
            setState(OnlineFlowState::Failed);
            return;
        }
        clearError();
        setState(online.isAuthenticated()
            ? OnlineFlowState::MainMenu : OnlineFlowState::SignedOut);
    }

    void driveExit() {
        if (exitTarget == ExitTarget::None) return;
        if (now() - exitStartedAtMs >= config.cleanupTimeoutMs) {
            exitTarget = ExitTarget::None;
            exitStartedAtMs = 0;
            error = OnlineFlowError::CleanupFailed;
            message = "Timed out cleaning online resources";
            setState(OnlineFlowState::Failed);
            return;
        }
        const auto status = online.getOnlineStatus();
        const auto p2p = online.getP2PStatus();

        // A committed assignment wins a cancellation race. Prefer tearing down
        // its transport even if an adapter still exposes a stale queued ticket.
        // networkSession is only a diagnostic snapshot and may retain the last
        // session id after disconnect, so it is not proof of live membership.
        if (status.topology != OnlineSessionTopology::None ||
            p2p.backendSession.sessionId != 0 ||
            status.state == OnlineSessionCoordinatorState::Assigned ||
            status.state == OnlineSessionCoordinatorState::Connecting ||
            status.state == OnlineSessionCoordinatorState::InSession ||
            status.state == OnlineSessionCoordinatorState::LeavingSession) {
            if (status.state != OnlineSessionCoordinatorState::LeavingSession) {
                (void)online.leaveSession();
            }
            publishIfChanged();
            return;
        }
        if (isLiveTicket(status.matchTicket) ||
            status.state == OnlineSessionCoordinatorState::Queueing ||
            status.state ==
                OnlineSessionCoordinatorState::AwaitingMatchAcceptance ||
            status.state == OnlineSessionCoordinatorState::CancellingMatch) {
            if (status.state != OnlineSessionCoordinatorState::CancellingMatch) {
                (void)online.cancelMatchmaking();
            }
            publishIfChanged();
            return;
        }
        if (status.lobby.lobbyId != 0) {
            if (status.state == OnlineSessionCoordinatorState::InLobby ||
                status.state == OnlineSessionCoordinatorState::Failed) {
                (void)online.leaveLobby();
            }
            publishIfChanged();
            return;
        }
        if (status.state == OnlineSessionCoordinatorState::Failed) {
            if (exitTarget == ExitTarget::Failed) {
                finishExit();
            } else {
                (void)online.reset();
                publishIfChanged();
            }
            return;
        }
        if (status.state != OnlineSessionCoordinatorState::Idle) {
            publishIfChanged();
            return;
        }
        finishExit();
    }

    bool beginExit(ExitTarget target,
                   OnlineFlowError terminalError = OnlineFlowError::None,
                   std::string terminalMessage = {}) {
        if (!online.isReady()) {
            return reject(OnlineFlowError::SubSystemNotReady,
                          "Online subsystem is not ready");
        }
        if (exitTarget != ExitTarget::None) return true;
        exitTarget = target;
        exitStartedAtMs = now();
        if (terminalError != OnlineFlowError::None) {
            error = terminalError;
            message = std::move(terminalMessage);
        } else {
            clearError();
        }
        setState(target == ExitTarget::SignedOut
            ? OnlineFlowState::SigningOut : OnlineFlowState::Leaving);
        driveExit();
        return true;
    }

    void synchronizeSessionState() {
        const auto status = online.getOnlineStatus();
        if (status.state == OnlineSessionCoordinatorState::Failed) {
            (void)beginExit(ExitTarget::Failed,
                            OnlineFlowError::SessionFailed,
                            status.message.empty()
                                ? "Online session failed" : status.message);
            return;
        }

        switch (status.state) {
        case OnlineSessionCoordinatorState::ListingLobbies:
            setState(OnlineFlowState::BrowsingLobbies);
            break;
        case OnlineSessionCoordinatorState::CreatingLobby:
            setState(OnlineFlowState::CreatingLobby);
            break;
        case OnlineSessionCoordinatorState::JoiningLobby:
            setState(OnlineFlowState::JoiningLobby);
            break;
        case OnlineSessionCoordinatorState::InLobby:
        case OnlineSessionCoordinatorState::RefreshingLobby:
        case OnlineSessionCoordinatorState::UpdatingLobby:
        case OnlineSessionCoordinatorState::CreatingLobbyInvitation:
            setState(OnlineFlowState::InLobby);
            break;
        case OnlineSessionCoordinatorState::LaunchingLobby:
        case OnlineSessionCoordinatorState::Assigned:
            setState(OnlineFlowState::StartingSession);
            break;
        case OnlineSessionCoordinatorState::Queueing:
        case OnlineSessionCoordinatorState::CancellingMatch:
            setState(OnlineFlowState::Matchmaking);
            break;
        case OnlineSessionCoordinatorState::AwaitingMatchAcceptance:
            setState(OnlineFlowState::MatchAcceptance);
            break;
        case OnlineSessionCoordinatorState::Connecting:
        case OnlineSessionCoordinatorState::InSession:
            beginLoading();
            if (status.state == OnlineSessionCoordinatorState::InSession &&
                worldLoaded) {
                setState(OnlineFlowState::InSession);
            }
            break;
        case OnlineSessionCoordinatorState::LeavingLobby:
        case OnlineSessionCoordinatorState::LeavingSession:
            if (exitTarget == ExitTarget::None) {
                exitTarget = ExitTarget::MainMenu;
                exitStartedAtMs = now();
                setState(OnlineFlowState::Leaving);
            }
            break;
        case OnlineSessionCoordinatorState::Idle:
            // Listing completes back to Idle while the application remains on
            // the browser screen. Explicit cleanup intents are handled above.
            if (state != OnlineFlowState::BrowsingLobbies &&
                state != OnlineFlowState::MainMenu &&
                state != OnlineFlowState::SignedOut) {
                setState(online.isAuthenticated()
                    ? OnlineFlowState::MainMenu : OnlineFlowState::SignedOut);
            } else {
                publishIfChanged();
            }
            break;
        default:
            publishIfChanged();
            break;
        }
    }

    IOnlineSubSystem& online;
    OnlineFlowConfig config;
    ::ayt::event::EventBus* eventBus = nullptr;
    OnlineFlowState state = OnlineFlowState::SignedOut;
    OnlineFlowError error = OnlineFlowError::None;
    std::string message;
    ExitTarget exitTarget = ExitTarget::None;
    uint64_t loadingGeneration = 0;
    uint64_t loadingStartedAtMs = 0;
    uint64_t exitStartedAtMs = 0;
    bool worldLoaded = false;
    OnlineFlowStatus lastPublished;
};

bool OnlineFlowConfig::isValid() const {
    return loadingTimeoutMs <= kMaximumLoadingTimeoutMs &&
           cleanupTimeoutMs != 0 &&
           cleanupTimeoutMs <= kMaximumCleanupTimeoutMs;
}

OnlineFlowCoordinator::OnlineFlowCoordinator(
    IOnlineSubSystem& online,
    OnlineFlowConfig config,
    ::ayt::event::EventBus* eventBus)
    : _impl(std::make_unique<Impl>(
          online, std::move(config), eventBus)) {}

OnlineFlowCoordinator::~OnlineFlowCoordinator() = default;

bool OnlineFlowCoordinator::signIn(std::string playerAccessToken,
                                   std::string p2pAdmissionToken) {
    if (!_impl->online.isReady()) {
        return _impl->reject(OnlineFlowError::SubSystemNotReady,
                             "Online subsystem is not ready");
    }
    if (_impl->state != OnlineFlowState::SignedOut ||
        _impl->exitTarget != Impl::ExitTarget::None) {
        return _impl->reject(OnlineFlowError::InvalidState,
                             "Sign-in requires the SignedOut state");
    }
    if (playerAccessToken.empty()) {
        return _impl->reject(OnlineFlowError::AuthenticationRequired,
                             "Player access token is empty");
    }
    if (!p2pAdmissionToken.empty() &&
        !_impl->online.setP2PAdmissionToken(std::move(p2pAdmissionToken))) {
        return _impl->reject(OnlineFlowError::CommandRejected,
                             "P2P admission token was rejected");
    }
    if (!_impl->online.setPlayerAccessToken(std::move(playerAccessToken)) ||
        !_impl->online.isAuthenticated()) {
        return _impl->reject(OnlineFlowError::CommandRejected,
                             "Player credential was rejected");
    }
    _impl->clearError();
    _impl->setState(OnlineFlowState::MainMenu);
    return true;
}

bool OnlineFlowCoordinator::refreshCredentials(
    std::string playerAccessToken,
    std::string p2pAdmissionToken) {
    if (!_impl->online.isReady()) {
        return _impl->reject(OnlineFlowError::SubSystemNotReady,
                             "Online subsystem is not ready");
    }
    if (_impl->state == OnlineFlowState::SignedOut ||
        _impl->state == OnlineFlowState::SigningOut ||
        _impl->exitTarget == Impl::ExitTarget::SignedOut) {
        return _impl->reject(OnlineFlowError::InvalidState,
                             "Use signIn to establish a signed-out flow");
    }
    if (playerAccessToken.empty()) {
        return _impl->reject(OnlineFlowError::AuthenticationRequired,
                             "Player access token is empty");
    }
    if (!p2pAdmissionToken.empty() &&
        !_impl->online.setP2PAdmissionToken(std::move(p2pAdmissionToken))) {
        return _impl->reject(OnlineFlowError::CommandRejected,
                             "P2P admission token was rejected");
    }
    if (!_impl->online.setPlayerAccessToken(std::move(playerAccessToken)) ||
        !_impl->online.isAuthenticated()) {
        return _impl->reject(OnlineFlowError::CommandRejected,
                             "Player credential was rejected");
    }
    if (_impl->error == OnlineFlowError::AuthenticationRequired ||
        _impl->snapshot().serviceError == OnlineServiceError::Unauthorized) {
        _impl->clearError();
    }
    _impl->publishIfChanged();
    return true;
}

bool OnlineFlowCoordinator::signOut() {
    if (_impl->state == OnlineFlowState::SignedOut) return true;
    return _impl->beginExit(Impl::ExitTarget::SignedOut);
}

bool OnlineFlowCoordinator::browseLobbies(ListLobbiesRequest request) {
    return _impl->issue(
        {OnlineFlowState::MainMenu, OnlineFlowState::BrowsingLobbies},
        OnlineFlowState::BrowsingLobbies,
        [&] { return _impl->online.listLobbies(std::move(request)); });
}

bool OnlineFlowCoordinator::createLobby(CreateLobbyRequest request) {
    return _impl->issue(
        {OnlineFlowState::MainMenu, OnlineFlowState::BrowsingLobbies},
        OnlineFlowState::CreatingLobby,
        [&] { return _impl->online.createLobby(std::move(request)); });
}

bool OnlineFlowCoordinator::joinLobby(LobbyId lobbyId) {
    return _impl->issue(
        {OnlineFlowState::MainMenu, OnlineFlowState::BrowsingLobbies},
        OnlineFlowState::JoiningLobby,
        [&] { return _impl->online.joinLobby(lobbyId); });
}

bool OnlineFlowCoordinator::joinLobby(JoinLobbyRequest request) {
    return _impl->issue(
        {OnlineFlowState::MainMenu, OnlineFlowState::BrowsingLobbies},
        OnlineFlowState::JoiningLobby,
        [&] { return _impl->online.joinLobby(std::move(request)); });
}

bool OnlineFlowCoordinator::refreshLobby() {
    return _impl->issue(
        {OnlineFlowState::InLobby}, OnlineFlowState::InLobby,
        [&] { return _impl->online.refreshLobby(); });
}

bool OnlineFlowCoordinator::updateLobby(UpdateLobbyRequest request) {
    return _impl->issue(
        {OnlineFlowState::InLobby}, OnlineFlowState::InLobby,
        [&] { return _impl->online.updateLobby(std::move(request)); });
}

bool OnlineFlowCoordinator::updateLobbyName(std::string name) {
    return _impl->issue(
        {OnlineFlowState::InLobby}, OnlineFlowState::InLobby,
        [&] { return _impl->online.updateLobbyName(std::move(name)); });
}

bool OnlineFlowCoordinator::createLobbyInvitation(
    uint32_t lifetimeSeconds, uint16_t maxUses) {
    return _impl->issue(
        {OnlineFlowState::InLobby}, OnlineFlowState::InLobby,
        [&] { return _impl->online.createLobbyInvitation(
            lifetimeSeconds, maxUses); });
}

LobbyInvitation OnlineFlowCoordinator::takeLobbyInvitation() {
    return _impl->online.takeLobbyInvitation();
}

bool OnlineFlowCoordinator::leaveLobby() {
    if (!_impl->canIssueFrom({OnlineFlowState::InLobby})) return false;
    return _impl->beginExit(Impl::ExitTarget::MainMenu);
}

bool OnlineFlowCoordinator::startLobbySession(uint16_t virtualPort) {
    return _impl->issue(
        {OnlineFlowState::InLobby}, OnlineFlowState::StartingSession,
        [&] { return _impl->online.launchLobbyP2P(virtualPort); });
}

bool OnlineFlowCoordinator::startMatchmaking(MatchmakingRequest request) {
    return _impl->issue(
        {OnlineFlowState::MainMenu, OnlineFlowState::BrowsingLobbies,
         OnlineFlowState::InLobby},
        OnlineFlowState::Matchmaking,
        [&] { return _impl->online.startMatchmaking(std::move(request)); });
}

bool OnlineFlowCoordinator::respondToMatch(bool accept) {
    return _impl->issue(
        {OnlineFlowState::MatchAcceptance},
        accept ? OnlineFlowState::Matchmaking : OnlineFlowState::MatchAcceptance,
        [&] { return _impl->online.respondToMatch(accept); });
}

bool OnlineFlowCoordinator::cancelMatchmaking() {
    if (!_impl->canIssueFrom({OnlineFlowState::Matchmaking,
                              OnlineFlowState::MatchAcceptance})) return false;
    return _impl->beginExit(Impl::ExitTarget::MainMenu);
}

bool OnlineFlowCoordinator::leaveSession() {
    if (!_impl->canIssueFrom({OnlineFlowState::StartingSession,
                              OnlineFlowState::LoadingSession,
                              OnlineFlowState::InSession})) return false;
    return _impl->beginExit(Impl::ExitTarget::MainMenu);
}

bool OnlineFlowCoordinator::returnToMainMenu() {
    if (!_impl->canIssueFrom({OnlineFlowState::BrowsingLobbies,
                              OnlineFlowState::CreatingLobby,
                              OnlineFlowState::JoiningLobby,
                              OnlineFlowState::InLobby,
                              OnlineFlowState::Matchmaking,
                              OnlineFlowState::MatchAcceptance,
                              OnlineFlowState::StartingSession,
                              OnlineFlowState::LoadingSession,
                              OnlineFlowState::InSession})) return false;
    return _impl->beginExit(Impl::ExitTarget::MainMenu);
}

bool OnlineFlowCoordinator::completeLoading(uint64_t generation) {
    if (_impl->state != OnlineFlowState::LoadingSession || generation == 0 ||
        generation != _impl->loadingGeneration ||
        _impl->exitTarget != Impl::ExitTarget::None) {
        return _impl->reject(OnlineFlowError::InvalidState,
                             "Loading completion is stale or not expected");
    }
    _impl->clearError();
    _impl->worldLoaded = true;
    if (_impl->online.getOnlineStatus().state ==
        OnlineSessionCoordinatorState::InSession) {
        _impl->setState(OnlineFlowState::InSession);
    } else {
        _impl->publishIfChanged();
    }
    return true;
}

bool OnlineFlowCoordinator::failLoading(uint64_t generation,
                                        std::string message) {
    if (_impl->state != OnlineFlowState::LoadingSession || generation == 0 ||
        generation != _impl->loadingGeneration ||
        _impl->exitTarget != Impl::ExitTarget::None) {
        return _impl->reject(OnlineFlowError::InvalidState,
                             "Loading failure is stale or not expected");
    }
    return _impl->beginExit(
        Impl::ExitTarget::Failed, OnlineFlowError::LoadingFailed,
        message.empty() ? "World loading failed" : std::move(message));
}

bool OnlineFlowCoordinator::failActiveSession(std::string message) {
    if (_impl->state != OnlineFlowState::InSession ||
        _impl->exitTarget != Impl::ExitTarget::None) {
        return _impl->reject(OnlineFlowError::InvalidState,
                             "Active world failure requires InSession");
    }
    return _impl->beginExit(
        Impl::ExitTarget::Failed, OnlineFlowError::WorldFailed,
        message.empty() ? "Active session world failed" : std::move(message));
}

bool OnlineFlowCoordinator::recover() {
    if (_impl->state != OnlineFlowState::Failed ||
        _impl->exitTarget != Impl::ExitTarget::None) {
        return _impl->reject(OnlineFlowError::InvalidState,
                             "Recover requires a settled Failed state");
    }
    return _impl->beginExit(_impl->online.isAuthenticated()
        ? Impl::ExitTarget::MainMenu : Impl::ExitTarget::SignedOut);
}

void OnlineFlowCoordinator::update() {
    if (!_impl->online.isReady()) {
        _impl->error = OnlineFlowError::SubSystemNotReady;
        _impl->message = "Online subsystem stopped while flow was active";
        _impl->exitTarget = Impl::ExitTarget::None;
        _impl->setState(OnlineFlowState::Failed);
        return;
    }
    if (_impl->exitTarget != Impl::ExitTarget::None) {
        _impl->driveExit();
        return;
    }
    _impl->synchronizeSessionState();
    if (_impl->state == OnlineFlowState::LoadingSession &&
        _impl->config.loadingTimeoutMs != 0 &&
        _impl->now() - _impl->loadingStartedAtMs >=
            _impl->config.loadingTimeoutMs) {
        (void)_impl->beginExit(Impl::ExitTarget::Failed,
                               OnlineFlowError::LoadingTimedOut,
                               "Session connection or world loading timed out");
    }
}

OnlineFlowStatus OnlineFlowCoordinator::getStatus() const {
    return _impl->snapshot();
}

std::vector<LobbyInfo> OnlineFlowCoordinator::getLobbyResults() const {
    return _impl->online.getLobbyResults();
}

} // namespace ayt::net
