#include <AYNetwork/Session/OnlineSubSystem.h>

#include <AYEventSystem/EventBus.h>
#include <AYGameLoop/SubSystemRegistry.h>
#include <AYNetwork/NetworkModule.h>
#include <AYNetwork/Session/HttpOnlineServices.h>
#include <AYNetwork/Session/HttpSessionService.h>
#include <AYNetwork/Session/OnlineSessionEvents.h>

#include <algorithm>
#include <chrono>
#include <limits>
#include <mutex>
#include <thread>
#include <utility>

namespace ayt::net
{
namespace
{

bool compatiblePeer(const PeerId& nested, const PeerId& canonical) {
    return nested.value.empty() || nested == canonical;
}

bool isLiveTicket(const MatchTicketInfo& ticket) {
    return ticket.ticketId != 0 &&
        (ticket.state == MatchTicketState::Queued ||
         ticket.state == MatchTicketState::Matching ||
         ticket.state == MatchTicketState::AwaitingAcceptance);
}

bool hasLiveResources(const OnlineSessionCoordinatorStatus& online,
                      const P2PSessionGrant& p2pGrant) {
    return online.lobby.lobbyId != 0 || isLiveTicket(online.matchTicket) ||
           online.topology != OnlineSessionTopology::None ||
           p2pGrant.member.isValid();
}

bool samePublishedStatus(const OnlineSessionCoordinatorStatus& a,
                         const OnlineSessionCoordinatorStatus& b,
                         const P2PSessionCoordinatorStatus& pa,
                         const P2PSessionCoordinatorStatus& pb) {
    return a.state == b.state && a.error == b.error &&
           a.serviceError == b.serviceError && a.p2pError == b.p2pError &&
           a.message == b.message && a.topology == b.topology &&
           a.lobby.lobbyId == b.lobby.lobbyId &&
           a.lobby.revision == b.lobby.revision &&
           a.lobby.state == b.lobby.state &&
           a.lobby.members.size() == b.lobby.members.size() &&
           a.matchTicket.ticketId == b.matchTicket.ticketId &&
           a.matchTicket.state == b.matchTicket.state &&
           a.matchTicket.assignment.matchId ==
               b.matchTicket.assignment.matchId &&
           a.matchTicket.acceptedMembers.size() ==
               b.matchTicket.acceptedMembers.size() &&
           a.matchTicket.acceptanceExpiresAtUnixSeconds ==
               b.matchTicket.acceptanceExpiresAtUnixSeconds &&
           a.matchTicket.failure == b.matchTicket.failure &&
           pa.state == pb.state && pa.error == pb.error &&
           pa.serviceError == pb.serviceError && pa.message == pb.message &&
           pa.backendSession.sessionId == pb.backendSession.sessionId &&
           pa.backendSession.epoch == pb.backendSession.epoch &&
           pa.backendSession.hostPeerId == pb.backendSession.hostPeerId &&
           pa.networkSession.state == pb.networkSession.state &&
           pa.networkSession.role == pb.networkSession.role &&
           pa.networkSession.migration == pb.networkSession.migration;
}

struct CredentialStore {
    std::string playerToken() const {
        std::lock_guard<std::mutex> lock(mutex);
        return player;
    }
    std::string admissionToken() const {
        std::lock_guard<std::mutex> lock(mutex);
        return admission;
    }
    void setPlayer(std::string value) {
        std::lock_guard<std::mutex> lock(mutex);
        player = std::move(value);
    }
    void setAdmission(std::string value) {
        std::lock_guard<std::mutex> lock(mutex);
        admission = std::move(value);
    }

    mutable std::mutex mutex;
    std::string player;
    std::string admission;
};

class OnlineSubSystem final : public IOnlineSubSystem {
public:
    OnlineSubSystem(INetworkSubSystem& network,
                    OnlineSubSystemConfig config,
                    OnlineSubSystemDependencies dependencies,
                    ::ayt::event::EventBus* eventBus)
        : _network(network),
          _config(std::move(config)),
          _dependencies(std::move(dependencies)),
          _eventBus(eventBus ? eventBus : &::ayt::event::EventBus::instance()),
          _credentials(std::make_shared<CredentialStore>()) {
        _credentials->setPlayer(_config.backend.playerAccessToken);
        _credentials->setAdmission(_config.backend.p2pAdmissionToken);
    }

    ~OnlineSubSystem() override { shutdown(); }

    const char* getName() const override { return "Online"; }

    const ::ayt::game::SubSystemDescriptor& getDescriptor() const override {
        static ::ayt::game::SubSystemDescriptor descriptor = {
            .name = "Online",
            .dependencies = {},
            .basePriority = 90,
            .timeType = ::ayt::game::SubSystemDescriptor::TimeType::Real,
            .phases = ::ayt::game::phaseBit(::ayt::game::FramePhase::Ingress),
            .clock = ::ayt::game::ClockDomain::RealWall,
            .initializeAfter = {"Network"},
            .runsAfter = {"Network"},
            .phasePriority = 90,
            .reads = {"Network.TransportState"},
            .writes = {"Network.SessionControl", "Online.SessionState"},
        };
        return descriptor;
    }

    bool initialize() override {
        if (_initialized) return true;
        if (!_config.isValid()) return false;

        const bool anyInjected = _dependencies.hasAnyBackendService();
        if (anyInjected != _dependencies.hasCompleteBackendServices()) {
            return false;
        }
        _usesHttpBackend = !anyInjected;
        if (_usesHttpBackend && !_config.isValidForHttp()) return false;

        if (_usesHttpBackend) {
            HttpP2PSessionClientConfig sessionConfig;
            sessionConfig.serverAddress = _config.backend.serverAddress;
            sessionConfig.serverPort = _config.backend.serverPort;
            sessionConfig.connectTimeoutMs = _config.backend.connectTimeoutMs;
            sessionConfig.requestTimeoutMs = _config.backend.requestTimeoutMs;
            const auto credentials = _credentials;
            sessionConfig.admissionTokenProvider = [credentials] {
                return credentials->admissionToken();
            };
            _sessionService = std::make_shared<HttpP2PSessionService>(
                std::move(sessionConfig));

            HttpOnlineServicesClientConfig onlineConfig;
            onlineConfig.serverAddress = _config.backend.serverAddress;
            onlineConfig.serverPort = _config.backend.serverPort;
            onlineConfig.connectTimeoutMs = _config.backend.connectTimeoutMs;
            onlineConfig.requestTimeoutMs = _config.backend.requestTimeoutMs;
            onlineConfig.localPeerId = _config.localPeerId;
            onlineConfig.playerAccessTokenProvider = [credentials] {
                return credentials->playerToken();
            };
            auto online = std::make_shared<HttpOnlineServices>(
                std::move(onlineConfig));
            _lobbyService = online;
            _matchmakingService = online;
        } else {
            _sessionService = _dependencies.sessionService;
            _lobbyService = _dependencies.lobbyService;
            _matchmakingService = _dependencies.matchmakingService;
        }

        auto p2pConfig = _config.p2p;
        p2pConfig.p2p.localPeerId = _config.localPeerId;
        auto sessionConfig = _config.sessions;
        sessionConfig.localPeerId = _config.localPeerId;
        _p2p = std::make_unique<P2PSessionCoordinator>(
            _network, _sessionService, std::move(p2pConfig));
        _online = std::make_unique<OnlineSessionCoordinator>(
            _lobbyService, _matchmakingService, *_p2p,
            std::move(sessionConfig), _dependencies.dedicatedConnector);
        _lastOnlineStatus = _online->getStatus();
        _lastP2PStatus = _p2p->getStatus();
        if (_lastOnlineStatus.state == OnlineSessionCoordinatorState::Failed ||
            _lastP2PStatus.state == P2PSessionCoordinatorState::Failed) {
            _online.reset();
            _p2p.reset();
            releaseServices();
            return false;
        }
        _initialized = true;
        return true;
    }

    void update(float) override {
        if (!_online || !_p2p) return;
        _online->update();
        publishChanges();
    }

    void fixedUpdate(float) override {}

    void shutdown() override {
        if (!_online && !_p2p) {
            _initialized = false;
            return;
        }
        drainOnlineResources();
        _online.reset();
        _p2p.reset();
        releaseServices();
        _lastOnlineStatus = {};
        _lastP2PStatus = {};
        _initialized = false;
        _usesHttpBackend = false;
    }

    bool isReady() const override { return _initialized && _online && _p2p; }

    bool isAuthenticated() const override {
        return isReady() &&
            (!_usesHttpBackend || !_credentials->playerToken().empty());
    }

    PeerId getLocalPeerId() const override { return _config.localPeerId; }

    bool setPlayerAccessToken(std::string token) override {
        if (token.empty() && _online && _p2p &&
            hasLiveResources(_online->getStatus(), _p2p->getGrant())) {
            return false;
        }
        _credentials->setPlayer(std::move(token));
        return true;
    }

    bool setP2PAdmissionToken(std::string token) override {
        _credentials->setAdmission(std::move(token));
        return true;
    }

    bool listLobbies(ListLobbiesRequest request) override {
        return trackAccepted(
            _online && _online->listLobbies(std::move(request)));
    }
    bool createLobby(CreateLobbyRequest request) override {
        return trackAccepted(
            _online && _online->createLobby(std::move(request)));
    }
    bool joinLobby(LobbyId lobbyId) override {
        return trackAccepted(_online && _online->joinLobby(lobbyId));
    }
    bool joinLobby(JoinLobbyRequest request) override {
        return trackAccepted(
            _online && _online->joinLobby(std::move(request)));
    }
    bool refreshLobby() override {
        return trackAccepted(_online && _online->refreshLobby());
    }
    bool updateLobby(UpdateLobbyRequest request) override {
        return trackAccepted(
            _online && _online->updateLobby(std::move(request)));
    }
    bool updateLobbyName(std::string name) override {
        return trackAccepted(
            _online && _online->updateLobbyName(std::move(name)));
    }
    bool createLobbyInvitation(uint32_t lifetimeSeconds,
                               uint16_t maxUses) override {
        return trackAccepted(_online && _online->createLobbyInvitation(
            lifetimeSeconds, maxUses));
    }
    LobbyInvitation takeLobbyInvitation() override {
        return _online ? _online->takeLobbyInvitation() : LobbyInvitation{};
    }
    bool leaveLobby() override {
        return trackAccepted(_online && _online->leaveLobby());
    }
    bool launchLobbyP2P(uint16_t virtualPort) override {
        return trackAccepted(
            _online && _online->launchLobbyP2P(virtualPort));
    }
    bool startMatchmaking(MatchmakingRequest request) override {
        return trackAccepted(
            _online && _online->startMatchmaking(std::move(request)));
    }
    bool respondToMatch(bool accept) override {
        return trackAccepted(_online && _online->respondToMatch(accept));
    }
    bool cancelMatchmaking() override {
        return trackAccepted(_online && _online->cancelMatchmaking());
    }
    bool leaveSession() override {
        return trackAccepted(_online && _online->leaveSession());
    }
    bool reset() override {
        return trackAccepted(_online && _online->reset());
    }

    OnlineSessionCoordinatorStatus getOnlineStatus() const override {
        return _online ? _online->getStatus()
                       : OnlineSessionCoordinatorStatus{};
    }
    P2PSessionCoordinatorStatus getP2PStatus() const override {
        return _p2p ? _p2p->getStatus() : P2PSessionCoordinatorStatus{};
    }
    std::vector<LobbyInfo> getLobbyResults() const override {
        return _online ? _online->getLobbyResults() : std::vector<LobbyInfo>{};
    }
    uint64_t getLobbyListGeneration() const override {
        return _lobbyListGeneration;
    }

private:
    bool trackAccepted(bool accepted) {
        if (accepted) publishChanges();
        return accepted;
    }

    void releaseServices() {
        _matchmakingService.reset();
        _lobbyService.reset();
        _sessionService.reset();
    }

    void postStatusEvent(const OnlineSessionCoordinatorStatus& current,
                         const P2PSessionCoordinatorStatus& p2p) {
        OnlineSessionStatusChangedEvent event;
        event.previousState = _lastOnlineStatus.state;
        event.state = current.state;
        event.error = current.error;
        event.serviceError = current.serviceError;
        event.p2pError = current.p2pError;
        event.topology = current.topology;
        event.p2pState = p2p.networkSession.state;
        event.p2pRole = p2p.networkSession.role;
        event.migration = p2p.networkSession.migration;
        event.matchTicketState = current.matchTicket.state;
        event.lobbyState = current.lobby.state;
        event.lobbyId = current.lobby.lobbyId;
        event.lobbyRevision = current.lobby.revision;
        event.matchTicketId = current.matchTicket.ticketId;
        event.sessionId = p2p.backendSession.sessionId != 0
            ? p2p.backendSession.sessionId : current.lobby.sessionId;
        event.sessionEpoch = p2p.backendSession.epoch;
        _eventBus->post(event);
    }

    void publishChanges() {
        const auto current = _online->getStatus();
        const auto p2p = _p2p->getStatus();
        if (!samePublishedStatus(_lastOnlineStatus, current,
                                 _lastP2PStatus, p2p)) {
            postStatusEvent(current, p2p);
        }
        if (_lastOnlineStatus.state ==
                OnlineSessionCoordinatorState::ListingLobbies &&
            current.state == OnlineSessionCoordinatorState::Idle) {
            ++_lobbyListGeneration;
            const size_t count = _online->getLobbyResults().size();
            OnlineLobbyListChangedEvent event;
            event.generation = _lobbyListGeneration;
            event.lobbyCount = static_cast<uint32_t>((std::min)(
                count, static_cast<size_t>((std::numeric_limits<uint32_t>::max)())));
            _eventBus->post(event);
        }
        _lastOnlineStatus = current;
        _lastP2PStatus = p2p;
    }

    bool requestCleanup() {
        const auto status = _online->getStatus();
        if (status.state == OnlineSessionCoordinatorState::Queueing ||
            status.state == OnlineSessionCoordinatorState::CancellingMatch ||
            (status.state == OnlineSessionCoordinatorState::Failed &&
             isLiveTicket(status.matchTicket))) {
            return _online->cancelMatchmaking();
        }
        if (status.topology != OnlineSessionTopology::None &&
            (status.state == OnlineSessionCoordinatorState::Connecting ||
             status.state == OnlineSessionCoordinatorState::InSession ||
             status.state == OnlineSessionCoordinatorState::Failed)) {
            return _online->leaveSession();
        }
        if (status.lobby.lobbyId != 0 &&
            (status.state == OnlineSessionCoordinatorState::InLobby ||
             status.state == OnlineSessionCoordinatorState::Failed)) {
            return _online->leaveLobby();
        }
        if (status.state == OnlineSessionCoordinatorState::Failed &&
            !hasLiveResources(status, _p2p->getGrant())) {
            return _online->reset();
        }
        return false;
    }

    void drainOnlineResources() {
        if (!_online || !_p2p || _config.gracefulShutdownTimeoutMs == 0) return;
        const auto deadline = std::chrono::steady_clock::now() +
            std::chrono::milliseconds(_config.gracefulShutdownTimeoutMs);
        while (std::chrono::steady_clock::now() < deadline) {
            _online->update();
            const auto status = _online->getStatus();
            if (!hasLiveResources(status, _p2p->getGrant()) &&
                (status.state == OnlineSessionCoordinatorState::Idle ||
                 status.state == OnlineSessionCoordinatorState::Failed)) {
                return;
            }
            (void)requestCleanup();
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }

    INetworkSubSystem& _network;
    OnlineSubSystemConfig _config;
    OnlineSubSystemDependencies _dependencies;
    ::ayt::event::EventBus* _eventBus = nullptr;
    std::shared_ptr<CredentialStore> _credentials;
    std::shared_ptr<IP2PSessionService> _sessionService;
    std::shared_ptr<ILobbyService> _lobbyService;
    std::shared_ptr<IMatchmakingService> _matchmakingService;
    std::unique_ptr<P2PSessionCoordinator> _p2p;
    std::unique_ptr<OnlineSessionCoordinator> _online;
    OnlineSessionCoordinatorStatus _lastOnlineStatus;
    P2PSessionCoordinatorStatus _lastP2PStatus;
    uint64_t _lobbyListGeneration = 0;
    bool _initialized = false;
    bool _usesHttpBackend = false;
};

} // namespace

bool OnlineBackendClientConfig::isValid() const {
    return !serverAddress.empty() && serverPort != 0 &&
           connectTimeoutMs != 0 && connectTimeoutMs <= 60000u &&
           requestTimeoutMs != 0 && requestTimeoutMs <= 60000u;
}

bool OnlineSubSystemConfig::isValid() const {
    if (!localPeerId.isValid() ||
        !compatiblePeer(p2p.p2p.localPeerId, localPeerId) ||
        !compatiblePeer(sessions.localPeerId, localPeerId) ||
        p2p.p2p.sessionId != 0 || p2p.p2p.sessionEpoch != 0 ||
        gracefulShutdownTimeoutMs > 10000u) {
        return false;
    }
    auto checkedP2P = p2p;
    checkedP2P.p2p.localPeerId = localPeerId;
    auto checkedSessions = sessions;
    checkedSessions.localPeerId = localPeerId;
    return checkedP2P.isValid() && checkedSessions.isValid();
}

bool OnlineSubSystemConfig::isValidForHttp() const {
    return isValid() && backend.isValid();
}

bool OnlineSubSystemDependencies::hasAnyBackendService() const {
    return sessionService || lobbyService || matchmakingService;
}

bool OnlineSubSystemDependencies::hasCompleteBackendServices() const {
    return sessionService && lobbyService && matchmakingService;
}

std::unique_ptr<IOnlineSubSystem> createOnlineSubSystem(
    INetworkSubSystem& network,
    OnlineSubSystemConfig config,
    OnlineSubSystemDependencies dependencies,
    ::ayt::event::EventBus* eventBus) {
    return std::make_unique<OnlineSubSystem>(
        network, std::move(config), std::move(dependencies), eventBus);
}

IOnlineSubSystem* findRegisteredOnlineSubSystem() {
    auto* system = ::ayt::game::SubSystemRegistry::instance().findSubSystem(
        "Online");
    return dynamic_cast<IOnlineSubSystem*>(system);
}

bool registerOnlineSubSystem(OnlineSubSystemConfig config,
                             OnlineSubSystemDependencies dependencies,
                             ::ayt::event::EventBus* eventBus) {
    if (findRegisteredOnlineSubSystem()) return true;
    const bool anyInjected = dependencies.hasAnyBackendService();
    if (!config.isValid() ||
        (anyInjected != dependencies.hasCompleteBackendServices()) ||
        (!anyInjected && !config.isValidForHttp())) {
        return false;
    }
    registerNetworkSubSystem();
    INetworkSubSystem* network = findRegisteredNetworkSubSystem();
    if (!network) return false;
    auto system = createOnlineSubSystem(
        *network, std::move(config), std::move(dependencies), eventBus);
    ::ayt::game::IGameLoop::instance().registerSubSystem(system.release());
    return findRegisteredOnlineSubSystem() != nullptr;
}

} // namespace ayt::net
