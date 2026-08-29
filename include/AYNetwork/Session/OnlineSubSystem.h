#pragma once
// GameLoop-owned facade that assembles Online Services and session transport.

#include <AYGameLoop/IGameLoop.h>
#include <AYNetwork/Session/OnlineSessionCoordinator.h>

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace ayt::event { class EventBus; }

namespace ayt::net
{

struct OnlineBackendClientConfig {
    std::string serverAddress = "127.0.0.1";
    uint16_t serverPort = 0;
    uint32_t connectTimeoutMs = 2000;
    uint32_t requestTimeoutMs = 3000;

    // Initial values only. IOnlineSubSystem can rotate either credential after
    // initialization without recreating HTTP clients. Secrets are never put in
    // EventBus payloads or returned by the subsystem status API.
    std::string playerAccessToken;
    std::string p2pAdmissionToken;

    bool isValid() const;
};

struct OnlineSubSystemConfig {
    PeerId localPeerId;
    OnlineBackendClientConfig backend;
    P2PSessionCoordinatorConfig p2p;
    OnlineSessionCoordinatorConfig sessions;
    uint32_t gracefulShutdownTimeoutMs = 2000;

    // Common runtime validation. A factory using injected services does not
    // require backend.serverPort; the default HTTP path also calls
    // isValidForHttp(). Nested local PeerIds may be empty or match localPeerId.
    bool isValid() const;
    bool isValidForHttp() const;
};

struct OnlineSubSystemDependencies {
    std::shared_ptr<IP2PSessionService> sessionService;
    std::shared_ptr<ILobbyService> lobbyService;
    std::shared_ptr<IMatchmakingService> matchmakingService;
    std::shared_ptr<IDedicatedSessionConnector> dedicatedConnector;

    bool hasAnyBackendService() const;
    bool hasCompleteBackendServices() const;
};

class IOnlineSubSystem : public ::ayt::game::ISubSystem {
public:
    ~IOnlineSubSystem() override = default;

    virtual bool isReady() const = 0;
    virtual bool isAuthenticated() const = 0;
    virtual PeerId getLocalPeerId() const = 0;

    // The player bearer may rotate while an operation/session is active.
    // Clearing it is accepted only when no live online resources remain.
    virtual bool setPlayerAccessToken(std::string token) = 0;
    virtual bool setP2PAdmissionToken(std::string token) = 0;

    virtual bool listLobbies(ListLobbiesRequest request = {}) = 0;
    virtual bool createLobby(CreateLobbyRequest request) = 0;
    virtual bool joinLobby(LobbyId lobbyId) = 0;
    virtual bool joinLobby(JoinLobbyRequest request) {
        return request.password.empty() && request.invitationToken.empty() &&
               joinLobby(request.lobbyId);
    }
    virtual bool refreshLobby() = 0;
    virtual bool updateLobby(UpdateLobbyRequest request) {
        if (request.replaceMetadata || request.setVisibility ||
            request.setPassword) return false;
        return updateLobbyName(std::move(request.name));
    }
    virtual bool updateLobbyName(std::string name) = 0;
    virtual bool createLobbyInvitation(uint32_t, uint16_t) { return false; }
    virtual LobbyInvitation takeLobbyInvitation() { return {}; }
    virtual bool leaveLobby() = 0;
    virtual bool launchLobbyP2P(uint16_t virtualPort) = 0;
    virtual bool startMatchmaking(MatchmakingRequest request) = 0;
    virtual bool respondToMatch(bool) { return false; }
    virtual bool cancelMatchmaking() = 0;
    virtual bool leaveSession() = 0;
    virtual bool reset() = 0;

    virtual OnlineSessionCoordinatorStatus getOnlineStatus() const = 0;
    virtual P2PSessionCoordinatorStatus getP2PStatus() const = 0;
    virtual std::vector<LobbyInfo> getLobbyResults() const = 0;
    virtual uint64_t getLobbyListGeneration() const = 0;
};

// Creates an unregistered subsystem. Complete injected dependencies bypass the
// HTTP backend config, which is useful for platform adapters and tests. Passing
// a partial dependency set is invalid. A null EventBus uses the process bus.
std::unique_ptr<IOnlineSubSystem> createOnlineSubSystem(
    INetworkSubSystem& network,
    OnlineSubSystemConfig config,
    OnlineSubSystemDependencies dependencies = {},
    ::ayt::event::EventBus* eventBus = nullptr);

// Register before GameLoop::preparePlaySession()/run(). The function also
// ensures the Network subsystem is registered and is idempotent by name.
bool registerOnlineSubSystem(
    OnlineSubSystemConfig config,
    OnlineSubSystemDependencies dependencies = {},
    ::ayt::event::EventBus* eventBus = nullptr);

IOnlineSubSystem* findRegisteredOnlineSubSystem();

} // namespace ayt::net
