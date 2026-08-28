#pragma once
// cpp-httplib client adapter for Lobby, Matchmaking and Dedicated services.

#include <AYNetwork/OnlineServices.h>

#include <cstdint>
#include <functional>
#include <memory>
#include <string>

namespace ayt::net
{

struct HttpOnlineServicesClientConfig {
    std::string serverAddress = "127.0.0.1";
    uint16_t serverPort = 0;
    uint32_t connectTimeoutMs = 2000;
    uint32_t requestTimeoutMs = 3000;

    // Player operations use this bearer and never transmit actorPeerId in the
    // body. The server derives PeerId from its trusted authentication callback.
    PeerId localPeerId;
    std::string playerAccessToken;
    // Optional thread-safe token source used by long-lived engine clients.
    // When present it takes precedence over playerAccessToken on each request.
    std::function<std::string()> playerAccessTokenProvider;

    // Trusted fleet/orchestrator operations: register/list/allocate servers.
    std::string dedicatedControlToken;
};

class HttpOnlineServices final : public ILobbyService,
                                 public IMatchmakingService,
                                 public IDedicatedServerService {
public:
    explicit HttpOnlineServices(HttpOnlineServicesClientConfig config);
    ~HttpOnlineServices() override;

    HttpOnlineServices(const HttpOnlineServices&) = delete;
    HttpOnlineServices& operator=(const HttpOnlineServices&) = delete;

    OnlineServiceResult<LobbyInfo> createLobby(
        const CreateLobbyRequest& request) override;
    OnlineServiceResult<std::vector<LobbyInfo>> listLobbies(
        const ListLobbiesRequest& request) override;
    OnlineServiceResult<LobbyInfo> joinLobby(
        LobbyId lobbyId, const PeerId& authenticatedPeer) override;
    OnlineServiceResult<LobbyInfo> leaveLobby(
        LobbyId lobbyId, const PeerId& authenticatedPeer) override;
    OnlineServiceResult<LobbyInfo> updateLobby(
        const UpdateLobbyRequest& request) override;
    OnlineServiceResult<LobbyInfo> getLobby(LobbyId lobbyId) override;
    OnlineServiceResult<LobbyLaunchResult> launchLobbyP2P(
        const LaunchLobbyRequest& request) override;

    OnlineServiceResult<MatchTicketInfo> enqueueMatch(
        const MatchmakingRequest& request) override;
    OnlineServiceResult<MatchTicketInfo> getMatch(
        MatchTicketId ticketId, const PeerId& authenticatedPeer) override;
    OnlineServiceResult<MatchTicketInfo> cancelMatch(
        MatchTicketId ticketId, const PeerId& authenticatedPeer) override;
    size_t runMatchmaking(size_t maxMatches = 1) override;

    OnlineServiceResult<DedicatedServerGrant> registerServer(
        const DedicatedServerRegistration& request) override;
    OnlineServiceResult<DedicatedServerInfo> heartbeatServer(
        const DedicatedServerCredential& credential) override;
    OnlineServiceResult<DedicatedServerInfo> setServerDraining(
        const DedicatedServerCredential& credential, bool draining) override;
    OnlineServiceResult<SessionServiceEmpty> unregisterServer(
        const DedicatedServerCredential& credential) override;
    OnlineServiceResult<DedicatedAllocation> allocateServer(
        const DedicatedAllocationRequest& request) override;
    OnlineServiceResult<SessionServiceEmpty> releaseAllocation(
        DedicatedAllocationId allocationId,
        const std::string& reservationToken) override;
    OnlineServiceResult<std::vector<DedicatedServerInfo>> listServers() override;

private:
    struct Impl;
    std::unique_ptr<Impl> _impl;
};

} // namespace ayt::net
