#pragma once
// Thread-safe reference Lobby / Matchmaking / Dedicated Server backend.

#include <AYNetwork/OnlineServices.h>

#include <functional>
#include <memory>

namespace ayt::net
{

struct InMemoryOnlineServicesConfig {
    size_t maxLobbies = 4096;
    size_t maxMatchTickets = 65536;
    size_t maxDedicatedServers = 4096;
    uint32_t dedicatedLeaseSeconds = 15;
    uint32_t allocationLifetimeSeconds = 60;
    std::function<uint64_t()> nowUnixSeconds;
};

class InMemoryOnlineServices final : public ILobbyService,
                                     public IMatchmakingService,
                                     public IDedicatedServerService {
public:
    explicit InMemoryOnlineServices(
        InMemoryOnlineServicesConfig config,
        std::shared_ptr<IP2PSessionService> p2pSessions = {});
    ~InMemoryOnlineServices() override;

    InMemoryOnlineServices(const InMemoryOnlineServices&) = delete;
    InMemoryOnlineServices& operator=(const InMemoryOnlineServices&) = delete;

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
