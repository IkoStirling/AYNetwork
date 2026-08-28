#pragma once
// Durable single-database Lobby / Matchmaking / Dedicated Server backend.

#include <AYNetwork/OnlineServices.h>

#include <array>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>

namespace ayt::net
{

struct SqliteOnlineServicesConfig {
    std::string databasePath;
    std::array<uint8_t, 32> storageKey{};
    uint32_t busyTimeoutMs = 5000;
    uint32_t operationClaimSeconds = 30;
    uint32_t matchTicketRetentionSeconds = 24u * 60u * 60u;

    size_t maxLobbies = 4096;
    size_t maxMatchTickets = 65536;
    size_t maxDedicatedServers = 4096;
    uint16_t maxLobbyCapacity = 64;
    uint16_t maxMatchPlayers = 64;
    uint16_t maxDedicatedServerCapacity = 4096;
    uint32_t dedicatedLeaseSeconds = 15;
    uint32_t allocationLifetimeSeconds = 60;
    std::function<uint64_t()> nowUnixSeconds;

    bool isValid() const;
};

// SQLite WAL supports multiple HTTP service processes on one host. Lobby
// launch and matchmaking use expiring database claims so only one process may
// perform their external P2P side effects. SQLite databases on network/shared
// filesystems are not supported; deploy an application database adapter there.
class SqliteOnlineServices final : public ILobbyService,
                                   public IMatchmakingService,
                                   public IDedicatedServerService {
public:
    explicit SqliteOnlineServices(
        SqliteOnlineServicesConfig config,
        std::shared_ptr<IP2PSessionService> p2pSessions = {});
    ~SqliteOnlineServices() override;

    SqliteOnlineServices(const SqliteOnlineServices&) = delete;
    SqliteOnlineServices& operator=(const SqliteOnlineServices&) = delete;

    bool isReady() const;
    std::string getLastError() const;

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
