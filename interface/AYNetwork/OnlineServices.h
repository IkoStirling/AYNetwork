#pragma once
// Backend-neutral Lobby, Matchmaking and Dedicated Server contracts.
// Transport adapters must supply authenticated PeerId values; implementations
// must never derive caller identity from an untrusted request body.

#include <AYNetwork/SessionService.h>

#include <cstdint>
#include <limits>
#include <string>
#include <utility>
#include <vector>

namespace ayt::net
{

enum class OnlineServiceError : uint8_t {
    None = 0,
    InvalidRequest,
    Unauthorized,
    NotFound,
    Conflict,
    Full,
    Closed,
    BackendUnavailable,
    NoCapacity,
    InternalError,
};

template <typename T>
struct OnlineServiceResult {
    OnlineServiceError error = OnlineServiceError::None;
    std::string message;
    T value{};

    bool ok() const { return error == OnlineServiceError::None; }
    explicit operator bool() const { return ok(); }

    static OnlineServiceResult success(T result) {
        OnlineServiceResult out;
        out.value = std::move(result);
        return out;
    }
    static OnlineServiceResult failure(OnlineServiceError error,
                                       std::string message = {}) {
        OnlineServiceResult out;
        out.error = error == OnlineServiceError::None
            ? OnlineServiceError::InternalError : error;
        out.message = std::move(message);
        return out;
    }
};

using LobbyId = uint64_t;

enum class LobbyState : uint8_t { Open = 0, Launching, InSession, Closed };

// Backend-issued logical content identity. contentId is resolved by the game
// to a local asset; it is never interpreted as a client filesystem path.
struct OnlineContentDescriptor {
    std::string contentId;
    std::string contentVersion;
    uint64_t contentSeed = 0;

    bool isValid() const {
        return !contentId.empty() && contentId.size() <= 128 &&
               !contentVersion.empty() && contentVersion.size() <= 64 &&
               contentSeed <= static_cast<uint64_t>(
                   (std::numeric_limits<int64_t>::max)());
    }

    bool operator==(const OnlineContentDescriptor&) const = default;
};

struct LobbyInfo {
    LobbyId lobbyId = 0;
    uint64_t revision = 0;
    PeerId ownerPeerId;
    std::string name;
    std::string region;
    std::string buildId;
    OnlineContentDescriptor content;
    uint16_t capacity = 0;
    LobbyState state = LobbyState::Closed;
    std::vector<PeerId> members;
    uint64_t sessionId = 0;

    bool isValid() const;
};

struct CreateLobbyRequest {
    PeerId ownerPeerId;
    std::string name;
    std::string region;
    std::string buildId;
    OnlineContentDescriptor content;
    uint16_t capacity = 8;
};

struct ListLobbiesRequest {
    std::string region;
    std::string buildId;
    uint16_t minimumOpenSlots = 1;
    size_t limit = 100;
};

struct UpdateLobbyRequest {
    LobbyId lobbyId = 0;
    PeerId actorPeerId;
    uint64_t expectedRevision = 0;
    std::string name;
};

struct LaunchLobbyRequest {
    LobbyId lobbyId = 0;
    PeerId actorPeerId;
    uint64_t expectedRevision = 0;
    uint16_t virtualPort = 0;
};

struct LobbyLaunchResult {
    LobbyInfo lobby;
    std::vector<P2PSessionGrant> memberGrants;
};

class ILobbyService {
public:
    virtual ~ILobbyService() = default;
    virtual OnlineServiceResult<LobbyInfo> createLobby(
        const CreateLobbyRequest& request) = 0;
    virtual OnlineServiceResult<std::vector<LobbyInfo>> listLobbies(
        const ListLobbiesRequest& request) = 0;
    virtual OnlineServiceResult<LobbyInfo> joinLobby(
        LobbyId lobbyId, const PeerId& authenticatedPeer) = 0;
    virtual OnlineServiceResult<LobbyInfo> leaveLobby(
        LobbyId lobbyId, const PeerId& authenticatedPeer) = 0;
    virtual OnlineServiceResult<LobbyInfo> updateLobby(
        const UpdateLobbyRequest& request) = 0;
    virtual OnlineServiceResult<LobbyInfo> getLobby(LobbyId lobbyId) = 0;
    virtual OnlineServiceResult<LobbyLaunchResult> launchLobbyP2P(
        const LaunchLobbyRequest& request) = 0;
};

using DedicatedServerId = uint64_t;
using DedicatedAllocationId = uint64_t;

struct DedicatedServerRegistration {
    std::string instanceName;
    std::string region;
    std::string buildId;
    std::string address;
    uint16_t port = 0;
    uint16_t capacity = 0;
};

struct DedicatedServerCredential {
    DedicatedServerId serverId = 0;
    std::string token;

    bool isValid() const { return serverId != 0 && token.size() == 64; }
};

struct DedicatedServerInfo {
    DedicatedServerId serverId = 0;
    std::string instanceName;
    std::string region;
    std::string buildId;
    std::string address;
    uint16_t port = 0;
    uint16_t capacity = 0;
    uint16_t reservedPlayers = 0;
    bool draining = false;
    uint64_t leaseExpiresAtUnixSeconds = 0;
};

struct DedicatedServerGrant {
    DedicatedServerInfo server;
    DedicatedServerCredential credential;
};

struct DedicatedAllocationRequest {
    std::string region;
    std::string buildId;
    uint16_t playerCount = 0;
};

struct DedicatedAllocation {
    DedicatedAllocationId allocationId = 0;
    DedicatedServerId serverId = 0;
    std::string address;
    uint16_t port = 0;
    uint16_t playerCount = 0;
    std::string reservationToken;
    uint64_t expiresAtUnixSeconds = 0;

    bool isValid() const {
        return allocationId != 0 && serverId != 0 && !address.empty() &&
               port != 0 && playerCount != 0 &&
               reservationToken.size() == 64 && expiresAtUnixSeconds != 0;
    }
};

class IDedicatedServerService {
public:
    virtual ~IDedicatedServerService() = default;
    virtual OnlineServiceResult<DedicatedServerGrant> registerServer(
        const DedicatedServerRegistration& request) = 0;
    virtual OnlineServiceResult<DedicatedServerInfo> heartbeatServer(
        const DedicatedServerCredential& credential) = 0;
    virtual OnlineServiceResult<DedicatedServerInfo> setServerDraining(
        const DedicatedServerCredential& credential, bool draining) = 0;
    virtual OnlineServiceResult<SessionServiceEmpty> unregisterServer(
        const DedicatedServerCredential& credential) = 0;
    virtual OnlineServiceResult<DedicatedAllocation> allocateServer(
        const DedicatedAllocationRequest& request) = 0;
    virtual OnlineServiceResult<SessionServiceEmpty> releaseAllocation(
        DedicatedAllocationId allocationId,
        const std::string& reservationToken) = 0;
    virtual OnlineServiceResult<std::vector<DedicatedServerInfo>> listServers() = 0;
};

using MatchTicketId = uint64_t;

enum class MatchTopology : uint8_t { P2P = 0, Dedicated, Any };
enum class MatchTicketState : uint8_t {
    Queued = 0,
    Matching,
    Matched,
    Cancelled,
    Failed,
};

struct MatchmakingRequest {
    std::vector<PeerId> partyMembers;
    std::string queue;
    std::string region;
    std::string buildId;
    OnlineContentDescriptor content;
    MatchTopology topology = MatchTopology::Any;
    uint16_t targetPlayers = 2;
    uint16_t virtualPort = 7350;
};

struct MatchAssignment {
    MatchTopology topology = MatchTopology::P2P;
    OnlineContentDescriptor content;
    std::vector<P2PSessionGrant> p2pGrants;
    DedicatedAllocation dedicated;
};

struct MatchTicketInfo {
    MatchTicketId ticketId = 0;
    MatchTicketState state = MatchTicketState::Queued;
    MatchmakingRequest request;
    MatchAssignment assignment;
    std::string failure;
};

class IMatchmakingService {
public:
    virtual ~IMatchmakingService() = default;
    virtual OnlineServiceResult<MatchTicketInfo> enqueueMatch(
        const MatchmakingRequest& request) = 0;
    virtual OnlineServiceResult<MatchTicketInfo> getMatch(
        MatchTicketId ticketId, const PeerId& authenticatedPeer) = 0;
    virtual OnlineServiceResult<MatchTicketInfo> cancelMatch(
        MatchTicketId ticketId, const PeerId& authenticatedPeer) = 0;

    // Reference implementations expose an explicit pump; production adapters
    // normally invoke equivalent work from a queue consumer.
    virtual size_t runMatchmaking(size_t maxMatches = 1) = 0;
};

} // namespace ayt::net
