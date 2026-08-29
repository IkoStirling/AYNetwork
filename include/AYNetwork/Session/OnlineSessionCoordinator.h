#pragma once
// Non-blocking game-facing orchestration for Lobby, Matchmaking and the
// resulting P2P or Dedicated session assignment.

#include <AYNetwork/OnlineServices.h>
#include <AYNetwork/Session/P2PSessionCoordinator.h>

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace ayt::net
{

enum class DedicatedSessionConnectionState : uint8_t {
    Idle = 0,
    Connecting,
    Active,
    Failed,
};

struct DedicatedSessionConnectionStatus {
    DedicatedSessionConnectionState state =
        DedicatedSessionConnectionState::Idle;
    std::string message;
};

// AYNetwork's legacy IP connect() deliberately has no opinion about game
// server reservation handshakes. Applications provide this small adapter so
// the complete DedicatedAllocation (including its short-lived reservation
// token) is installed before connecting. All methods run on the owner's update
// thread and must be non-blocking.
class IDedicatedSessionConnector {
public:
    virtual ~IDedicatedSessionConnector() = default;
    virtual bool connect(const DedicatedAllocation& allocation) = 0;
    virtual void disconnect() = 0;
    virtual void update() = 0;
    virtual DedicatedSessionConnectionStatus getStatus() const = 0;
};

enum class OnlineSessionCoordinatorState : uint8_t {
    Idle = 0,
    ListingLobbies,
    CreatingLobby,
    JoiningLobby,
    InLobby,
    RefreshingLobby,
    UpdatingLobby,
    CreatingLobbyInvitation,
    LeavingLobby,
    LaunchingLobby,
    Queueing,
    AwaitingMatchAcceptance,
    CancellingMatch,
    Assigned,
    Connecting,
    InSession,
    LeavingSession,
    Failed,
};

enum class OnlineSessionCoordinatorError : uint8_t {
    None = 0,
    InvalidConfiguration,
    Busy,
    OnlineServiceRejected,
    InvalidAssignment,
    P2PStartFailed,
    P2PFailed,
    DedicatedUnavailable,
    DedicatedStartFailed,
    DedicatedFailed,
    WorkerStartFailed,
};

enum class OnlineSessionTopology : uint8_t {
    None = 0,
    P2P,
    Dedicated,
};

struct OnlineSessionCoordinatorConfig {
    PeerId localPeerId;
    uint32_t matchmakingPollIntervalMs = 250;
    uint32_t maxConsecutivePollFailures = 5;

    // Optional deterministic clock seam. The value is monotonic milliseconds,
    // not Unix time. Empty uses std::chrono::steady_clock.
    std::function<uint64_t()> nowMilliseconds;

    bool isValid() const;
};

struct OnlineSessionCoordinatorStatus {
    OnlineSessionCoordinatorState state =
        OnlineSessionCoordinatorState::Idle;
    OnlineSessionCoordinatorError error =
        OnlineSessionCoordinatorError::None;
    OnlineServiceError serviceError = OnlineServiceError::None;
    P2PSessionCoordinatorError p2pError =
        P2PSessionCoordinatorError::None;
    std::string message;

    LobbyInfo lobby;
    MatchTicketInfo matchTicket;
    MatchAssignment assignment;
    OnlineSessionTopology topology = OnlineSessionTopology::None;
    DedicatedSessionConnectionStatus dedicated;
};

// The owner calls update() from its regular application/network loop. Backend
// calls run on one bounded worker at a time; P2P and Dedicated transitions are
// consumed on the update thread. The services, P2PSessionCoordinator and
// Dedicated connector must outlive any in-flight backend request.
class OnlineSessionCoordinator {
public:
    OnlineSessionCoordinator(
        std::shared_ptr<ILobbyService> lobbies,
        std::shared_ptr<IMatchmakingService> matchmaking,
        P2PSessionCoordinator& p2p,
        OnlineSessionCoordinatorConfig config,
        std::shared_ptr<IDedicatedSessionConnector> dedicated = {});
    ~OnlineSessionCoordinator();

    OnlineSessionCoordinator(const OnlineSessionCoordinator&) = delete;
    OnlineSessionCoordinator& operator=(const OnlineSessionCoordinator&) = delete;

    bool listLobbies(ListLobbiesRequest request = {});
    bool createLobby(CreateLobbyRequest request);
    bool joinLobby(LobbyId lobbyId);
    bool joinLobby(JoinLobbyRequest request);
    bool refreshLobby();
    bool updateLobby(UpdateLobbyRequest request);
    bool updateLobbyName(std::string name);
    bool createLobbyInvitation(uint32_t lifetimeSeconds = 600,
                               uint16_t maxUses = 1);
    bool leaveLobby();
    bool launchLobbyP2P(uint16_t virtualPort);

    // Empty partyMembers creates a solo ticket. Non-empty parties must contain
    // localPeerId; authorization of the remaining party is still a backend
    // responsibility.
    bool startMatchmaking(MatchmakingRequest request);
    bool respondToMatch(bool accept);
    bool cancelMatchmaking();

    // Leaves the active transport/backend membership. A Lobby-launched session
    // also leaves the Lobby after the P2P/Dedicated transport has stopped.
    bool leaveSession();
    void update();

    // Clears a terminal error only when no live Lobby, queued ticket, active
    // topology or P2P membership remains. Use leave/cancel first when
    // recoverable state exists.
    bool reset();

    OnlineSessionCoordinatorStatus getStatus() const;
    std::vector<LobbyInfo> getLobbyResults() const;
    // Invitation tokens are secret and are never copied into status/events.
    // Successful retrieval consumes the locally cached result.
    LobbyInvitation takeLobbyInvitation();

private:
    struct Impl;
    std::unique_ptr<Impl> _impl;
};

} // namespace ayt::net
