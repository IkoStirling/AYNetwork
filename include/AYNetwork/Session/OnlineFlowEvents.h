#pragma once
// Application-flow events derived from IOnlineSubSystem state.

#include <AYEventSystem/EventPriority.h>
#include <AYNetwork/Session/OnlineSessionCoordinator.h>

#include <cstdint>
#include <type_traits>

namespace ayt::net
{

enum class OnlineFlowState : uint8_t {
    SignedOut = 0,
    MainMenu,
    BrowsingLobbies,
    CreatingLobby,
    JoiningLobby,
    InLobby,
    Matchmaking,
    StartingSession,
    LoadingSession,
    InSession,
    Leaving,
    SigningOut,
    Failed,
};

enum class OnlineFlowError : uint8_t {
    None = 0,
    InvalidConfiguration,
    SubSystemNotReady,
    AuthenticationRequired,
    InvalidState,
    CommandRejected,
    SessionFailed,
    LoadingFailed,
    LoadingTimedOut,
    CleanupFailed,
};

// AYNetwork owns the 0x0008'xxxx event-id range. These payloads deliberately
// contain no bearer, admission or Dedicated reservation token.
struct OnlineFlowStatusChangedEvent {
    static constexpr uint32_t kTypeId = 0x0008'0003u;
    static constexpr ayt::event::EventPriority kPriority =
        ayt::event::EventPriority::High;

    OnlineFlowState previousState = OnlineFlowState::SignedOut;
    OnlineFlowState state = OnlineFlowState::SignedOut;
    OnlineFlowError error = OnlineFlowError::None;
    OnlineSessionCoordinatorState sessionState =
        OnlineSessionCoordinatorState::Idle;
    OnlineSessionCoordinatorError sessionError =
        OnlineSessionCoordinatorError::None;
    OnlineServiceError serviceError = OnlineServiceError::None;
    P2PSessionCoordinatorError p2pError =
        P2PSessionCoordinatorError::None;
    OnlineSessionTopology topology = OnlineSessionTopology::None;
    uint64_t loadingGeneration = 0;
    LobbyId lobbyId = 0;
    MatchTicketId matchTicketId = 0;
    uint64_t sessionId = 0;
    uint32_t sessionEpoch = 0;
    DedicatedAllocationId dedicatedAllocationId = 0;
    bool worldLoaded = false;
};

// A game/scene loader subscribes to this event, reads any richer assignment
// data through the pull API if needed, and later calls completeLoading() or
// failLoading() with the same generation.
struct OnlineFlowLoadRequestedEvent {
    static constexpr uint32_t kTypeId = 0x0008'0004u;
    static constexpr ayt::event::EventPriority kPriority =
        ayt::event::EventPriority::High;

    uint64_t generation = 0;
    OnlineSessionTopology topology = OnlineSessionTopology::None;
    LobbyId lobbyId = 0;
    MatchTicketId matchTicketId = 0;
    uint64_t sessionId = 0;
    uint32_t sessionEpoch = 0;
    DedicatedAllocationId dedicatedAllocationId = 0;
};

static_assert(std::is_trivially_copyable_v<OnlineFlowStatusChangedEvent>);
static_assert(std::is_trivially_copyable_v<OnlineFlowLoadRequestedEvent>);

} // namespace ayt::net
