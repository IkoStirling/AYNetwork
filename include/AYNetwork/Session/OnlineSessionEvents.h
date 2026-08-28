#pragma once
// Trivially-copyable Online Session events suitable for EventBus::post().

#include <AYEventSystem/EventPriority.h>
#include <AYNetwork/Session/OnlineSessionCoordinator.h>

#include <cstdint>
#include <type_traits>

namespace ayt::net
{

// AYNetwork owns the 0x0008'xxxx event-id range.
struct OnlineSessionStatusChangedEvent {
    static constexpr uint32_t kTypeId = 0x0008'0001u;
    static constexpr ayt::event::EventPriority kPriority =
        ayt::event::EventPriority::High;

    OnlineSessionCoordinatorState previousState =
        OnlineSessionCoordinatorState::Idle;
    OnlineSessionCoordinatorState state =
        OnlineSessionCoordinatorState::Idle;
    OnlineSessionCoordinatorError error =
        OnlineSessionCoordinatorError::None;
    OnlineServiceError serviceError = OnlineServiceError::None;
    P2PSessionCoordinatorError p2pError =
        P2PSessionCoordinatorError::None;
    OnlineSessionTopology topology = OnlineSessionTopology::None;
    P2PSessionState p2pState = P2PSessionState::Unconfigured;
    P2PSessionRole p2pRole = P2PSessionRole::None;
    P2PHostMigrationState migration = P2PHostMigrationState::Disabled;
    MatchTicketState matchTicketState = MatchTicketState::Queued;
    LobbyState lobbyState = LobbyState::Closed;
    LobbyId lobbyId = 0;
    uint64_t lobbyRevision = 0;
    MatchTicketId matchTicketId = 0;
    uint64_t sessionId = 0;
    uint32_t sessionEpoch = 0;
};

struct OnlineLobbyListChangedEvent {
    static constexpr uint32_t kTypeId = 0x0008'0002u;
    static constexpr ayt::event::EventPriority kPriority =
        ayt::event::EventPriority::Normal;

    uint64_t generation = 0;
    uint32_t lobbyCount = 0;
};

static_assert(std::is_trivially_copyable_v<OnlineSessionStatusChangedEvent>);
static_assert(std::is_trivially_copyable_v<OnlineLobbyListChangedEvent>);

} // namespace ayt::net
