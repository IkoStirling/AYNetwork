#pragma once
// Game/application-facing flow over the engine Online subsystem.

#include <AYNetwork/Session/OnlineFlowEvents.h>
#include <AYNetwork/Session/OnlineSubSystem.h>

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace ayt::event { class EventBus; }

namespace ayt::net
{

struct OnlineFlowConfig {
    // One deadline covers transport establishment plus world loading. Zero
    // disables the deadline. The upper bound prevents accidental multi-hour
    // stale flows while still allowing large development maps.
    uint32_t loadingTimeoutMs = 120000;

    // Cleanup commands are retried while transient coordinator work settles,
    // but the application flow must never remain in Leaving forever.
    uint32_t cleanupTimeoutMs = 15000;

    // Optional deterministic monotonic-millisecond clock for tests.
    std::function<uint64_t()> nowMilliseconds;

    bool isValid() const;
};

struct OnlineFlowStatus {
    OnlineFlowState state = OnlineFlowState::SignedOut;
    OnlineFlowError error = OnlineFlowError::None;
    std::string message;

    OnlineSessionCoordinatorState sessionState =
        OnlineSessionCoordinatorState::Idle;
    OnlineSessionCoordinatorError sessionError =
        OnlineSessionCoordinatorError::None;
    OnlineServiceError serviceError = OnlineServiceError::None;
    P2PSessionCoordinatorError p2pError =
        P2PSessionCoordinatorError::None;
    OnlineSessionTopology topology = OnlineSessionTopology::None;
    // Pull-only because status events remain trivially copyable and secret-free.
    OnlineContentDescriptor content;

    uint64_t loadingGeneration = 0;
    bool worldLoaded = false;
    LobbyId lobbyId = 0;
    MatchTicketId matchTicketId = 0;
    uint64_t sessionId = 0;
    uint32_t sessionEpoch = 0;
    DedicatedAllocationId dedicatedAllocationId = 0;
};

// This coordinator does not authenticate accounts or load worlds itself. The
// account layer supplies short-lived credentials through signIn(); the scene
// layer consumes OnlineFlowLoadRequestedEvent and acknowledges completion with
// its generation. Call update() after IOnlineSubSystem::update() each frame.
class OnlineFlowCoordinator {
public:
    explicit OnlineFlowCoordinator(
        IOnlineSubSystem& online,
        OnlineFlowConfig config = {},
        ::ayt::event::EventBus* eventBus = nullptr);
    ~OnlineFlowCoordinator();

    OnlineFlowCoordinator(const OnlineFlowCoordinator&) = delete;
    OnlineFlowCoordinator& operator=(const OnlineFlowCoordinator&) = delete;

    // The token is assumed to have been issued by the application's account
    // service. A non-empty admission token replaces the current P2P token.
    bool signIn(std::string playerAccessToken,
                std::string p2pAdmissionToken = {});
    bool refreshCredentials(std::string playerAccessToken,
                            std::string p2pAdmissionToken = {});
    bool signOut();

    bool browseLobbies(ListLobbiesRequest request = {});
    bool createLobby(CreateLobbyRequest request);
    bool joinLobby(LobbyId lobbyId);
    bool refreshLobby();
    bool updateLobbyName(std::string name);
    bool leaveLobby();
    bool startLobbySession(uint16_t virtualPort);

    bool startMatchmaking(MatchmakingRequest request);
    bool cancelMatchmaking();

    // Loading and active-session teardown both return to MainMenu. signOut()
    // performs the same cleanup but clears credentials and ends SignedOut.
    bool leaveSession();
    bool returnToMainMenu();

    // Generation fencing prevents an old asynchronous scene load from
    // completing a newer assignment after cancellation/retry.
    bool completeLoading(uint64_t generation);
    bool failLoading(uint64_t generation, std::string message = {});

    // Recover performs any remaining online cleanup/reset before returning to
    // MainMenu (or SignedOut when no player credential exists).
    bool recover();
    void update();

    OnlineFlowStatus getStatus() const;
    std::vector<LobbyInfo> getLobbyResults() const;

private:
    struct Impl;
    std::unique_ptr<Impl> _impl;
};

} // namespace ayt::net
