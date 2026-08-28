#pragma once
// Async application-facing orchestration for backend-authorized P2P sessions.

#include <AYNetwork/INetwork.h>
#include <AYNetwork/Session/P2PSessionLeaseKeeper.h>

#include <cstdint>
#include <memory>
#include <string>

namespace ayt::net
{

enum class P2PSessionCoordinatorState : uint8_t {
    Idle = 0,
    Creating,
    Joining,
    Starting,
    Hosting,
    Connecting,
    Active,
    Migrating,
    Leaving,
    Failed,
};

enum class P2PSessionCoordinatorError : uint8_t {
    None = 0,
    InvalidConfiguration,
    Busy,
    SessionServiceRejected,
    NetworkConfigurationFailed,
    NetworkStartFailed,
    LeaseStartFailed,
    AuthorityRejected,
    AuthorityTimeout,
    MigrationFailed,
};

struct P2PSessionCoordinatorConfig {
    // localPeerId and ICE policy/candidates are supplied by the application.
    // sessionId/sessionEpoch must remain zero; the backend grant fills them.
    P2PConfig p2p;
    uint16_t capacity = 8;
    bool enableHostMigration = true;
    uint32_t authorityRetryMs = 250;
    uint32_t authorityTimeoutMs = 15000;
    P2PSessionLeaseKeeperConfig lease;

    bool isValid() const;
};

struct P2PSessionCoordinatorStatus {
    P2PSessionCoordinatorState state = P2PSessionCoordinatorState::Idle;
    P2PSessionCoordinatorError error = P2PSessionCoordinatorError::None;
    SessionServiceError serviceError = SessionServiceError::None;
    std::string message;
    P2PBackendSessionInfo backendSession;
    P2PSessionInfo networkSession;
};

// The owner calls update() from its regular application/network loop. Backend
// HTTP operations run on worker threads; update() only consumes completed
// results and performs the fast AYNetwork configuration/state transitions.
// INetworkSubSystem must already be initialized and must outlive this object.
class P2PSessionCoordinator {
public:
    P2PSessionCoordinator(
        INetworkSubSystem& network,
        std::shared_ptr<IP2PSessionService> service,
        P2PSessionCoordinatorConfig config);
    ~P2PSessionCoordinator();

    P2PSessionCoordinator(const P2PSessionCoordinator&) = delete;
    P2PSessionCoordinator& operator=(const P2PSessionCoordinator&) = delete;

    // createSession uses config.p2p.virtualPort and config.capacity.
    bool createSession();
    bool joinSession(uint64_t sessionId);

    // Starts from a backend grant already issued by Lobby or Matchmaking.
    // This does not call createSession()/joinSession() again. The grant must
    // belong to config.p2p.localPeerId; the backend Host identity determines
    // whether AYNetwork listens or connects.
    bool startAssignedSession(P2PSessionGrant grant);
    bool leaveSession();
    bool requestHostMigration();
    void update();

    // Clears a completed failure after disconnecting. It does not contact the
    // backend; use leaveSession() when a valid membership should be revoked.
    bool reset();

    P2PSessionCoordinatorStatus getStatus() const;
    P2PSessionGrant getGrant() const;

private:
    struct Impl;
    std::unique_ptr<Impl> _impl;
};

} // namespace ayt::net
