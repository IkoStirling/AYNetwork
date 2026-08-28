#pragma once
// Background host-lease renewal and epoch-CAS helpers for P2P sessions.

#include <AYNetwork/SessionService.h>

#include <cstdint>
#include <memory>

namespace ayt::net
{

struct P2PSessionLeaseKeeperConfig {
    // Keep comfortably below the reference service's 10-second lease.
    uint32_t heartbeatIntervalMs = 3000;
    uint32_t transientFailureRetryMs = 1000;

    bool isValid() const {
        return heartbeatIntervalMs != 0 && transientFailureRetryMs != 0;
    }
};

// Own one member credential and renew the lease while that member is Host.
// HTTP calls run on a private worker so an unavailable session service never
// stalls the engine/network update thread.  Call stop() before destroying the
// underlying service; the destructor also stops and joins the worker.
class P2PSessionLeaseKeeper {
public:
    P2PSessionLeaseKeeper(
        std::shared_ptr<IP2PSessionService> service,
        P2PSessionMemberCredential member,
        P2PSessionLeaseKeeperConfig config = {});
    ~P2PSessionLeaseKeeper();

    P2PSessionLeaseKeeper(const P2PSessionLeaseKeeper&) = delete;
    P2PSessionLeaseKeeper& operator=(const P2PSessionLeaseKeeper&) = delete;

    // Starts (or restarts) renewal for the exact authority epoch.  The first
    // heartbeat is sent immediately.  Permanent authority errors stop it.
    bool start(uint32_t expectedEpoch);
    void stop();
    bool isRunning() const;

    SessionServiceError getLastError() const;
    P2PBackendSessionInfo getLastSession() const;

    // Shared CAS operation for graceful transfer and expired-lease election.
    // Transferring away stops this member's renewal.  A successful self-claim
    // automatically starts renewal at the newly returned epoch.
    SessionServiceResult<P2PBackendSessionInfo> claimHost(
        uint32_t expectedEpoch, const PeerId& newHostPeerId);

private:
    struct Impl;
    std::unique_ptr<Impl> _impl;
};

} // namespace ayt::net
