#pragma once
// Durable SQLite-backed authority service for production-oriented deployments.

#include <AYNetwork/Session/SessionTicket.h>
#include <AYNetwork/SessionService.h>

#include <array>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>

namespace ayt::net
{

struct SqliteP2PSessionServiceConfig {
    std::string databasePath;
    std::string publicSignalingAddress;
    uint16_t signalingPort = 0;
    uint32_t hostLeaseSeconds = 10;
    uint32_t joinTicketLifetimeSeconds = 60;
    uint32_t signalingTokenLifetimeSeconds = 24u * 60u * 60u;
    uint32_t busyTimeoutMs = 5000;
    size_t maxSessions = 100000;

    // Encrypts member and signaling credentials at rest. This key must be
    // supplied by a secret manager and remain stable across restarts.
    std::array<uint8_t, 32> storageKey{};
    std::function<uint64_t()> nowUnixSeconds;

    bool isValid() const;
};

// SQLite WAL permits multiple service processes on one host to share the same
// database. Every mutation uses BEGIN IMMEDIATE and exact-epoch comparisons;
// only one competing Host claim can commit. A network/shared filesystem is not
// supported by SQLite and requires a database-backed IP2PSessionService adapter.
class SqliteP2PSessionService final : public IP2PSessionService {
public:
    SqliteP2PSessionService(
        SqliteP2PSessionServiceConfig config,
        const SessionTicketKeyPair& ticketKeys);
    ~SqliteP2PSessionService() override;

    SqliteP2PSessionService(const SqliteP2PSessionService&) = delete;
    SqliteP2PSessionService& operator=(const SqliteP2PSessionService&) = delete;

    bool isReady() const;
    std::string getLastError() const;
    SessionTicketPublicKey getTicketPublicKey() const;

    SessionServiceResult<P2PSessionGrant> createSession(
        const P2PSessionCreateRequest& request) override;
    SessionServiceResult<P2PSessionGrant> joinSession(
        const P2PSessionJoinRequest& request) override;
    SessionServiceResult<P2PBackendSessionInfo> heartbeat(
        const P2PSessionHeartbeatRequest& request) override;
    SessionServiceResult<P2PBackendSessionInfo> claimHost(
        const P2PSessionClaimHostRequest& request) override;
    SessionServiceResult<SessionServiceEmpty> leaveSession(
        const P2PSessionLeaveRequest& request) override;
    SessionServiceResult<P2PBackendSessionInfo> getSession(
        uint64_t sessionId) override;

    bool resolveSignalingCredential(
        const PeerId& peerId, const std::string& room,
        std::array<uint8_t, 32>& token,
        uint64_t& expiresAtUnixSeconds) const;

private:
    struct Impl;
    std::unique_ptr<Impl> _impl;
};

} // namespace ayt::net
