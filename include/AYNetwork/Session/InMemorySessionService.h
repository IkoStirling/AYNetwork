#pragma once
// Thread-safe reference authority-session service used by the standalone tool.

#include <AYNetwork/Session/SessionTicket.h>
#include <AYNetwork/SessionService.h>

#include <array>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>

namespace ayt::net
{

struct InMemoryP2PSessionServiceConfig {
    std::string publicSignalingAddress = "127.0.0.1";
    uint16_t signalingPort = 0;
    uint32_t hostLeaseSeconds = 10;
    uint32_t joinTicketLifetimeSeconds = 60;
    uint32_t signalingTokenLifetimeSeconds = 24u * 60u * 60u;
    size_t maxSessions = 4096;
    std::function<uint64_t()> nowUnixSeconds;
};

class InMemoryP2PSessionService final : public IP2PSessionService {
public:
    explicit InMemoryP2PSessionService(
        InMemoryP2PSessionServiceConfig config,
        const SessionTicketKeyPair* keyPair = nullptr);
    ~InMemoryP2PSessionService() override;

    InMemoryP2PSessionService(const InMemoryP2PSessionService&) = delete;
    InMemoryP2PSessionService& operator=(const InMemoryP2PSessionService&) = delete;

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

    SessionTicketPublicKey getTicketPublicKey() const;

    // Adapter seam consumed by SecureUdpSignalingServer's credential resolver.
    bool resolveSignalingCredential(
        const PeerId& peerId, const std::string& room,
        std::array<uint8_t, 32>& token,
        uint64_t& expiresAtUnixSeconds) const;

private:
    struct Impl;
    std::unique_ptr<Impl> _impl;
};

} // namespace ayt::net
