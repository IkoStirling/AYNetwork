#pragma once
// Backend-neutral authority-session directory contract for P2P games.

#include <AYNetwork/P2P.h>

#include <array>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

namespace ayt::net
{

enum class SessionServiceError : uint8_t {
    None = 0,
    InvalidRequest,
    Unauthorized,
    SessionNotFound,
    SessionFull,
    SessionClosed,
    EpochConflict,
    HostLeaseExpired,
    TransportError,
    ProtocolError,
    InternalError,
    RateLimited,
};

template <typename T>
struct SessionServiceResult {
    SessionServiceError error = SessionServiceError::None;
    std::string message;
    T value{};

    bool ok() const { return error == SessionServiceError::None; }
    explicit operator bool() const { return ok(); }

    static SessionServiceResult success(T result) {
        SessionServiceResult out;
        out.value = std::move(result);
        return out;
    }

    static SessionServiceResult failure(SessionServiceError why,
                                        std::string detail = {}) {
        SessionServiceResult out;
        out.error = why == SessionServiceError::None
            ? SessionServiceError::InternalError : why;
        out.message = std::move(detail);
        return out;
    }
};

struct SessionServiceEmpty {};

// Public, non-secret metadata for one authority session. Session discovery is
// intentionally separate from GNS signaling and real-time game traffic.
struct P2PBackendSessionInfo {
    uint64_t sessionId = 0;
    uint32_t epoch = 0;
    PeerId hostPeerId;
    uint16_t virtualPort = 0;
    uint16_t capacity = 0;
    uint16_t memberCount = 0;
    bool open = false;
    uint64_t hostLeaseExpiresAtUnixSeconds = 0;

    std::string signalingAddress;
    uint16_t signalingPort = 0;
    std::string signalingRoom;

    bool isValid() const {
        return sessionId != 0 && epoch != 0 && hostPeerId.isValid() &&
               virtualPort != 0 && capacity != 0 && memberCount != 0 &&
               memberCount <= capacity && !signalingAddress.empty() &&
               signalingPort != 0 && !signalingRoom.empty();
    }
};

// Per-member secret used to authenticate heartbeat/claim/leave requests to
// the session service. It is unrelated to the GNS signaling token.
struct P2PSessionMemberCredential {
    uint64_t sessionId = 0;
    PeerId peerId;
    std::string token;

    bool isValid() const {
        return sessionId != 0 && peerId.isValid() && token.size() == 64;
    }
};

// Returned from create/join. The Join Ticket is signed and may be handed
// to AYNetwork through setP2PJoinTicket(); the signaling token is consumed by
// SecureUdpSignalingClient and must not be reused as a member credential.
struct P2PSessionGrant {
    P2PBackendSessionInfo session;
    P2PSessionMemberCredential member;
    std::string signalingToken;
    std::vector<uint8_t> joinTicket;
    std::array<uint8_t, 32> ticketPublicKey{};

    bool isValid() const {
        bool hasPublicKey = false;
        for (uint8_t byte : ticketPublicKey) hasPublicKey = hasPublicKey || byte != 0;
        return session.isValid() && member.isValid() &&
               member.sessionId == session.sessionId &&
               signalingToken.size() == 64 && !joinTicket.empty() && hasPublicKey;
    }
};

struct P2PSessionCreateRequest {
    PeerId hostPeerId;
    uint16_t virtualPort = 0;
    uint16_t capacity = 8;
};

struct P2PSessionJoinRequest {
    uint64_t sessionId = 0;
    PeerId peerId;
};

struct P2PSessionHeartbeatRequest {
    P2PSessionMemberCredential member;
    uint32_t expectedEpoch = 0;
};

// The current Host may transfer immediately to an admitted member. A
// non-Host member may claim only after the existing Host lease expires. Both
// paths compare expectedEpoch atomically and advance it exactly once.
struct P2PSessionClaimHostRequest {
    P2PSessionMemberCredential member;
    uint32_t expectedEpoch = 0;
    PeerId newHostPeerId;
};

struct P2PSessionLeaveRequest {
    P2PSessionMemberCredential member;
};

class IP2PSessionService {
public:
    virtual ~IP2PSessionService() = default;

    virtual SessionServiceResult<P2PSessionGrant> createSession(
        const P2PSessionCreateRequest& request) = 0;
    virtual SessionServiceResult<P2PSessionGrant> joinSession(
        const P2PSessionJoinRequest& request) = 0;
    virtual SessionServiceResult<P2PBackendSessionInfo> heartbeat(
        const P2PSessionHeartbeatRequest& request) = 0;
    virtual SessionServiceResult<P2PBackendSessionInfo> claimHost(
        const P2PSessionClaimHostRequest& request) = 0;
    virtual SessionServiceResult<SessionServiceEmpty> leaveSession(
        const P2PSessionLeaveRequest& request) = 0;
    virtual SessionServiceResult<P2PBackendSessionInfo> getSession(
        uint64_t sessionId) = 0;
};

} // namespace ayt::net
