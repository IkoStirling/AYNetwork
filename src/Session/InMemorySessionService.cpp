#include <AYNetwork/Session/InMemorySessionService.h>

#include <AYCrypto.h>

#include <algorithm>
#include <chrono>
#include <limits>
#include <map>
#include <mutex>
#include <unordered_map>

namespace ayt::net
{
namespace
{

constexpr char kHex[] = "0123456789abcdef";

uint64_t systemUnixSeconds() {
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()).count());
}

std::string toHex(const uint8_t* bytes, size_t size) {
    std::string out(size * 2, '0');
    for (size_t i = 0; i < size; ++i) {
        out[i * 2] = kHex[bytes[i] >> 4];
        out[i * 2 + 1] = kHex[bytes[i] & 0x0f];
    }
    return out;
}

int hexValue(char ch) {
    if (ch >= '0' && ch <= '9') return ch - '0';
    if (ch >= 'a' && ch <= 'f') return ch - 'a' + 10;
    if (ch >= 'A' && ch <= 'F') return ch - 'A' + 10;
    return -1;
}

bool fromHex(const std::string& text, uint8_t* bytes, size_t size) {
    if (!bytes || text.size() != size * 2) return false;
    for (size_t i = 0; i < size; ++i) {
        const int hi = hexValue(text[i * 2]);
        const int lo = hexValue(text[i * 2 + 1]);
        if (hi < 0 || lo < 0) return false;
        bytes[i] = static_cast<uint8_t>((hi << 4) | lo);
    }
    return true;
}

template <size_t N>
bool hasNonZero(const std::array<uint8_t, N>& value) {
    for (uint8_t byte : value) if (byte != 0) return true;
    return false;
}

} // namespace

struct InMemoryP2PSessionService::Impl {
    struct Member {
        PeerId peerId;
        std::array<uint8_t, 32> memberToken{};
        std::array<uint8_t, 32> signalingToken{};
        uint64_t signalingExpiresAt = 0;
    };

    struct Session {
        uint64_t sessionId = 0;
        uint32_t epoch = 1;
        PeerId hostPeerId;
        uint16_t virtualPort = 0;
        uint16_t capacity = 0;
        bool closed = false;
        uint64_t hostLeaseExpiresAt = 0;
        std::string signalingRoom;
        std::map<std::string, Member> members;
    };

    explicit Impl(InMemoryP2PSessionServiceConfig input,
                  const SessionTicketKeyPair* supplied)
        : config(std::move(input)) {
        if (!config.nowUnixSeconds) config.nowUnixSeconds = systemUnixSeconds;
        if (supplied) {
            keys = *supplied;
            keysReady = hasNonZero(keys.publicKey) &&
                        hasNonZero(keys.secretKey) &&
                        validateSessionTicketKeyPair(keys);
        } else {
            keysReady = generateSessionTicketKeyPair(keys);
        }
    }

    ~Impl() {
        ayt::crypto::secureZero(keys.secretKey.data(), keys.secretKey.size());
    }

    uint64_t now() const { return config.nowUnixSeconds(); }

    static SessionServiceError validateCreate(
        const P2PSessionCreateRequest& request) {
        if (!request.hostPeerId.isValid() || request.virtualPort == 0 ||
            request.capacity == 0 || request.capacity > kP2PMaxSessionMembers) {
            return SessionServiceError::InvalidRequest;
        }
        return SessionServiceError::None;
    }

    uint64_t allocateSessionId() const {
        for (unsigned attempt = 0; attempt < 32; ++attempt) {
            uint64_t value = 0;
            ayt::crypto::generateRandomBytes(
                reinterpret_cast<uint8_t*>(&value), sizeof(value));
            if (value != 0 && sessions.find(value) == sessions.end()) return value;
        }
        return 0;
    }

    Member makeMember(const PeerId& peer, uint64_t nowSeconds) const {
        Member member;
        member.peerId = peer;
        ayt::crypto::generateRandomBytes(
            member.memberToken.data(), member.memberToken.size());
        ayt::crypto::generateRandomBytes(
            member.signalingToken.data(), member.signalingToken.size());
        member.signalingExpiresAt = nowSeconds +
            config.signalingTokenLifetimeSeconds;
        return member;
    }

    P2PBackendSessionInfo describe(const Session& session,
                                   uint64_t nowSeconds) const {
        P2PBackendSessionInfo info;
        info.sessionId = session.sessionId;
        info.epoch = session.epoch;
        info.hostPeerId = session.hostPeerId;
        info.virtualPort = session.virtualPort;
        info.capacity = session.capacity;
        info.memberCount = static_cast<uint16_t>(session.members.size());
        info.open = !session.closed && nowSeconds < session.hostLeaseExpiresAt &&
                    session.members.size() < session.capacity;
        info.hostLeaseExpiresAtUnixSeconds = session.hostLeaseExpiresAt;
        info.signalingAddress = config.publicSignalingAddress;
        info.signalingPort = config.signalingPort;
        info.signalingRoom = session.signalingRoom;
        return info;
    }

    bool authenticate(const Session& session,
                      const P2PSessionMemberCredential& credential,
                      const Member*& member) const {
        member = nullptr;
        if (!credential.isValid() || credential.sessionId != session.sessionId) {
            return false;
        }
        const auto it = session.members.find(credential.peerId.value);
        if (it == session.members.end()) return false;
        std::array<uint8_t, 32> supplied{};
        if (!fromHex(credential.token, supplied.data(), supplied.size())) return false;
        const bool equal = ayt::crypto::secureMemCompare(
            supplied.data(), it->second.memberToken.data(), supplied.size()) == 0;
        ayt::crypto::secureZero(supplied.data(), supplied.size());
        if (!equal) return false;
        member = &it->second;
        return true;
    }

    SessionServiceResult<P2PSessionGrant> makeGrant(
        const Session& session, const Member& member, uint64_t nowSeconds) const {
        SessionJoinTicketClaims claims;
        claims.sessionId = session.sessionId;
        claims.epoch = session.epoch;
        claims.peerId = member.peerId;
        claims.issuedAtUnixSeconds = nowSeconds;
        claims.expiresAtUnixSeconds = nowSeconds +
            config.joinTicketLifetimeSeconds;
        ayt::crypto::generateRandomBytes(claims.nonce.data(), claims.nonce.size());

        P2PSessionGrant grant;
        grant.session = describe(session, nowSeconds);
        grant.member.sessionId = session.sessionId;
        grant.member.peerId = member.peerId;
        grant.member.token = toHex(member.memberToken.data(), member.memberToken.size());
        grant.signalingToken = toHex(
            member.signalingToken.data(), member.signalingToken.size());
        grant.ticketPublicKey = keys.publicKey;
        if (!issueSessionJoinTicket(claims, keys.secretKey, grant.joinTicket)) {
            return SessionServiceResult<P2PSessionGrant>::failure(
                SessionServiceError::InternalError, "failed to issue Join Ticket");
        }
        return SessionServiceResult<P2PSessionGrant>::success(std::move(grant));
    }

    InMemoryP2PSessionServiceConfig config;
    SessionTicketKeyPair keys{};
    bool keysReady = false;
    mutable std::mutex mutex;
    std::unordered_map<uint64_t, Session> sessions;
};

InMemoryP2PSessionService::InMemoryP2PSessionService(
    InMemoryP2PSessionServiceConfig config,
    const SessionTicketKeyPair* keyPair)
    : _impl(std::make_unique<Impl>(std::move(config), keyPair)) {}

InMemoryP2PSessionService::~InMemoryP2PSessionService() = default;

SessionServiceResult<P2PSessionGrant>
InMemoryP2PSessionService::createSession(
    const P2PSessionCreateRequest& request) {
    if (!_impl->keysReady || _impl->config.signalingPort == 0 ||
        _impl->config.publicSignalingAddress.empty() ||
        _impl->config.hostLeaseSeconds == 0 ||
        _impl->config.joinTicketLifetimeSeconds == 0 ||
        _impl->config.signalingTokenLifetimeSeconds == 0 ||
        Impl::validateCreate(request) != SessionServiceError::None) {
        return SessionServiceResult<P2PSessionGrant>::failure(
            SessionServiceError::InvalidRequest, "invalid session configuration");
    }
    std::lock_guard lock(_impl->mutex);
    if (_impl->sessions.size() >= _impl->config.maxSessions) {
        return SessionServiceResult<P2PSessionGrant>::failure(
            SessionServiceError::SessionFull, "session directory is full");
    }
    const uint64_t id = _impl->allocateSessionId();
    if (id == 0) {
        return SessionServiceResult<P2PSessionGrant>::failure(
            SessionServiceError::InternalError, "failed to allocate session id");
    }
    const uint64_t now = _impl->now();
    Impl::Session session;
    session.sessionId = id;
    session.hostPeerId = request.hostPeerId;
    session.virtualPort = request.virtualPort;
    session.capacity = request.capacity;
    session.hostLeaseExpiresAt = now + _impl->config.hostLeaseSeconds;
    session.signalingRoom = "s-" + toHex(
        reinterpret_cast<const uint8_t*>(&id), sizeof(id));
    auto [memberIt, inserted] = session.members.emplace(
        request.hostPeerId.value, _impl->makeMember(request.hostPeerId, now));
    if (!inserted || !hasNonZero(memberIt->second.memberToken) ||
        !hasNonZero(memberIt->second.signalingToken)) {
        return SessionServiceResult<P2PSessionGrant>::failure(
            SessionServiceError::InternalError,
            "failed to create secure host credentials");
    }
    auto [sessionIt, sessionInserted] =
        _impl->sessions.emplace(id, std::move(session));
    if (!sessionInserted) {
        return SessionServiceResult<P2PSessionGrant>::failure(
            SessionServiceError::InternalError, "session id collision");
    }
    return _impl->makeGrant(sessionIt->second,
                            sessionIt->second.members.begin()->second, now);
}

SessionServiceResult<P2PSessionGrant>
InMemoryP2PSessionService::joinSession(
    const P2PSessionJoinRequest& request) {
    if (request.sessionId == 0 || !request.peerId.isValid()) {
        return SessionServiceResult<P2PSessionGrant>::failure(
            SessionServiceError::InvalidRequest, "invalid join request");
    }
    std::lock_guard lock(_impl->mutex);
    const auto sessionIt = _impl->sessions.find(request.sessionId);
    if (sessionIt == _impl->sessions.end()) {
        return SessionServiceResult<P2PSessionGrant>::failure(
            SessionServiceError::SessionNotFound, "session not found");
    }
    Impl::Session& session = sessionIt->second;
    const uint64_t now = _impl->now();
    if (session.closed) {
        return SessionServiceResult<P2PSessionGrant>::failure(
            SessionServiceError::SessionClosed, "session is closed");
    }
    if (now >= session.hostLeaseExpiresAt) {
        return SessionServiceResult<P2PSessionGrant>::failure(
            SessionServiceError::HostLeaseExpired, "Host lease expired");
    }
    if (session.members.contains(request.peerId.value)) {
        return SessionServiceResult<P2PSessionGrant>::failure(
            SessionServiceError::Unauthorized, "peer is already a member");
    }
    if (session.members.size() >= session.capacity) {
        return SessionServiceResult<P2PSessionGrant>::failure(
            SessionServiceError::SessionFull, "session is full");
    }
    auto [memberIt, inserted] = session.members.emplace(
        request.peerId.value, _impl->makeMember(request.peerId, now));
    if (!inserted || !hasNonZero(memberIt->second.memberToken) ||
        !hasNonZero(memberIt->second.signalingToken)) {
        if (inserted) session.members.erase(memberIt);
        return SessionServiceResult<P2PSessionGrant>::failure(
            SessionServiceError::InternalError,
            "failed to create secure member credentials");
    }
    return _impl->makeGrant(session, memberIt->second, now);
}

SessionServiceResult<P2PBackendSessionInfo>
InMemoryP2PSessionService::heartbeat(
    const P2PSessionHeartbeatRequest& request) {
    std::lock_guard lock(_impl->mutex);
    const auto sessionIt = _impl->sessions.find(request.member.sessionId);
    if (sessionIt == _impl->sessions.end()) {
        return SessionServiceResult<P2PBackendSessionInfo>::failure(
            SessionServiceError::SessionNotFound, "session not found");
    }
    Impl::Session& session = sessionIt->second;
    const Impl::Member* member = nullptr;
    if (!_impl->authenticate(session, request.member, member) ||
        member->peerId != session.hostPeerId) {
        return SessionServiceResult<P2PBackendSessionInfo>::failure(
            SessionServiceError::Unauthorized, "only the current Host may heartbeat");
    }
    if (request.expectedEpoch != session.epoch) {
        return SessionServiceResult<P2PBackendSessionInfo>::failure(
            SessionServiceError::EpochConflict, "authority epoch changed");
    }
    const uint64_t now = _impl->now();
    if (now >= session.hostLeaseExpiresAt) {
        return SessionServiceResult<P2PBackendSessionInfo>::failure(
            SessionServiceError::HostLeaseExpired, "Host lease expired");
    }
    session.hostLeaseExpiresAt = now + _impl->config.hostLeaseSeconds;
    return SessionServiceResult<P2PBackendSessionInfo>::success(
        _impl->describe(session, now));
}

SessionServiceResult<P2PBackendSessionInfo>
InMemoryP2PSessionService::claimHost(
    const P2PSessionClaimHostRequest& request) {
    if (!request.newHostPeerId.isValid() || request.expectedEpoch == 0) {
        return SessionServiceResult<P2PBackendSessionInfo>::failure(
            SessionServiceError::InvalidRequest, "invalid Host claim");
    }
    std::lock_guard lock(_impl->mutex);
    const auto sessionIt = _impl->sessions.find(request.member.sessionId);
    if (sessionIt == _impl->sessions.end()) {
        return SessionServiceResult<P2PBackendSessionInfo>::failure(
            SessionServiceError::SessionNotFound, "session not found");
    }
    Impl::Session& session = sessionIt->second;
    const Impl::Member* caller = nullptr;
    if (!_impl->authenticate(session, request.member, caller)) {
        return SessionServiceResult<P2PBackendSessionInfo>::failure(
            SessionServiceError::Unauthorized, "invalid member credential");
    }
    if (request.expectedEpoch != session.epoch) {
        return SessionServiceResult<P2PBackendSessionInfo>::failure(
            SessionServiceError::EpochConflict, "authority epoch changed");
    }
    const auto newHostIt = session.members.find(request.newHostPeerId.value);
    if (newHostIt == session.members.end() ||
        request.newHostPeerId == session.hostPeerId) {
        return SessionServiceResult<P2PBackendSessionInfo>::failure(
            SessionServiceError::InvalidRequest,
            "new Host must be a different admitted member");
    }
    const uint64_t now = _impl->now();
    const bool gracefulTransfer = now < session.hostLeaseExpiresAt &&
                                  caller->peerId == session.hostPeerId;
    const bool expiredSelfClaim = now >= session.hostLeaseExpiresAt &&
                                  caller->peerId == request.newHostPeerId;
    if (!gracefulTransfer && !expiredSelfClaim) {
        return SessionServiceResult<P2PBackendSessionInfo>::failure(
            SessionServiceError::Unauthorized,
            "active Host lease prevents this claim");
    }
    if (session.epoch == std::numeric_limits<uint32_t>::max()) {
        return SessionServiceResult<P2PBackendSessionInfo>::failure(
            SessionServiceError::InternalError, "authority epoch exhausted");
    }
    ++session.epoch;
    session.hostPeerId = request.newHostPeerId;
    session.hostLeaseExpiresAt = now + _impl->config.hostLeaseSeconds;
    return SessionServiceResult<P2PBackendSessionInfo>::success(
        _impl->describe(session, now));
}

SessionServiceResult<SessionServiceEmpty>
InMemoryP2PSessionService::leaveSession(
    const P2PSessionLeaveRequest& request) {
    std::lock_guard lock(_impl->mutex);
    const auto sessionIt = _impl->sessions.find(request.member.sessionId);
    if (sessionIt == _impl->sessions.end()) {
        return SessionServiceResult<SessionServiceEmpty>::failure(
            SessionServiceError::SessionNotFound, "session not found");
    }
    Impl::Session& session = sessionIt->second;
    const Impl::Member* member = nullptr;
    if (!_impl->authenticate(session, request.member, member)) {
        return SessionServiceResult<SessionServiceEmpty>::failure(
            SessionServiceError::Unauthorized, "invalid member credential");
    }
    const bool leavingHost = member->peerId == session.hostPeerId;
    const std::string peer = member->peerId.value;
    if (leavingHost) {
        if (_impl->now() >= session.hostLeaseExpiresAt) {
            return SessionServiceResult<SessionServiceEmpty>::failure(
                SessionServiceError::HostLeaseExpired,
                "expired Host cannot terminate the session");
        }
        _impl->sessions.erase(sessionIt);
    } else {
        session.members.erase(peer);
    }
    return SessionServiceResult<SessionServiceEmpty>::success({});
}

SessionServiceResult<P2PBackendSessionInfo>
InMemoryP2PSessionService::getSession(uint64_t sessionId) {
    if (sessionId == 0) {
        return SessionServiceResult<P2PBackendSessionInfo>::failure(
            SessionServiceError::InvalidRequest, "invalid session id");
    }
    std::lock_guard lock(_impl->mutex);
    const auto it = _impl->sessions.find(sessionId);
    if (it == _impl->sessions.end()) {
        return SessionServiceResult<P2PBackendSessionInfo>::failure(
            SessionServiceError::SessionNotFound, "session not found");
    }
    return SessionServiceResult<P2PBackendSessionInfo>::success(
        _impl->describe(it->second, _impl->now()));
}

SessionTicketPublicKey InMemoryP2PSessionService::getTicketPublicKey() const {
    std::lock_guard lock(_impl->mutex);
    return _impl->keys.publicKey;
}

bool InMemoryP2PSessionService::resolveSignalingCredential(
    const PeerId& peerId, const std::string& room,
    std::array<uint8_t, 32>& token,
    uint64_t& expiresAtUnixSeconds) const {
    token = {};
    expiresAtUnixSeconds = 0;
    if (!peerId.isValid() || room.empty()) return false;
    std::lock_guard lock(_impl->mutex);
    const uint64_t now = _impl->now();
    for (const auto& [id, session] : _impl->sessions) {
        (void)id;
        if (session.closed || session.signalingRoom != room) continue;
        const auto memberIt = session.members.find(peerId.value);
        if (memberIt == session.members.end() ||
            now >= memberIt->second.signalingExpiresAt) return false;
        token = memberIt->second.signalingToken;
        expiresAtUnixSeconds = memberIt->second.signalingExpiresAt;
        return true;
    }
    return false;
}

} // namespace ayt::net
