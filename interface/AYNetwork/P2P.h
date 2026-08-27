#pragma once
// AYNetwork/P2P.h - backend-neutral peer-to-peer connection contracts.

#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>
#include <utility>
#include <vector>

namespace ayt::net
{

// Stable application identity used by signaling.  It is deliberately not a
// SteamID: account services may map their own opaque user/session id into it.
struct PeerId {
    std::string value;

    PeerId() = default;
    explicit PeerId(std::string text) : value(std::move(text)) {}

    bool isValid() const {
        if (value.empty() || value.size() > 63) return false;
        for (unsigned char ch : value) {
            const bool alphaNum = (ch >= 'a' && ch <= 'z') ||
                                  (ch >= 'A' && ch <= 'Z') ||
                                  (ch >= '0' && ch <= '9');
            if (!alphaNum && ch != '-' && ch != '_' && ch != '.' && ch != ':') {
                return false;
            }
        }
        return true;
    }

    friend bool operator==(const PeerId&, const PeerId&) = default;
};

enum class P2PIcePolicy : uint8_t {
    DirectOnly = 0,     // private/LAN and STUN-derived public candidates only
    DirectOrRelay = 1, // prefer direct, allow TURN when direct traversal fails
    RelayOnly = 2,      // TURN only; useful for privacy or restrictive networks
};

enum class P2PPathKind : uint8_t {
    Unknown = 0,
    Direct = 1,
    Relayed = 2,
};

// Backend-neutral view of the engine-level P2P session. Signaling rooms are
// deliberately not exposed here: a platform lobby, account service, or the
// built-in UDP rendezvous backend may own that discovery metadata.
enum class P2PSessionRole : uint8_t {
    None = 0,
    Host = 1,
    Client = 2,
};

enum class P2PSessionState : uint8_t {
    Unconfigured = 0,
    Idle = 1,
    Hosting = 2,
    Connecting = 3,
    Active = 4,
    Joining = 5,
    Migrating = 6,
};

enum class P2PHostMigrationState : uint8_t {
    Disabled = 0,
    Stable = 1,
    Reconnecting = 2,
    Electing = 3,
    Promoting = 4,
    Failed = 5,
};

enum class P2PAdmissionState : uint8_t {
    NotRequired = 0,
    Pending = 1,
    Admitted = 2,
    Rejected = 3,
};

enum class P2PJoinRejectReason : uint8_t {
    None = 0,
    MissingTicket = 1,
    InvalidTicket = 2,
    SessionFull = 3,
    SessionClosed = 4,
    MalformedRequest = 5,
};

constexpr size_t kP2PMaxJoinTicketBytes = 1024;
constexpr size_t kP2PMaxSessionMembers = 64;

struct P2PJoinDecision {
    bool accepted = false;
    P2PJoinRejectReason reason = P2PJoinRejectReason::InvalidTicket;

    static P2PJoinDecision accept() {
        return {true, P2PJoinRejectReason::None};
    }
    static P2PJoinDecision reject(P2PJoinRejectReason why) {
        return {false, why == P2PJoinRejectReason::None
            ? P2PJoinRejectReason::InvalidTicket : why};
    }
};

enum class P2PPeerState : uint8_t {
    Connecting = 0,
    Ready = 1,
    Disconnecting = 2,
    Reserved = 3,
};

struct P2PConfig {
    PeerId localPeerId;
    uint16_t virtualPort = 0;
    P2PIcePolicy icePolicy = P2PIcePolicy::DirectOrRelay;

    // Comma-separated values are assembled by AYNetwork before passing them
    // to GNS.  Empty STUN disables public server-reflexive candidates; empty
    // TURN fields mean relay fallback is unavailable.
    std::vector<std::string> stunServers;
    std::vector<std::string> turnServers;
    std::vector<std::string> turnUsers;
    std::vector<std::string> turnPasswords;

    bool allowPrivateCandidates = true;
    bool isValid() const {
        const auto validList = [](const std::vector<std::string>& values) {
            for (const auto& value : values) {
                // GNS consumes these vectors as comma-separated lists.  Empty
                // entries or embedded delimiters would desynchronise TURN
                // server/user/password indices after joining.
                if (value.empty() || value.find(',') != std::string::npos) return false;
            }
            return true;
        };
        if (!localPeerId.isValid()) return false;
        if (!validList(stunServers) || !validList(turnServers) ||
            !validList(turnUsers) || !validList(turnPasswords)) return false;
        if (turnUsers.size() != turnPasswords.size()) return false;
        if (!turnUsers.empty() && turnUsers.size() != turnServers.size()) return false;
        if (icePolicy == P2PIcePolicy::RelayOnly && turnServers.empty()) return false;
        if (icePolicy == P2PIcePolicy::DirectOnly &&
            !allowPrivateCandidates && stunServers.empty()) return false;
        if (icePolicy == P2PIcePolicy::DirectOrRelay &&
            !allowPrivateCandidates && stunServers.empty() && turnServers.empty()) return false;
        return true;
    }
};

struct P2PConnectionInfo {
    PeerId localPeerId;
    PeerId remotePeerId;
    P2PPathKind path = P2PPathKind::Unknown;
    std::string remoteAddress;
    int pingMs = -1;
};

// One remote engine peer. connectionId is the stable AYNetwork id for this
// concrete connection; a reconnect can replace it while retaining peerId.
struct P2PPeerInfo {
    PeerId peerId;
    uint32_t connectionId = 0;
    P2PPeerState state = P2PPeerState::Connecting;
    P2PPathKind path = P2PPathKind::Unknown;
    std::string remoteAddress;
    int pingMs = -1;
    bool isSessionHost = false;
    bool admitted = false;
    uint32_t seatId = 0;
    bool reserved = false;
};

struct P2PSessionMemberInfo {
    PeerId peerId;
    uint32_t seatId = 0;
    bool connected = false;
    bool ready = false;
    bool isHost = false;
    bool reserved = false;
};

struct P2PReadyBarrierInfo {
    uint32_t revision = 0;
    size_t readyMemberCount = 0;
    size_t totalMemberCount = 0;
    bool localReady = false;
    bool open = false;
};

struct P2PSessionInfo {
    P2PSessionRole role = P2PSessionRole::None;
    P2PSessionState state = P2PSessionState::Unconfigured;
    PeerId localPeerId;
    PeerId hostPeerId;
    uint16_t virtualPort = 0;
    size_t readyPeerCount = 0;
    P2PAdmissionState admission = P2PAdmissionState::NotRequired;
    P2PJoinRejectReason rejectionReason = P2PJoinRejectReason::None;
    uint64_t sessionId = 0;
    uint32_t epoch = 0;
    uint32_t localSeatId = 0;
    size_t reservedPeerCount = 0;
    P2PHostMigrationState migration = P2PHostMigrationState::Disabled;
    PeerId previousHostPeerId;
    PeerId electedHostPeerId;
};

enum class P2PSessionEventType : uint8_t {
    SeatReserved = 0,
    SeatRestored = 1,
    SeatReservationExpired = 2,
    MigrationStarted = 3,
    AuthorityChanged = 4,
    MigrationFailed = 5,
};

// Application-facing lifecycle notification. Delivery occurs from the
// network update thread after transport pumping has completed, so handlers
// may safely update game/session state without running inside a GNS callback.
struct P2PSessionEvent {
    P2PSessionEventType type = P2PSessionEventType::MigrationFailed;
    P2PSessionInfo session;
    PeerId subjectPeerId;
    uint32_t seatId = 0;
};

// Signaling carries opaque GNS rendezvous blobs.  Implementations may use the
// built-in UDP rendezvous protocol, WebSocket/HTTPS, a platform SDK, or an
// application-specific authenticated service.  sendSignal() can be called by
// GNS from any thread and therefore must be thread-safe.
class ISignalingTransport {
public:
    using ReceiveHandler =
        std::function<void(const PeerId& sender, const void* data, size_t size)>;

    virtual ~ISignalingTransport() = default;
    virtual bool start(const PeerId& localPeer) = 0;
    virtual void stop() = 0;
    virtual bool isRunning() const = 0;
    virtual bool sendSignal(const PeerId& destination,
                            const void* data, size_t size) = 0;
    virtual size_t poll(const ReceiveHandler& handler,
                        size_t maxMessages = 64) = 0;
};

} // namespace ayt::net
