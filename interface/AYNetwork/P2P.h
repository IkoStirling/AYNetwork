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
