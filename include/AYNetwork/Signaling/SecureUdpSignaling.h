#pragma once
// SecureUdpSignaling.h - authenticated room-scoped signaling over UDP.

#include <AYNetwork/P2P.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>

namespace ayt::net
{

struct SignalingRoomId {
    std::string value;

    bool isValid() const;
    friend bool operator==(const SignalingRoomId&, const SignalingRoomId&) = default;
};

struct SignalingToken {
    static constexpr size_t kSize = 32;
    std::array<uint8_t, kSize> bytes{};

    bool isValid() const;
    friend bool operator==(const SignalingToken&, const SignalingToken&) = default;
};

bool parseSignalingTokenHex(const std::string& text, SignalingToken& token);
std::string signalingTokenToHex(const SignalingToken& token);

enum class SecureSignalingState : uint8_t {
    Stopped = 0,
    Registering,
    Ready,
    Failed,
};

enum class SecureSignalingError : uint8_t {
    None = 0,
    InvalidConfig,
    SocketFailure,
    AuthenticationFailed,
    CredentialExpired,
    NotRegistered,
    RoomMismatch,
    PeerUnavailable,
    RateLimited,
    QueueFull,
    ProtocolError,
};

struct SecureUdpSignalingClientConfig {
    std::string serverAddress = "127.0.0.1";
    uint16_t serverPort = 0;
    SignalingRoomId roomId;
    SignalingToken token;
    uint32_t heartbeatIntervalMs = 5000;
    uint32_t registrationRefreshMs = 10000;
    size_t maxDatagramBytes = 48u * 1024u;
    size_t maxPendingSignals = 128;
};

class SecureUdpSignalingClient final : public ISignalingTransport {
public:
    explicit SecureUdpSignalingClient(SecureUdpSignalingClientConfig config);
    ~SecureUdpSignalingClient() override;

    SecureUdpSignalingClient(const SecureUdpSignalingClient&) = delete;
    SecureUdpSignalingClient& operator=(const SecureUdpSignalingClient&) = delete;

    bool start(const PeerId& localPeer) override;
    void stop() override;
    bool isRunning() const override;
    bool sendSignal(const PeerId& destination,
                    const void* data, size_t size) override;
    size_t poll(const ReceiveHandler& handler,
                size_t maxMessages = 64) override;

    SecureSignalingState getState() const;
    SecureSignalingError getLastError() const;
    bool isReady() const;
    size_t getPendingSignalCount() const;

private:
    struct Impl;
    std::unique_ptr<Impl> _impl;
};

struct SecureSignalingCredential {
    SignalingToken token;
    // Zero means no protocol-level expiry. Production resolvers should always
    // return an expiry tied to the account/session service.
    uint64_t expiresAtUnixSeconds = 0;
};

using SecureSignalingCredentialResolver =
    std::function<bool(const PeerId&, const SignalingRoomId&,
                       SecureSignalingCredential&)>;

struct SecureUdpSignalingServerConfig {
    std::string bindAddress = "0.0.0.0";
    uint16_t port = 0;
    uint32_t peerTimeoutMs = 30000;
    // Revalidate active credentials against the resolver. Zero checks every
    // authenticated packet; production services may use a small cached value.
    uint32_t credentialRecheckIntervalMs = 5000;
    size_t maxDatagramBytes = 48u * 1024u;
    size_t maxPeers = 4096;
    uint32_t maxPacketsPerPeerPerSecond = 256;
    size_t maxBytesPerPeerPerSecond = 512u * 1024u;
    SecureSignalingCredentialResolver resolveCredential;
};

struct SecureSignalingServerStats {
    uint64_t authenticatedPackets = 0;
    uint64_t forwardedSignals = 0;
    uint64_t authenticationFailures = 0;
    uint64_t replayDrops = 0;
    uint64_t roomMismatchDrops = 0;
    uint64_t rateLimitedDrops = 0;
    uint64_t malformedDrops = 0;
};

class SecureUdpSignalingServer {
public:
    explicit SecureUdpSignalingServer(SecureUdpSignalingServerConfig config);
    ~SecureUdpSignalingServer();

    SecureUdpSignalingServer(const SecureUdpSignalingServer&) = delete;
    SecureUdpSignalingServer& operator=(const SecureUdpSignalingServer&) = delete;

    bool start();
    void stop();
    bool isRunning() const;
    uint16_t getBoundPort() const;
    size_t getPeerCount() const;
    size_t pump(size_t maxMessages = 256);
    void pruneExpired();
    SecureSignalingServerStats getStats() const;

private:
    struct Impl;
    std::unique_ptr<Impl> _impl;
};

} // namespace ayt::net
