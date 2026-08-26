#pragma once
// AYNetwork/Signaling/UdpSignaling.h - small self-hosted signaling backend.

#include <AYNetwork/P2P.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

namespace ayt::net
{

struct UdpSignalingClientConfig {
    std::string serverAddress = "127.0.0.1";
    uint16_t serverPort = 0;
    uint32_t heartbeatIntervalMs = 5000;
    size_t maxDatagramBytes = 48u * 1024u;
};

// Lightweight built-in backend intended for self-hosting and development.
// It performs rendezvous forwarding only; production account authentication
// can be supplied by replacing ISignalingTransport without changing P2P code.
class UdpSignalingClient final : public ISignalingTransport {
public:
    explicit UdpSignalingClient(UdpSignalingClientConfig config);
    ~UdpSignalingClient() override;

    UdpSignalingClient(const UdpSignalingClient&) = delete;
    UdpSignalingClient& operator=(const UdpSignalingClient&) = delete;

    bool start(const PeerId& localPeer) override;
    void stop() override;
    bool isRunning() const override;
    bool sendSignal(const PeerId& destination,
                    const void* data, size_t size) override;
    size_t poll(const ReceiveHandler& handler,
                size_t maxMessages = 64) override;

private:
    struct Impl;
    std::unique_ptr<Impl> _impl;
};

struct UdpSignalingServerConfig {
    std::string bindAddress = "0.0.0.0";
    uint16_t port = 0;
    uint32_t peerTimeoutMs = 30000;
    size_t maxDatagramBytes = 48u * 1024u;
    size_t maxPeers = 4096;
};

// Pump-driven rendezvous server used by the standalone executable and tests.
// No engine/game state is hosted here: it only remembers peer UDP endpoints
// and forwards opaque signaling datagrams.
class UdpSignalingServer {
public:
    explicit UdpSignalingServer(UdpSignalingServerConfig config = {});
    ~UdpSignalingServer();

    UdpSignalingServer(const UdpSignalingServer&) = delete;
    UdpSignalingServer& operator=(const UdpSignalingServer&) = delete;

    bool start();
    void stop();
    bool isRunning() const;
    uint16_t getBoundPort() const;
    size_t getPeerCount() const;
    size_t pump(size_t maxMessages = 256);
    void pruneExpired();

private:
    struct Impl;
    std::unique_ptr<Impl> _impl;
};

} // namespace ayt::net
