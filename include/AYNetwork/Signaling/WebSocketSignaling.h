#pragma once
// Authenticated room-scoped signaling over ws/wss. TLS is normally
// terminated by the deployment reverse proxy in front of SessionServer.

#include <AYNetwork/P2P.h>
#include <AYNetwork/Signaling/SecureUdpSignaling.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

namespace ayt::net
{

struct WebSocketSignalingClientConfig {
    std::string serverAddress = "127.0.0.1";
    uint16_t serverPort = 0;
    std::string path = "/v1/signaling";
    bool useTls = false;
    SignalingRoomId roomId;
    SignalingToken token;
    uint32_t connectTimeoutMs = 3000;
    uint32_t ioTimeoutMs = 10000;
    size_t maxMessageBytes = 48u * 1024u;
    size_t maxPendingSignals = 128;

    bool isValid() const;
};

class WebSocketSignalingClient final : public ISignalingTransport {
public:
    explicit WebSocketSignalingClient(WebSocketSignalingClientConfig config);
    ~WebSocketSignalingClient() override;

    WebSocketSignalingClient(const WebSocketSignalingClient&) = delete;
    WebSocketSignalingClient& operator=(const WebSocketSignalingClient&) = delete;

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

} // namespace ayt::net
