// AYNetwork_P2PSmokePeer - two-process custom-signaling/ICE smoke utility.

#include <AYNetwork/P2P.h>
#include <AYNetwork/Signaling/UdpSignaling.h>
#include <AYNetwork/Transport/GnsConnection.h>

#include <steam/steamclientpublic.h>
#include <steam/isteamnetworkingsockets.h>

#include <charconv>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <memory>
#include <string>
#include <thread>

using namespace ayt::net;

namespace
{

bool parsePort(const char* text, uint16_t& value) {
    if (!text) return false;
    unsigned parsed = 0;
    const std::string input{text};
    const auto result = std::from_chars(input.data(), input.data() + input.size(), parsed);
    if (result.ec != std::errc{} || result.ptr != input.data() + input.size() ||
        parsed > 65535) return false;
    value = static_cast<uint16_t>(parsed);
    return true;
}

void pumpSignaling(const std::shared_ptr<ISignalingTransport>& signaling) {
    (void)signaling->poll(
        [signaling](const PeerId& sender, const void* data, size_t size) {
            (void)GnsConnection::receiveP2PSignal(sender, data, size, signaling);
        });
    (void)GnsConnection::pump();
}

P2PConfig makeDirectConfig(const PeerId& local, uint16_t virtualPort) {
    P2PConfig config;
    config.localPeerId = local;
    config.virtualPort = virtualPort;
    config.icePolicy = P2PIcePolicy::DirectOnly;
    config.allowPrivateCandidates = true;
    return config;
}

int runHost(const PeerId& local,
            const std::shared_ptr<ISignalingTransport>& signaling,
            uint16_t virtualPort) {
    std::unique_ptr<GnsConnection> peer;
    bool received = false;
    GnsConnection::setP2PAdoptFactory(
        virtualPort, signaling,
        [&](HSteamNetConnection incoming, const PeerId& remote) -> GnsConnection* {
            if (peer || !GnsConnection::s_gns) return nullptr;
            if (GnsConnection::s_gns->AcceptConnection(incoming) != k_EResultOK) return nullptr;
            peer = std::make_unique<GnsConnection>();
            peer->setProtocolVersion(0);
            peer->adoptIncomingP2PConnection(incoming, remote, virtualPort);
            peer->onData([&](const uint8_t* data, size_t size) {
                static constexpr char kPayload[] = "AY_P2P_SMOKE";
                if (size == sizeof(kPayload) - 1 &&
                    std::memcmp(data, kPayload, size) == 0) {
                    received = peer->send(CHANNEL_RELIABLE, data, size) == 0;
                }
            });
            return peer.get();
        });

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(15);
    while (!received && std::chrono::steady_clock::now() < deadline) {
        pumpSignaling(signaling);
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    if (received) {
        const auto linger = std::chrono::steady_clock::now() + std::chrono::milliseconds(500);
        while (std::chrono::steady_clock::now() < linger) {
            pumpSignaling(signaling);
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }
    GnsConnection::clearP2PAdoptFactory(virtualPort);
    if (peer) peer->disconnect("smoke host done");
    return received ? 0 : 3;
}

int runJoin(const PeerId& local, const PeerId& remote,
            const std::shared_ptr<ISignalingTransport>& signaling,
            const P2PConfig& config) {
    GnsConnection peer;
    peer.setProtocolVersion(0);
    bool echoed = false;
    peer.onData([&](const uint8_t* data, size_t size) {
        static constexpr char kPayload[] = "AY_P2P_SMOKE";
        echoed = size == sizeof(kPayload) - 1 &&
                 std::memcmp(data, kPayload, size) == 0;
    });
    if (!peer.initP2PClient(remote, config, signaling)) return 4;
    const auto connectDeadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(15);
    while (!peer.isConnected() && std::chrono::steady_clock::now() < connectDeadline) {
        pumpSignaling(signaling);
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    static constexpr char kPayload[] = "AY_P2P_SMOKE";
    if (!peer.isConnected() ||
        peer.send(CHANNEL_RELIABLE, kPayload, sizeof(kPayload) - 1) != 0) {
        peer.disconnect("smoke connect/send failed");
        return 5;
    }
    const auto echoDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!echoed && std::chrono::steady_clock::now() < echoDeadline) {
        pumpSignaling(signaling);
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    peer.disconnect("smoke join done");
    return echoed ? 0 : 6;
}

} // namespace

int main(int argc, char** argv) {
    if (argc != 6 && argc != 7) {
        std::fprintf(stderr,
            "host: AYNetwork_P2PSmokePeer host <local-peer> <signal-ip> <signal-port> <virtual-port>\n"
            "join: AYNetwork_P2PSmokePeer join <local-peer> <remote-peer> <signal-ip> <signal-port> <virtual-port>\n");
        return 2;
    }
    const bool host = std::strcmp(argv[1], "host") == 0;
    const bool join = std::strcmp(argv[1], "join") == 0;
    if ((!host && !join) || (host && argc != 6) || (join && argc != 7)) return 2;
    const PeerId local{argv[2]};
    const PeerId remote{join ? argv[3] : ""};
    const char* signalAddress = argv[join ? 4 : 3];
    uint16_t signalPort = 0;
    uint16_t virtualPort = 0;
    if (!local.isValid() || (join && !remote.isValid()) ||
        !parsePort(argv[join ? 5 : 4], signalPort) || signalPort == 0 ||
        !parsePort(argv[join ? 6 : 5], virtualPort)) return 2;

    auto signaling = std::make_shared<UdpSignalingClient>(
        UdpSignalingClientConfig{.serverAddress = signalAddress,
                                 .serverPort = signalPort});
    if (!gns::init() || !signaling->start(local) ||
        !GnsConnection::setLocalP2PIdentity(local)) {
        signaling->stop();
        gns::shutdown();
        return 1;
    }
    const P2PConfig config = makeDirectConfig(local, virtualPort);
    const int result = host
        ? runHost(local, signaling, virtualPort)
        : runJoin(local, remote, signaling, config);
    signaling->stop();
    gns::shutdown();
    return result;
}
