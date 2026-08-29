#include <AYNetwork/Session/HttpSessionService.h>
#include <AYNetwork/Session/InMemorySessionService.h>
#include <AYNetwork/Signaling/WebSocketSignaling.h>
#include <AYTest.h>
#include <httplib.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include <thread>

using namespace ayt::net;

namespace
{

SignalingToken tokenWith(uint8_t value) {
    SignalingToken token;
    token.bytes.fill(value);
    return token;
}

bool waitSignal(WebSocketSignalingClient& client, const PeerId& sender,
                const std::string& expected) {
    const auto deadline = std::chrono::steady_clock::now() +
        std::chrono::seconds(2);
    while (std::chrono::steady_clock::now() < deadline) {
        bool received = false;
        client.poll([&](const PeerId& from, const void* bytes, size_t size) {
            received = from == sender &&
                std::string(static_cast<const char*>(bytes), size) == expected;
        });
        if (received) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return false;
}

} // namespace

TEST_SUITE(WebSocketSignaling)

TEST_CASE(WebSocketSignalingAuthenticatesAndForwardsOpaqueFrames) {
    ayt::test::setCurrentCase(
        "WebSocketSignalingAuthenticatesAndForwardsOpaqueFrames");
    InMemoryP2PSessionServiceConfig serviceConfig;
    serviceConfig.publicSignalingAddress = "ws://127.0.0.1/v1/signaling";
    serviceConfig.signalingPort = 28081;
    auto service = std::make_shared<InMemoryP2PSessionService>(serviceConfig);
    const SignalingToken hostToken = tokenWith(0x31);
    const SignalingToken joinToken = tokenWith(0x52);
    auto joinAllowed = std::make_shared<std::atomic<bool>>(true);

    HttpP2PSessionServerConfig serverConfig;
    serverConfig.bindAddress = "127.0.0.1";
    serverConfig.enableWebSocketSignaling = true;
    serverConfig.webSocketCredentialRecheckSeconds = 1;
    serverConfig.signalingCredentialResolver =
        [hostToken, joinToken, joinAllowed](
            const PeerId& peer, const SignalingRoomId& room,
            SecureSignalingCredential& credential) {
            if (room.value != "room-one") return false;
            if (peer == PeerId{"ws-host"}) credential.token = hostToken;
            else if (peer == PeerId{"ws-join"} && joinAllowed->load()) {
                credential.token = joinToken;
            }
            else return false;
            return true;
        };
    HttpP2PSessionServer server(std::move(serverConfig), service);
    CHECK(server.start());
    CHECK(server.isReady());
    httplib::Client probe("127.0.0.1", server.getBoundPort());
    const auto live = probe.Get("/livez");
    const auto ready = probe.Get("/readyz");
    const auto metricsBefore = probe.Get("/metrics");
    CHECK(live && live->status == 200);
    CHECK(ready && ready->status == 200);
    CHECK(metricsBefore && metricsBefore->status == 200);

    WebSocketSignalingClientConfig hostConfig;
    hostConfig.serverPort = server.getBoundPort();
    hostConfig.roomId = SignalingRoomId{"room-one"};
    hostConfig.token = hostToken;
    WebSocketSignalingClient host(hostConfig);
    WebSocketSignalingClientConfig joinConfig = hostConfig;
    joinConfig.token = joinToken;
    WebSocketSignalingClient join(joinConfig);
    CHECK(host.start(PeerId{"ws-host"}));
    CHECK(join.start(PeerId{"ws-join"}));
    const std::string payload = "opaque-gns-signal";
    CHECK(host.sendSignal(PeerId{"ws-join"}, payload.data(), payload.size()));
    CHECK(waitSignal(join, PeerId{"ws-host"}, payload));

    joinAllowed->store(false);
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    CHECK(host.sendSignal(PeerId{"ws-join"}, payload.data(), payload.size()));
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    CHECK_INT_EQ(join.poll([](const PeerId&, const void*, size_t) {}), 0);

    WebSocketSignalingClientConfig deniedConfig = hostConfig;
    deniedConfig.token = tokenWith(0x7f);
    WebSocketSignalingClient denied(deniedConfig);
    CHECK(!denied.start(PeerId{"ws-host-bad"}));
    const auto metrics = server.getMetrics();
    CHECK_INT_EQ(metrics.websocketConnections, 2);
    CHECK_INT_EQ(metrics.websocketForwarded, 1);
    CHECK_INT_EQ(metrics.websocketAuthenticationFailures, 1);

    server.setReady(false);
    CHECK(!server.isReady());
    const auto draining = probe.Get("/readyz");
    CHECK(draining && draining->status == 503);
    join.stop();
    host.stop();
    server.stop();
}

} // TEST_SUITE(WebSocketSignaling)
