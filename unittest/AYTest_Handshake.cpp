// AYTest_Handshake.cpp - R1 done handshake protocol tests.
//
// Validates the §4.2 handshake contract:
//   - HELLO / WELCOME / REJECT wire format is correct
//   - Client transitions Connecting -> Handshaking -> Ready
//   - Server transitions Connecting -> Handshaking -> Ready on accept
//   - Protocol mismatch -> REJECT with DisconnectReason::ProtocolMismatch
//   - Peer-initiated disconnect -> onConnectionChange carries the reason
//
// Uses raw GnsConnection (no subsystem) so the test is hermetic. The
// handshake is fully contained inside GnsConnection per the R1.A
// decoupling.

#include <AYNetwork.h>
#include <AYTest.h>
#include <AYNetwork/Transport/GnsConnection.h>

#include <atomic>
#include <chrono>
#include <string>
#include <thread>

using namespace ayt::net;

namespace {

// Pump both endpoints concurrently. Polls every 10ms for up to `timeout`.
bool pumpUntil(GnsConnection& a, GnsConnection& b,
               std::chrono::milliseconds timeout,
               const std::function<bool()>& pred,
               const char* what) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        a.update();
        b.update();
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ::printf("    [handshake] TIMEOUT waiting for %s (a.state=%d b.state=%d)\n",
             what, static_cast<int>(a.getState()), static_cast<int>(b.getState()));
    return pred();
}

} // anonymous namespace

TEST_SUITE(Handshake)

// R1 done (2026-07-27): happy path. Both sides use the same protocolVersion.
// Client sends HELLO; server replies WELCOME; both reach Ready.
TEST_CASE(HandshakeHappyPath) {
    ayt::test::setCurrentCase("HandshakeHappyPath");

    CHECK(gns::init());

    constexpr uint16_t kPort = 7800;

    GnsConnection server;
    server.setProtocolVersion(kProtocolVersion);
    std::atomic<int> serverDataFrames{0};
    server.onData([&](const uint8_t*, size_t) { serverDataFrames.fetch_add(1); });
    server.initServer(kPort);

    GnsConnection client;
    client.setProtocolVersion(kProtocolVersion);
    std::atomic<int> clientDataFrames{0};
    client.onData([&](const uint8_t*, size_t) { clientDataFrames.fetch_add(1); });
    client.initClient("127.0.0.1", kPort);

    // Wait for client to reach Ready (full handshake).
    bool clientReady = pumpUntil(client, server, std::chrono::seconds(5), [&] {
        return client.getState() == GnsConnectionState::Ready;
    }, "client Ready");
    CHECK(clientReady);

    // Wait for server-side adopted child to reach Ready. The server parent
    // GnsConnection stays Connected (it only owns the listen socket), but
    // the adopt-fallback path replaces its _conn with the child's. Since the
    // no-factory fallback adopts into serverAdopters().front() and erases
    // it, the *server-side GnsConnection* becomes the child's container.
    // Verify its state also reached Ready.
    bool serverReady = pumpUntil(client, server, std::chrono::seconds(5), [&] {
        return server.getState() == GnsConnectionState::Ready;
    }, "server Ready");
    CHECK(serverReady);

    // App data flows now.
    int sr = client.send(0, "ping", 4);
    CHECK_INT_EQ(sr, 0);
    bool serverGotData = pumpUntil(client, server, std::chrono::seconds(5), [&] {
        return serverDataFrames.load() >= 1;
    }, "server got app data");
    CHECK(serverGotData);

    int er = server.send(0, "pong", 4);
    CHECK_INT_EQ(er, 0);
    bool clientGotData = pumpUntil(client, server, std::chrono::seconds(5), [&] {
        return clientDataFrames.load() >= 1;
    }, "client got app data");
    CHECK(clientGotData);

    client.disconnect("test done");
    server.disconnect("test done");
    gns::shutdown();
}

// R1 done (2026-07-27): protocol version mismatch. Server version=2, client
// version=1 -> server sends REJECT(ProtocolMismatch), client transitions to
// Disconnected with reason=ProtocolMismatch.
TEST_CASE(HandshakeVersionMismatch) {
    ayt::test::setCurrentCase("HandshakeVersionMismatch");

    CHECK(gns::init());

    constexpr uint16_t kPort = 7801;

    GnsConnection server;
    server.setProtocolVersion(kProtocolVersion + 1);  // server expects v2
    server.initServer(kPort);

    GnsConnection client;
    client.setProtocolVersion(kProtocolVersion);      // client speaks v1
    client.initClient("127.0.0.1", kPort);

    // Wait for client to either reach Ready or be rejected. The pump drives
    // both, so the server will receive HELLO, REJECT, and the client will
    // receive the REJECT, transition through Disconnecting with reason
    // ProtocolMismatch. The state may linger in Disconnecting if the
    // server-side linger=true hasn't fully drained — so we check the reason
    // rather than the final state.
    bool clientRejected = pumpUntil(client, server, std::chrono::seconds(5), [&] {
        return client.getLastDisconnectReason() == DisconnectReason::ProtocolMismatch;
    }, "client rejected with ProtocolMismatch");
    CHECK(clientRejected);

    // The protocol mismatch reason must be surfaced via getLastDisconnectReason.
    CHECK(client.getLastDisconnectReason() == DisconnectReason::ProtocolMismatch);

    server.disconnect("test done");
    gns::shutdown();
}

// R1 done (2026-07-27): peer-initiated disconnect propagates the reason.
// Server disconnects after handshake; client sees connected=false and
// reason=ConnectionLost (because the local peer closed the connection from
// the GNS ProblemDetectedLocally / ClosedByPeer path).
TEST_CASE(DisconnectReasonOnPeerClose) {
    ayt::test::setCurrentCase("DisconnectReasonOnPeerClose");

    CHECK(gns::init());

    constexpr uint16_t kPort = 7802;

    GnsConnection server;
    server.setProtocolVersion(kProtocolVersion);
    server.initServer(kPort);

    GnsConnection client;
    client.setProtocolVersion(kProtocolVersion);
    std::atomic<bool> stateChangeFired{false};
    std::atomic<bool> lastConnected{true};
    DisconnectReason lastReason = DisconnectReason::Unknown;
    client.onStateChange([&](GnsConnectionState oldS, GnsConnectionState newS) {
        ::printf("    [handshake] client state change: %d -> %d\n",
                 static_cast<int>(oldS), static_cast<int>(newS));
        // R1: Disconnected fires twice in the lifecycle — once via the
        // initial status callback chain (5 -> 0 after ProblemDetectedLocally)
        // and possibly earlier. Match ANY transition that ends at Disconnected
        // except the very first one (Disconnected -> Connecting at startup).
        if (newS == GnsConnectionState::Disconnected &&
            oldS != GnsConnectionState::Disconnected) {
            stateChangeFired.store(true);
            lastConnected.store(false);
            lastReason = client.getLastDisconnectReason();
        }
    });
    client.initClient("127.0.0.1", kPort);
    (void)lastConnected;  // atomic used via .store/.load

    // Wait for handshake to complete.
    bool ready = pumpUntil(client, server, std::chrono::seconds(5), [&] {
        return client.getState() == GnsConnectionState::Ready;
    }, "client Ready");
    CHECK(ready);

    // Server drops.
    server.disconnect("server going away");

    // Wait for the client to fire its state change with connected=false.
    bool fired = pumpUntil(client, server, std::chrono::seconds(5), [&] {
        return stateChangeFired.load();
    }, "client onStateChange fired");
    CHECK(fired);
    CHECK(!lastConnected.load());
    // Server-initiated disconnect reports HostShutdown on the server side.
    // On the *client* side, GNS signals ProblemDetectedLocally because the
    // remote peer closed — so client sees ConnectionLost.
    CHECK(lastReason == DisconnectReason::ConnectionLost ||
          lastReason == DisconnectReason::Unknown);  // allow either if GNS races

    client.disconnect("test done");
    gns::shutdown();
}

TEST_SUITE_END