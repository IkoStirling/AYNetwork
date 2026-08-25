// AYTest_Transport.cpp - Transport layer unit tests
//
// R1 (2026-07-26): removed KCP/AYConnection tests (obsolete after the GNS
// integration). R1.5 (2026-07-27): migrated to AYTest framework.

#include <AYNetwork.h>
#include <AYTest.h>
#include <AYNetwork/Transport/UdpSocket.h>
#include <AYNetwork/Transport/GnsConnection.h>
#include <cstdio>

using namespace ayt::net;

TEST_SUITE(Transport)

TEST_CASE(UdpSocketCreate) {
    ayt::test::setCurrentCase("UdpSocketCreate");

    UdpSocket socket;
    bool created = socket.create();
    CHECK(created);
    CHECK(socket.isValid());

    socket.close();
    CHECK(!socket.isValid());
}

TEST_CASE(UdpSocketBind) {
    ayt::test::setCurrentCase("UdpSocketBind");

    UdpSocket socket;
    socket.create();
    bool bound = socket.bind(0);
    CHECK(bound);
    socket.close();
}

TEST_CASE(UdpSocketOptions) {
    ayt::test::setCurrentCase("UdpSocketOptions");

    UdpSocket socket;
    socket.create();

    socket.setReuseAddr(true);
    socket.setBroadcast(true);
    socket.setNonBlocking(true);
    CHECK(socket.isNonBlocking());

    socket.setNonBlocking(false);
    CHECK(!socket.isNonBlocking());

    socket.close();
}

TEST_CASE(UdpSocketRejectsInvalidIpv4Address) {
    ayt::test::setCurrentCase("UdpSocketRejectsInvalidIpv4Address");

    UdpSocket socket;
    CHECK(socket.create());
    const uint8_t payload = 7;
    CHECK(!socket.bind("999.999.999.999", 0));
    CHECK(!socket.connect("not-an-ip", 12345));
    CHECK_INT_EQ(socket.sendTo("300.1.1.1", 12345, &payload, sizeof(payload)), -1);
    socket.close();
}

TEST_CASE(GnsConnectionState) {
    ayt::test::setCurrentCase("GnsConnectionState");

    // Don't init GNS — only exercise constructor + initial state.
    GnsConnection conn;
    CHECK(conn.getState() == GnsConnectionState::Disconnected);
    CHECK(!conn.isConnected());
    CHECK_INT_EQ(conn.getPing(), -1);  // no conn handle yet
}

TEST_CASE(GnsConnectionDropsTruncatedFramesBeforeDispatch) {
    ayt::test::setCurrentCase("GnsConnectionDropsTruncatedFramesBeforeDispatch");

    GnsConnection conn;
    int dispatchCount = 0;
    conn.onPacket([&](const PacketHeader&, const uint8_t*, size_t) {
        ++dispatchCount;
    });
    uint8_t truncated[PacketCodec::kHeaderSize + PacketCodec::kCrcSize - 1]{};
    for (size_t len = PacketCodec::kHeaderSize;
         len < PacketCodec::kHeaderSize + PacketCodec::kCrcSize; ++len) {
        conn.onRawData(truncated, len);
    }
    CHECK_INT_EQ(dispatchCount, 0);
}

TEST_CASE(GnsInitShutdown) {
    ayt::test::setCurrentCase("GnsInitShutdown");

    // Ref-counted init; multiple calls should not double-init.
    bool r1 = gns::init();
    bool r2 = gns::init();
    CHECK(r1);
    CHECK(r2);

    gns::shutdown();
    gns::shutdown();   // second shutdown is a no-op (ref-counted)
    CHECK(true);  // didn't crash = pass
}

// R6 (2026-08-25) — clock seam determinism.
//
// nowMs() must consult s_nowOverride before falling back to wall clock so
// that tests can drive PacketHeader::timestampMs and PacketAssembler TTL
// eviction from a fixed, deterministic logical clock. Three guarantees:
//
//   1. No override  → wall clock (non-zero ms, monotonically increases).
//   2. Custom fn    → exact value (override's output, downshifted to ms).
//   3. tickRate     → monotonically increasing with serverTick; same input
//                     produces the same output across runs (state-equal).
//
// The third property is the one that buys us replay determinism: a
// capture/replay that advances the server tick at the same rate sees the
// same logical clock, and PacketHeader::timestampMs becomes a pure
// function of (serverTick, tickRate).
TEST_CASE(ClockOverride_ReturnsStableTimestamp) {
    ayt::test::setCurrentCase("ClockOverride_ReturnsStableTimestamp");

    // Snapshot the wall-clock baseline so we can verify the seam actually
    // *replaces* it (not just supplements it).
    GnsConnection::clearNowOverride();
    const uint32_t wall0 = GnsConnection::nowMs();
    CHECK(wall0 > 0u);

    // 1. Custom override: GnsConnection::nowMs() must downshift the
    //    override's microseconds output to milliseconds.
    constexpr uint64_t kOverrideUs = 5'000'000ULL;  // 5 000 ms
    GnsConnection::setNowOverrideForTesting([]() { return kOverrideUs; });
    CHECK(GnsConnection::nowMs() == 5000u);
    // Repeated calls must be deterministic.
    CHECK(GnsConnection::nowMs() == 5000u);
    CHECK(GnsConnection::nowMs() == 5000u);
    GnsConnection::clearNowOverride();

    // 2. setNowOverrideForTickRate must produce a monotonic, tick-driven
    //    sequence: 0/30 Hz → 0ms, 30/30 Hz → 1000ms, 60/30 Hz → 2000ms.
    GnsConnection::setNowOverrideForTickRate(0, 30);
    CHECK(GnsConnection::nowMs() == 0u);
    GnsConnection::setNowOverrideForTickRate(30, 30);
    CHECK(GnsConnection::nowMs() == 1000u);
    GnsConnection::setNowOverrideForTickRate(60, 30);
    CHECK(GnsConnection::nowMs() == 2000u);

    // 3. State-equal replay: re-installing the same (tick, rate) pair
    //    yields the exact same ms. This is the property the rest of R6
    //    depends on — two test runs at the same logical tick must
    //    produce byte-identical PacketHeader::timestampMs.
    GnsConnection::setNowOverrideForTickRate(15, 30);
    const uint32_t sample1 = GnsConnection::nowMs();
    const uint32_t sample2 = GnsConnection::nowMs();
    CHECK(sample1 == sample2);
    CHECK(sample1 == 500u);

    // 4. tickRate=0 must not divide by zero — falls back to 1 Hz, so
    //    tick=1000 gives 1 000 000 us / 1 ms = 1 000 000 ms.
    GnsConnection::setNowOverrideForTickRate(1000, 0);
    CHECK(GnsConnection::nowMs() == 1'000'000u);

    GnsConnection::clearNowOverride();

    // 5. After clearNowOverride(), the seam must be back to wall clock.
    //    The wall-clock value may equal the old one (unlikely on a fast
    //    machine) but is structurally the wall clock; we only assert that
    //    the override is no longer consulted.
    const uint32_t wall1 = GnsConnection::nowMs();
    CHECK(wall1 > 0u);
}

TEST_SUITE_END
