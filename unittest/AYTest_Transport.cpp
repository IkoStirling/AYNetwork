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

TEST_SUITE_END
