// TransportTest.cpp - Transport layer unit tests

#include <UdpSocket.h>
#include <AYConnection.h>
#include <KcpConnection.h>
#include <cstdio>

namespace ayt::net
{
namespace test
{

static int s_passed = 0;
static int s_failed = 0;

#define CHECK(cond) \
    do { \
        if (cond) { \
            s_passed++; \
            printf("[PASS] %s\n", #cond); \
        } else { \
            s_failed++; \
            printf("[FAIL] %s\n", #cond); \
        } \
    } while (false)

#define CHECK_EQUAL(a, b) \
    do { \
        if ((a) == (b)) { \
            s_passed++; \
            printf("[PASS] %s == %s (%d == %d)\n", #a, #b, (int)(a), (int)(b)); \
        } else { \
            s_failed++; \
            printf("[FAIL] %s == %s (%d != %d)\n", #a, #b, (int)(a), (int)(b)); \
        } \
    } while (false)

void testUdpSocketCreate() {
    printf("\n=== UDP Socket Create Tests ===\n");

    UdpSocket socket;
    bool created = socket.create();
    CHECK(created);
    CHECK(socket.isValid());

    socket.close();
    CHECK(!socket.isValid());
}

void testUdpSocketBind() {
    printf("\n=== UDP Socket Bind Tests ===\n");

    UdpSocket socket;
    socket.create();

    bool bound = socket.bind(0);
    CHECK(bound);

    socket.close();
}

void testUdpSocketOptions() {
    printf("\n=== UDP Socket Options Tests ===\n");

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

void testKcpConfig() {
    printf("\n=== KCP Config Tests ===\n");

    KcpConfig config;
    CHECK_EQUAL(config.conv, 0);
    CHECK_EQUAL(config.mtu, 1400);
    CHECK_EQUAL(config.wndSize, 64);
    CHECK_EQUAL(config.noDelay, 1);
    CHECK_EQUAL(config.interval, 10);
}

void testKcpConnection() {
    printf("\n=== KCP Connection Tests ===\n");

    KcpConnection conn;
    CHECK(!conn.isConnected());

    KcpConfig config;
    config.conv = 12345;
    conn.init(config);

    CHECK(!conn.isConnected());
}

void testConnectionState() {
    printf("\n=== Connection State Tests ===\n");

    AYConnection conn;
    CHECK(conn.getState() == ConnectionState::Disconnected);
    CHECK(!conn.isConnected());
}

int runTransportTests() {
    printf("\n========== Transport Layer Unit Tests ==========\n");

    testUdpSocketCreate();
    testUdpSocketBind();
    testUdpSocketOptions();
    testKcpConfig();
    testKcpConnection();
    testConnectionState();

    printf("\n========== Results: %d passed, %d failed ==========\n\n", s_passed, s_failed);
    return s_failed;
}

} // namespace test
} // namespace ayt::net

int main() {
    return ayt::net::test::runTransportTests();
}