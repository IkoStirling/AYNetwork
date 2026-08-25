// AYTest_AckPipeline.cpp - R4.1-B CHANNEL_ACK explicit pipeline tests

#include <AYNetwork.h>
#include <AYTest.h>

#include <AYNetwork/Protocol/AckPipeline.h>
#include <AYNetwork/Protocol/PacketCodec.h>
#include <AYNetwork/Protocol/PacketHeader.h>
#include <AYNetwork/Transport/GnsConnection.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <functional>
#include <string>
#include <thread>
#include <vector>

using namespace ayt::net;

namespace
{

bool pumpUntil(GnsConnection& a, GnsConnection& b,
               std::chrono::milliseconds timeout,
               const std::function<bool()>& pred,
               const char* what) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        a.update();
        b.update();
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    (void)what;
    return pred();
}

} // anonymous namespace

TEST_SUITE(AckPipeline)

TEST_CASE(SealAckableUnwrapRoundTrip) {
    ayt::test::setCurrentCase("SealAckableUnwrapRoundTrip");
    const char* payload = "ack-me-plz";
    const uint32_t seq = 0xAABBCCDDu;
    auto wire = AckPipeline::sealAckable(
        reinterpret_cast<const uint8_t*>(payload), std::strlen(payload),
        kMsgTypeApp, CHANNEL_UNRELIABLE, seq, /*timestampMs=*/ 1234);

    auto decoded = PacketCodec::decode(wire.data(), wire.size());
    CHECK(decoded.ok);
    CHECK(hasFlag(decoded.header.flags, PacketFlag::RequiresAck));

    uint32_t seqOut = 0;
    CHECK(AckPipeline::unwrapAckableBody(decoded.body, seqOut));
    CHECK_INT_EQ(static_cast<uint32_t>(seqOut), seq);
    CHECK_INT_EQ(static_cast<size_t>(decoded.body.size()), std::strlen(payload));
    CHECK(std::memcmp(decoded.body.data(), payload, decoded.body.size()) == 0);
}

TEST_CASE(SealAckUsesChannelAck) {
    ayt::test::setCurrentCase("SealAckUsesChannelAck");
    const uint32_t seq = 42;
    auto wire = AckPipeline::sealAck(seq, /*timestampMs=*/ 99);
    auto decoded = PacketCodec::decode(wire.data(), wire.size());
    CHECK(decoded.ok);
    CHECK_INT_EQ(static_cast<int>(decoded.header.msgType),
                 static_cast<int>(kMsgTypeAppAck));
    CHECK_INT_EQ(static_cast<int>(decoded.header.channel),
                 static_cast<int>(CHANNEL_ACK));

    uint32_t seqOut = 0;
    CHECK(AckPipeline::parseAckBody(decoded.body.data(), decoded.body.size(), seqOut));
    CHECK_INT_EQ(static_cast<uint32_t>(seqOut), static_cast<uint32_t>(seq));
}

TEST_CASE(AckTrackerFiresOnAck) {
    ayt::test::setCurrentCase("AckTrackerFiresOnAck");
    AckTracker tracker;
    std::atomic<bool> confirmed{false};
    tracker.registerPending(7, [&](bool ok) {
        if (ok) confirmed.store(true);
    });
    CHECK_INT_EQ(static_cast<size_t>(tracker.pendingCount()), static_cast<size_t>(1));
    tracker.onAck(7);
    CHECK(confirmed.load());
    CHECK_INT_EQ(static_cast<size_t>(tracker.pendingCount()), static_cast<size_t>(0));
}

TEST_CASE(AckTrackerExpiresPending) {
    ayt::test::setCurrentCase("AckTrackerExpiresPending");
    AckTracker tracker;
    std::atomic<bool> failed{false};
    tracker.registerPending(9, [&](bool ok) {
        if (!ok) failed.store(true);
    }, /*timeoutMs=*/ 1);
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    tracker.expire();
    CHECK(failed.load());
    CHECK_INT_EQ(static_cast<size_t>(tracker.pendingCount()), static_cast<size_t>(0));
}

// R6 C3 (2026-08-25): AckTracker::_pending switched from std::unordered_map
// to std::map (B-07). expire() iterates pending acks in ascending seq
// order — the B-07 determinism finding. Three expired callbacks registered
// out of order; we assert the firing order is strictly ascending seq so
// two runs see identical expire() side-effect order.
TEST_CASE(AckTrackerExpireFiresInAscendingSeqOrder) {
    ayt::test::setCurrentCase("AckTrackerExpireFiresInAscendingSeqOrder");
    AckTracker tracker;
    std::vector<uint32_t> firedSeqs;
    // Register in deliberately non-monotonic order: 30, 10, 20.
    tracker.registerPending(30, [&](bool ok) { if (!ok) firedSeqs.push_back(30); }, /*timeoutMs=*/ 1);
    tracker.registerPending(10, [&](bool ok) { if (!ok) firedSeqs.push_back(10); }, /*timeoutMs=*/ 1);
    tracker.registerPending(20, [&](bool ok) { if (!ok) firedSeqs.push_back(20); }, /*timeoutMs=*/ 1);
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    tracker.expire();
    CHECK_INT_EQ(static_cast<size_t>(firedSeqs.size()), static_cast<size_t>(3));
    CHECK_INT_EQ(static_cast<uint32_t>(firedSeqs[0]), static_cast<uint32_t>(10));
    CHECK_INT_EQ(static_cast<uint32_t>(firedSeqs[1]), static_cast<uint32_t>(20));
    CHECK_INT_EQ(static_cast<uint32_t>(firedSeqs[2]), static_cast<uint32_t>(30));
    CHECK_INT_EQ(static_cast<size_t>(tracker.pendingCount()), static_cast<size_t>(0));
}

TEST_CASE(RequireAckRoundTripOverGns) {
    ayt::test::setCurrentCase("RequireAckRoundTripOverGns");
    CHECK(gns::init());

    constexpr uint16_t kVirtualPort = 7788;
    const char* kPayload = "require-ack-payload";

    GnsConnection server;
    GnsConnection client;
    std::atomic<bool> serverGotPayload{false};
    std::atomic<bool> clientConfirmed{false};

    server.onData([&](const uint8_t* data, size_t len) {
        if (len == std::strlen(kPayload) &&
            std::memcmp(data, kPayload, len) == 0) {
            serverGotPayload.store(true);
        }
    });

    server.initServer(kVirtualPort);
    client.initClient("127.0.0.1", kVirtualPort);

    CHECK(pumpUntil(server, client, std::chrono::seconds(3),
        [&]() {
            return client.getState() == GnsConnectionState::Connected &&
                   server.getState() == GnsConnectionState::Connected;
        },
        "connect"));

    CHECK(client.sendRequireAck(
        kMsgTypeApp, CHANNEL_UNRELIABLE,
        kPayload, std::strlen(kPayload),
        [&](bool ok) {
            if (ok) clientConfirmed.store(true);
        }) == 0);

    CHECK(pumpUntil(server, client, std::chrono::seconds(3),
        [&]() { return serverGotPayload.load() && clientConfirmed.load(); },
        "ack round-trip"));

    CHECK_INT_EQ(static_cast<size_t>(client.pendingAckCountForTesting()), static_cast<size_t>(0));

    gns::shutdown();
}

TEST_SUITE_END
