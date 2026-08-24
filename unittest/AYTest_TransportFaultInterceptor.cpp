// AYNetwork/unittest/AYTest_TransportFaultInterceptor.cpp - R5.4 (2026-08-25)
// TransportFaultInterceptor end-to-end tests. 5 cases: loss, dup, reorder,
// latency+jitter, rate-limit, channel-mask gating.

#include <AYTest.h>
#include <AYNetwork/TransportFaultProfile.h>
#include "TransportFaultController.h"
#include "TransportFaultInterceptor.h"

#include <vector>

using namespace ayt::net;

namespace
{
constexpr const char* kCase1 = "Interceptor_NoProfilePassesThrough";
constexpr const char* kCase2 = "Interceptor_LossPercent";
constexpr const char* kCase3 = "Interceptor_LatencyAndJitter";
constexpr const char* kCase4 = "Interceptor_RateLimit";
constexpr const char* kCase5 = "Interceptor_ChannelMaskGating";

std::vector<uint8_t> bytes(char c) { return {static_cast<uint8_t>(c)}; }

// Helper: drain the interceptor with one tick at `nowMs`. Returns the released
// send-side frames; recv-side is dropped because we only feed via onSend.
struct DrainResult {
    std::vector<std::pair<std::vector<uint8_t>, uint8_t>> sendOut;
    std::vector<std::pair<std::vector<uint8_t>, uint8_t>> recvOut;
    size_t total;
};

DrainResult drainOne(TransportFaultInterceptor& i, uint64_t nowMs)
{
    DrainResult r;
    r.total = i.tick(nowMs, /*dt=*/0.001, r.sendOut, r.recvOut);
    return r;
}
} // anonymous namespace

TEST_SUITE(TransportFaultInterceptor)

TEST_CASE(Interceptor_NoProfilePassesThrough)
{
    ayt::test::setCurrentCase(kCase1);
    TransportFaultController ctl;
    TransportFaultInterceptor i(/*netId=*/1, ctl);

    // No profile installed → isEnabled() = false. Callers skip us entirely.
    CHECK(!i.isEnabled());

    // Even if we DO get called (defensive probe), the no-op path enqueues
    // the frame with releaseAtMs = nowMs, so the next tick releases it.
    i.onSend(/*nowMs=*/0, bytes('a').data(), 1, /*channel=*/0);
    auto d = drainOne(i, 0);
    CHECK(d.sendOut.size() == 1u);
    CHECK(d.sendOut[0].second == 0u);
    CHECK(d.sendOut[0].first.size() == 1u);
    CHECK(d.sendOut[0].first[0] == 'a');
    CHECK(d.total == 1u);
}

TEST_CASE(Interceptor_LossPercent)
{
    ayt::test::setCurrentCase(kCase2);
    TransportFaultController ctl;
    TransportFaultProfile p;
    p.randomSeed = 0xABCDEFULL;
    p.channelMask = kFaultChannelAll;
    p.lossPercent = 100.0f; // drop everything
    ctl.setProfile(/*netId=*/7, p);

    TransportFaultInterceptor i(7, ctl);
    CHECK(i.isEnabled());

    constexpr int kFrames = 50;
    for (int n = 0; n < kFrames; ++n) {
        i.onSend(/*nowMs=*/0, bytes('x').data(), 1, /*channel=*/1);
    }
    // 100% loss: nothing enqueued, nothing released. sendDroppedCount
    // counts queue-cap drops, NOT loss hits (loss never reaches the
    // queue), so we verify via queue size / drained output instead.
    CHECK(i.sendQueueSize(1) == 0u);
    auto d = drainOne(i, /*nowMs=*/1000);
    CHECK(d.sendOut.empty());

    // 0% loss = all frames survive. With latencyMeanMs=0 they release
    // on the next tick.
    ctl.clearProfile(7);
    TransportFaultProfile p2;
    p2.randomSeed = 0xABCDEFULL;
    p2.channelMask = kFaultChannelAll;
    p2.lossPercent = 0.0f;
    p2.latencyMeanMs = 0; // no delay
    ctl.setProfile(7, p2);

    TransportFaultInterceptor i2(7, ctl);
    for (int n = 0; n < kFrames; ++n) {
        i2.onSend(0, bytes('x').data(), 1, /*channel=*/1);
    }
    auto d2 = drainOne(i2, 1000);
    CHECK(d2.sendOut.size() == static_cast<size_t>(kFrames));
}

TEST_CASE(Interceptor_LatencyAndJitter)
{
    ayt::test::setCurrentCase(kCase3);
    TransportFaultController ctl;
    TransportFaultProfile p;
    p.randomSeed = 0x1234ULL;
    p.channelMask = kFaultChannelAll;
    p.lossPercent = 0.0f;
    p.dupPercent = 0.0f;
    p.reorderPercent = 0.0f;
    p.latencyMeanMs = 100;
    p.latencyJitterMs = 50;
    ctl.setProfile(/*netId=*/11, p);

    TransportFaultInterceptor i(11, ctl);

    constexpr int kFrames = 100;
    for (int n = 0; n < kFrames; ++n) {
        i.onSend(/*nowMs=*/0, bytes('x').data(), 1, /*channel=*/2);
    }
    CHECK(i.sendQueueSize(2) == static_cast<size_t>(kFrames));

    // At nowMs=49 nothing released (minimum delay is 50 ms with mean=100, jitter=50).
    auto d = drainOne(i, 49);
    CHECK(d.sendOut.empty());

    // At nowMs=150 every frame must be released (max delay is 150 ms).
    auto d2 = drainOne(i, 150);
    CHECK(d2.sendOut.size() == static_cast<size_t>(kFrames));
    CHECK(i.sendQueueSize(2) == 0u);
}

TEST_CASE(Interceptor_RateLimit)
{
    ayt::test::setCurrentCase(kCase4);
    TransportFaultController ctl;
    TransportFaultProfile p;
    p.randomSeed = 0x5555ULL;
    p.channelMask = kFaultChannelAll;
    p.latencyMeanMs = 0;
    p.lossPercent = 0.0f;
    p.rateLimitBytesPerSec = 1000;
    p.rateLimitBurstBytes  = 2000;
    ctl.setProfile(/*netId=*/13, p);

    TransportFaultInterceptor i(13, ctl);

    // Enqueue 10 frames of 500 bytes each. With latencyMeanMs=0 they all
    // sit in the queue at releaseAtMs=0 and are due on the next tick.
    for (int n = 0; n < 10; ++n) {
        i.onSend(0, bytes('x').data(), /*len=*/500, /*channel=*/3);
    }
    CHECK(i.sendQueueSize(3) == 10u);

    // First tick: dt=0.0 — no per-frame refill. Bucket starts at 0, so
    // NO frame can drain. The bucket also rejects them and re-enqueues
    // at nowMs+1ms. Queue should be ~10 still.
    std::vector<std::pair<std::vector<uint8_t>, uint8_t>> sendOut, recvOut;
    size_t n1 = i.tick(/*nowMs=*/0, /*dt=*/0.0, sendOut, recvOut);
    CHECK(n1 == 0u);
    CHECK(sendOut.empty());
    CHECK(i.sendQueueSize(3) == 10u);

    // Now grant dt=10.0 (10 seconds at 1000 bytes/sec = 10000 tokens,
    // capped at burst=2000). With dt passed to each per-frame tryConsume,
    // each frame can consume 500. With 2000 tokens available at every
    // tryConsume call, all 10 frames release in this tick.
    sendOut.clear(); recvOut.clear();
    size_t n2 = i.tick(/*nowMs=*/10, /*dt=*/10.0, sendOut, recvOut);
    CHECK(n2 == 10u);
    CHECK(sendOut.size() == 10u);
    CHECK(i.sendQueueSize(3) == 0u);

    // Verify rate-limit is OFF for a profile without knobs (no rate limit).
    TransportFaultProfile p2;
    p2.randomSeed = 0x5555ULL;
    p2.channelMask = kFaultChannelAll;
    p2.latencyMeanMs = 0;
    p2.lossPercent = 0.0f;
    ctl.setProfile(13, p2);
    TransportFaultInterceptor i2(13, ctl);
    for (int n = 0; n < 5; ++n) {
        i2.onSend(0, bytes('x').data(), /*len=*/500, /*channel=*/3);
    }
    sendOut.clear(); recvOut.clear();
    size_t n3 = i2.tick(/*nowMs=*/0, /*dt=*/0.0, sendOut, recvOut);
    CHECK(n3 == 5u);
    CHECK(sendOut.size() == 5u);
}

TEST_CASE(Interceptor_ChannelMaskGating)
{
    ayt::test::setCurrentCase(kCase5);
    TransportFaultController ctl;
    TransportFaultProfile p;
    p.randomSeed = 0x9999ULL;
    p.channelMask = kFaultChannelReliable; // only channel 0 affected
    p.latencyMeanMs = 5000;                // long enough that no frame can drain
    ctl.setProfile(/*netId=*/99, p);

    TransportFaultInterceptor i(99, ctl);

    // Send on channel 0 (Reliable) → frame is held (long delay).
    i.onSend(/*nowMs=*/0, bytes('r').data(), 1, /*channel=*/0);
    CHECK(i.sendQueueSize(0) == 1u);

    // Send on channel 1 (Unreliable) → masked-out → enqueued with releaseAtMs=nowMs
    // (i.e. released immediately on the next tick). Queue briefly holds it.
    i.onSend(/*nowMs=*/0, bytes('u').data(), 1, /*channel=*/1);
    // Drain immediately releases it.
    auto d1 = drainOne(i, /*nowMs=*/0);
    CHECK(d1.sendOut.size() == 1u);
    CHECK(d1.sendOut[0].second == 1u); // channel 1
    CHECK(d1.sendOut[0].first[0] == 'u');

    // Channel 0 frame is still queued (long latency).
    CHECK(i.sendQueueSize(0) == 1u);

    // Send on channel 2 (Fragmented) → also masked-out, drained immediately.
    i.onSend(/*nowMs=*/0, bytes('f').data(), 1, /*channel=*/2);
    auto d2 = drainOne(i, 0);
    CHECK(d2.sendOut.size() == 1u);
    CHECK(d2.sendOut[0].second == 2u);

    // Channel 0 still queued; channel 1 + 2 drained.
    CHECK(i.sendQueueSize(0) == 1u);
    CHECK(i.sendQueueSize(1) == 0u);
    CHECK(i.sendQueueSize(2) == 0u);
}

TEST_SUITE_END
