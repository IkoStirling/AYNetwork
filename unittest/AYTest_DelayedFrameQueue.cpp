// AYNetwork/unittest/AYTest_DelayedFrameQueue.cpp - R5.4 (2026-08-25)
// DelayedFrameQueue per-channel deque tests. 3 cases: enqueue + release at
// deadline, multi-frame FIFO order, drop-on-cap behavior.

#include <AYTest.h>
#include "DelayedFrameQueue.h"

#include <vector>

using namespace ayt::net;

namespace
{
constexpr const char* kCase1 = "DelayedQueue_ReleaseAtDeadline";
constexpr const char* kCase2 = "DelayedQueue_FifoOrder";
constexpr const char* kCase3 = "DelayedQueue_DropOnCap";
} // anonymous namespace

TEST_SUITE(DelayedFrameQueue)

TEST_CASE(DelayedQueue_ReleaseAtDeadline)
{
    ayt::test::setCurrentCase(kCase1);
    DelayedFrameQueue q;

    // Empty queue.
    CHECK(q.size() == 0u);
    CHECK(q.releaseReady(/*nowMs=*/0).empty());

    DelayedFrame a;
    a.releaseAtMs = 100;
    a.bytes = {0x01, 0x02};
    a.channel = 0;
    q.enqueue(std::move(a));

    DelayedFrame b;
    b.releaseAtMs = 200;
    b.bytes = {0x03};
    b.channel = 0;
    q.enqueue(std::move(b));

    CHECK(q.size() == 2u);

    // nowMs=99 → nothing ready yet.
    CHECK(q.releaseReady(99).empty());
    CHECK(q.size() == 2u);

    // nowMs=100 → first frame ready, second still held.
    auto ready = q.releaseReady(100);
    CHECK(ready.size() == 1u);
    CHECK(ready[0].releaseAtMs == 100u);
    CHECK(ready[0].bytes.size() == 2u);
    CHECK(ready[0].bytes[0] == 0x01);
    CHECK(q.size() == 1u);

    // nowMs=199 → still holding second.
    CHECK(q.releaseReady(199).empty());

    // nowMs=200 → second frame released.
    ready = q.releaseReady(200);
    CHECK(ready.size() == 1u);
    CHECK(ready[0].releaseAtMs == 200u);
    CHECK(q.size() == 0u);
}

TEST_CASE(DelayedQueue_FifoOrder)
{
    ayt::test::setCurrentCase(kCase2);
    DelayedFrameQueue q;

    // Enqueue three frames with monotonically increasing deadlines.
    for (uint64_t i = 0; i < 3; ++i) {
        DelayedFrame f;
        f.releaseAtMs = (i + 1) * 10; // 10, 20, 30
        f.bytes = {static_cast<uint8_t>(i)};
        f.channel = 1;
        q.enqueue(std::move(f));
    }
    CHECK(q.size() == 3u);

    // Drain all in one shot.
    auto ready = q.releaseReady(/*nowMs=*/100);
    CHECK(ready.size() == 3u);
    CHECK(ready[0].bytes[0] == 0u); // first in, first out
    CHECK(ready[1].bytes[0] == 1u);
    CHECK(ready[2].bytes[0] == 2u);
    CHECK(q.size() == 0u);

    // Subsequent calls return empty.
    CHECK(q.releaseReady(1000).empty());
}

TEST_CASE(DelayedQueue_DropOnCap)
{
    ayt::test::setCurrentCase(kCase3);
    DelayedFrameQueue q;
    constexpr size_t kCap = 4;

    // Fill to capacity.
    for (size_t i = 0; i < kCap; ++i) {
        DelayedFrame f;
        f.releaseAtMs = static_cast<uint64_t>((i + 1) * 10);
        f.bytes = {static_cast<uint8_t>(i)};
        f.channel = 2;
        q.enqueue(std::move(f));
    }
    CHECK(q.size() == kCap);
    CHECK(q.consumeDroppedCount() == 0u);

    // One more should drop oldest when we enforce the cap.
    DelayedFrame extra;
    extra.releaseAtMs = 50;
    extra.bytes = {0xFF};
    extra.channel = 2;
    q.enqueue(std::move(extra));
    CHECK(q.size() == kCap + 1u); // not auto-capped yet

    // enforceCap brings it back to kCap and increments dropped count.
    uint64_t dropped = 0;
    q.enforceCap(kCap, &dropped);
    CHECK(dropped == 1u);
    CHECK(q.size() == kCap);
    CHECK(q.consumeDroppedCount() == 1u);

    // Drain all → oldest (releaseAtMs=10) was dropped; remaining are 20, 30, 40, 50.
    auto ready = q.releaseReady(1000);
    CHECK(ready.size() == kCap);
    CHECK(ready[0].releaseAtMs == 20u);
    CHECK(ready[1].releaseAtMs == 30u);
    CHECK(ready[2].releaseAtMs == 40u);
    CHECK(ready[3].releaseAtMs == 50u);

    // Consume resets the counter.
    CHECK(q.consumeDroppedCount() == 0u);
}

TEST_SUITE_END
