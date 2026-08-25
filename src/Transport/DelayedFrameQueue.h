// AYNetwork/Transport/DelayedFrameQueue.h - R5.4 (2026-08-25)
// per-channel delay queue. Holds sealed frames until their release
// deadline. The reorder-swap logic lives in `TransportFaultInterceptor`,
// not here — the queue is a pure delay primitive.

#pragma once

#include <cstdint>
#include <deque>
#include <algorithm>
#include <vector>

namespace ayt::net
{

// One frame waiting in the delay pipeline. `bytes` is a sealed frame
// (send-side: post PacketCodec::encode; recv-side: raw GNS payload,
// pre PacketCodec::decode). `channel` records which channel the frame
// belongs to. `isDuplicate` is informational — set true when the
// frame is the second copy of a dup-percent hit, used by tests.
struct DelayedFrame {
    uint64_t              releaseAtMs = 0;
    std::vector<uint8_t>  bytes;
    uint8_t               channel     = 0;
    bool                  isDuplicate = false;
};

class DelayedFrameQueue
{
public:
    DelayedFrameQueue() = default;

    // Append a frame with an explicit deadline. Always succeeds. Caller
    // computes the deadline via `TransportFaultInterceptor` (mean ± jitter).
    void enqueue(DelayedFrame&& frame) {
        _q.push_back(std::move(frame));
    }

    // Drain and return all frames whose deadline has passed. Deadlines are
    // not guaranteed to be monotonic when jitter/reorder is active, so scan
    // the full queue instead of allowing an unready front item to block a
    // ready item behind it. Relative queue order is preserved.
    std::vector<DelayedFrame> releaseReady(uint64_t nowMs) {
        std::vector<DelayedFrame> out;
        for (auto it = _q.begin(); it != _q.end();) {
            if (it->releaseAtMs <= nowMs) {
                out.push_back(std::move(*it));
                it = _q.erase(it);
            } else {
                ++it;
            }
        }
        return out;
    }

    size_t size() const { return _q.size(); }
    bool   empty() const { return _q.empty(); }

    // Swap the two newest entries. Used by the fault interceptor to model
    // one adjacent reorder without retaining an out-of-queue predecessor.
    bool swapLastTwo() {
        if (_q.size() < 2) return false;
        std::iter_swap(_q.end() - 1, _q.end() - 2);
        return true;
    }

    // True if any frame in the queue has a deadline <= nowMs.
    bool hasReady(uint64_t nowMs) const {
        for (const auto& frame : _q) {
            if (frame.releaseAtMs <= nowMs) return true;
        }
        return false;
    }

    // Cap queue size. When exceeded, the OLDEST entries are dropped and
    // `droppedCount` is incremented (caller can read via
    // `consumeDroppedCount` for logging). Returns the number of frames
    // dropped in this call.
    size_t enforceCap(size_t cap, uint64_t* droppedOut = nullptr) {
        if (_q.size() <= cap) return 0;
        const size_t excess = _q.size() - cap;
        for (size_t i = 0; i < excess; ++i) _q.pop_front();
        _dropped += excess;
        if (droppedOut) *droppedOut = excess;
        return excess;
    }

    uint64_t consumeDroppedCount() {
        const uint64_t v = _dropped;
        _dropped = 0;
        return v;
    }

    void clear() { _q.clear(); }

private:
    std::deque<DelayedFrame> _q;
    uint64_t                 _dropped = 0;
};

} // namespace ayt::net
