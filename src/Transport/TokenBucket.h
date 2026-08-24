// AYNetwork/Transport/TokenBucket.h - R5.4 (2026-08-25) header-only
// token-bucket rate limiter primitive used by TransportFaultInterceptor.
//
// Behavior:
//   - Rate `R` bytes/sec, burst capacity `B` bytes.
//   - Tokens refill linearly in time: tokens += dt * R, clamped to B.
//   - `tryConsume(bytes, dt)` refills then deducts; returns false if
//     insufficient tokens remain (caller re-enqueues).
//   - Zero rate = bucket disabled (tryConsume always returns true).
//   - Tokens are stored as `double` for sub-byte precision under long
//     cumulative runs; the per-tick refill accumulates rounding error
//     slower than float.
//
// Determinism: no internal RNG. Behavior is fully determined by the
// (rate, burst, dt, frame-size) sequence the caller feeds.

#pragma once

#include <algorithm>
#include <cstdint>

namespace ayt::net
{

class TokenBucket
{
public:
    TokenBucket() = default;

    void configure(uint32_t bytesPerSec, uint32_t burstBytes) {
        _rate  = static_cast<double>(bytesPerSec);
        _burst = static_cast<double>(burstBytes);
        // Clamp tokens into the new burst band so reconfiguring smaller
        // doesn't keep stale surplus around.
        if (_tokens > _burst) _tokens = _burst;
    }

    // Refill and try to consume. dtSeconds is the wall-clock elapsed
    // since the previous tryConsume call. Returns true if `bytes` were
    // deducted (and so the frame may proceed); false if the bucket has
    // insufficient tokens (caller should re-enqueue).
    bool tryConsume(uint32_t bytes, double dtSeconds) {
        if (_rate <= 0.0) {
            // Bucket disabled — always allow.
            return true;
        }
        if (dtSeconds > 0.0) {
            _tokens += dtSeconds * _rate;
            if (_tokens > _burst) _tokens = _burst;
        }
        const double want = static_cast<double>(bytes);
        if (_tokens >= want) {
            _tokens -= want;
            return true;
        }
        return false;
    }

    // Tokens available right now (after the most recent refill).
    // Used by tests for assertions. Returns `_burst` when disabled.
    double availableTokens() const {
        return (_rate <= 0.0) ? _burst : _tokens;
    }

    uint32_t rateBytesPerSec() const {
        return (_rate <= 0.0) ? 0u : static_cast<uint32_t>(_rate);
    }
    uint32_t burstBytes() const {
        return (_burst <= 0.0) ? 0u : static_cast<uint32_t>(_burst);
    }

private:
    double _rate   = 0.0;
    double _burst  = 0.0;
    double _tokens = 0.0;
};

} // namespace ayt::net
