// NetworkTime.cpp - R5.0 implementation. Pure math, no IO.

#include <AYNetwork/Snapshot/NetworkTime.h>

#include <algorithm>

namespace ayt::net
{

NetworkTime::NetworkTime() = default;

void NetworkTime::setTickRate(double hz) {
    _tickRate = (hz > 0.0) ? hz : 30.0;
}

void NetworkTime::advance(double dtSec) {
    if (dtSec <= 0.0 || _tickRate <= 0.0) return;

    // R6 (2026-08-25): fixed-point accumulator (B-01, M-17).
    //
    // Convert dtSec → microseconds at function entry so the entire
    // advance() path runs in uint64. dtSec is the only floating-point
    // input (caller passes frame delta). The conversion rounds-half-up
    // at the microsecond boundary, which is below the FPU's rounding
    // error for typical frame deltas (≈16.67 ms ≈ 16670 us).
    //
    // 1 tick = (1'000'000 / tickRate) microseconds. Sub-tick remainder
    // accumulates as `_accumulatorUs`. Whole ticks are extracted by
    // integer divide and subtracted; carry-over < 1 tick persists.
    //
    // Cap catch-up at 64 ticks per call so a long stall (e.g.
    // breakpoint) doesn't fire 1000 snapshots when execution resumes.
    const uint64_t kMaxAccumulatorUs = 64ULL * 1000000ULL; // 64 ticks
    if (_accumulatorUs >= kMaxAccumulatorUs) {
        // Already at the cap — drop this dt on the floor.
        return;
    }
    const uint64_t dtUs = static_cast<uint64_t>(dtSec * 1000000.0 + 0.5);
    if (dtUs == 0) return;

    uint64_t newAccum = _accumulatorUs + dtUs;
    if (newAccum > kMaxAccumulatorUs) newAccum = kMaxAccumulatorUs;

    // 1 tick = 1'000'000 / tickRate microseconds. Compute tick
    // duration in us (rounded half-up so 30 Hz gives 33333us not
    // 33334us). tickRate is double; convert once.
    const double tickRate = _tickRate;
    const uint64_t tickUs = static_cast<uint64_t>((1000000.0 / tickRate) + 0.5);
    if (tickUs == 0) {
        // Degenerate (tickRate > 1e6 Hz) — advance the counter by
        // microseconds directly so we still make progress.
        _serverTick += static_cast<uint32_t>(newAccum);
        _accumulatorUs = 0;
        return;
    }
    const uint32_t wholeTicks = static_cast<uint32_t>(newAccum / tickUs);
    _accumulatorUs = newAccum - static_cast<uint64_t>(wholeTicks) * tickUs;
    _serverTick += wholeTicks;
}

void NetworkTime::advanceClient(double dtSec) {
    if (dtSec <= 0.0) return;
    _clientTimeSec += dtSec;
}

double NetworkTime::tickToSeconds(uint32_t tick) const {
    return (_tickRate > 0.0) ? (static_cast<double>(tick) / _tickRate) : 0.0;
}

void NetworkTime::reset() {
    _serverTick = 0;
    _accumulatorUs = 0;
    _clientTimeSec = 0.0;
}

} // namespace ayt::net