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
    _accumulator += dtSec * _tickRate;
    // Floor of accumulator is whole ticks. Cap the catch-up at 64 ticks
    // per call so a long stall (e.g. breakpoint) doesn't fire 1000 snapshots
    // when execution resumes.
    uint32_t wholeTicks = static_cast<uint32_t>(_accumulator);
    if (wholeTicks > 64u) wholeTicks = 64u;
    _accumulator -= static_cast<double>(wholeTicks);
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
    _accumulator = 0.0;
    _clientTimeSec = 0.0;
}

} // namespace ayt::net