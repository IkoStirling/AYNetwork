#pragma once
// AYNetwork/Snapshot/NetworkTime.h - R5.0 tick-rate / interpolation clock.
//
// Design goal: keep "what time is it for the network layer?" in one tiny
// place. Both the authority and the client hold one of these; the two
// instances never share state (clock drift between processes is a separate
// problem handled by the SnapshotBuffer ring, not by NetworkTime itself).
//
// Three orthogonal clocks live here:
//
//   serverTick      — monotonic counter on the authority. Each replication
//                     frame stamped with the tick at emission time.
//   serverTimeSec   — derived: serverTick / tickRate. Client uses this as
//                     the X-axis of the interpolation buffer.
//   clientTimeSec   — local monotonic clock since session start; the
//                     interpolator reads (clientTimeSec - interpDelay)
//                     every render frame to pick which snapshot pair to
//                     blend between.
//
// The class holds no IO / no sockets; update() is a pure math call. This
// keeps unit tests trivial (drive dt manually, no sleep loops).

#include <cstdint>

namespace ayt::net
{

class NetworkTime {
public:
    NetworkTime();

    // ---- Tick rate ----
    void   setTickRate(double hz);
    double getTickRate() const { return _tickRate; }

    // ---- Server-side API ----
    // Bump the monotonic tick counter by `dtSec` seconds at the configured
    // rate. Carries sub-tick fractional time internally so the count is
    // accurate even when dt drifts.
    void     advance(double dtSec);
    uint32_t getServerTick() const { return _serverTick; }
    // Caller may pre-set a tick (e.g. tests, or resuming from a snapshot
    // log). Does NOT modify the accumulator.
    void     setServerTick(uint32_t t) { _serverTick = t; }

    // ---- Client-side API ----
    // Bump the client local clock only; does not touch serverTick.
    void   advanceClient(double dtSec);
    double getClientTimeSec() const { return _clientTimeSec; }

    // ---- Interpolation delay ----
    void   setInterpolationDelaySec(double d) { _interpDelay = d; }
    double getInterpolationDelaySec() const  { return _interpDelay; }

    // The time the client should render at right now (client clock minus
    // the configured delay). == 0 before any client advance() call.
    double getInterpolationTimeSec() const {
        return _clientTimeSec - _interpDelay;
    }

    // Convenience: convert a tick to its float seconds value.
    double tickToSeconds(uint32_t tick) const;

    // Reset to zero state. Useful when transitioning from lobby to gameplay.
    void reset();

private:
    double   _tickRate    = 30.0;
    double   _interpDelay = 0.1;       // 100 ms — typical Unreal / Unity default
    uint32_t _serverTick  = 0;
    double   _accumulator = 0.0;        // sub-tick fractional time
    double   _clientTimeSec = 0.0;
};

} // namespace ayt::net