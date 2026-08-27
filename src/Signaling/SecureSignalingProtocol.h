#pragma once
// Internal replay window shared by secure signaling and its focused tests.

#include <cstdint>

namespace ayt::net::detail
{

struct SignalingReplayWindow {
    uint64_t highest = 0;
    uint64_t bits = 0;

    void reset() { highest = 0; bits = 0; }

    bool accept(uint64_t sequence) {
        if (sequence == 0) return false;
        if (sequence > highest) {
            const uint64_t shift = sequence - highest;
            bits = shift >= 64 ? 1u : ((bits << shift) | 1u);
            highest = sequence;
            return true;
        }
        const uint64_t delta = highest - sequence;
        if (delta >= 64) return false;
        const uint64_t mask = uint64_t{1} << delta;
        if ((bits & mask) != 0) return false;
        bits |= mask;
        return true;
    }
};

} // namespace ayt::net::detail
