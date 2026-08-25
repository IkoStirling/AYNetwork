// AYNetwork/Transport/TransportFaultController.cpp - R5.4 (2026-08-25).

#include "TransportFaultController.h"

namespace ayt::net
{

void TransportFaultController::setProfile(uint32_t netId,
                                          const TransportFaultProfile& profile,
                                          uint64_t sessionSeed) {
    _profiles[netId] = profile;
    const uint64_t seed = profile.randomSeed != 0
                        ? profile.randomSeed
                        : (sessionSeed != 0 ? sessionSeed : kDefaultSessionSeed);
    // std::mt19937_64 is not copy-assignable but is default-constructible,
    // so we re-seat the RNG in place via a fresh engine.
    _rngs.erase(netId);
    _rngs.emplace(std::piecewise_construct,
                  std::forward_as_tuple(netId),
                  std::forward_as_tuple(seed));
}

void TransportFaultController::clearProfile(uint32_t netId) {
    _profiles.erase(netId);
    _rngs.erase(netId);
}

std::mt19937_64& TransportFaultController::rngFor(uint32_t netId) {
    auto it = _rngs.find(netId);
    if (it != _rngs.end()) return it->second;

    // First use for this netId — seed from the profile (if any),
    // then the configured session seed, then the default fallback.
    // R6 C4 B-09: consults _sessionSeed (settable via setSessionSeed)
    // so tests and the replay recorder can drive reproducible draws.
    uint64_t seed = _sessionSeed != 0 ? _sessionSeed : kDefaultSessionSeed;
    auto pit = _profiles.find(netId);
    if (pit != _profiles.end() && pit->second.randomSeed != 0) {
        seed = pit->second.randomSeed;
    }
    auto inserted = _rngs.emplace(std::piecewise_construct,
                                  std::forward_as_tuple(netId),
                                  std::forward_as_tuple(seed));
    return inserted.first->second;
}

} // namespace ayt::net
