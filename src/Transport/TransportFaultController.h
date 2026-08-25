// AYNetwork/Transport/TransportFaultController.h - R5.4 (2026-08-25)
// per-connection fault-profile store. Owns the `TransportFaultProfile`
// map (keyed by AYNetwork netId) and one `std::mt19937_64` per profile
// so loss/dup/reorder draws are reproducible.
//
// One controller instance is owned by `AYNetworkSubSystem`. It is
// accessed by `TransportFaultInterceptor` (one per GnsConnection) so
// the interceptor can look up profiles by netId. Tests can poke the
// controller directly via `INetworkSubSystem::setTransportFaultProfile`.

#pragma once

#include <AYNetwork/TransportFaultProfile.h>

#include <cstdint>
#include <random>
#include <unordered_map>

namespace ayt::net
{

class TransportFaultController
{
public:
    TransportFaultController() = default;

    // ----- Session seed (R6 C4 B-09) -----
    //
    // Sets the fallback RNG seed used when a profile's `randomSeed == 0`.
    // Default value is kDefaultSessionSeed. Tests and the replay
    // recorder install a per-session seed here so that fault draws
    // (loss / dup / reorder / latency) are reproducible across runs.
    void setSessionSeed(uint64_t seed) { _sessionSeed = seed; }
    uint64_t getSessionSeed() const { return _sessionSeed; }

    // ----- Profile management -----

    // Install or replace a profile for a connection. `sessionSeed` is
    // the session randomSeed used when `profile.randomSeed == 0`.
    void setProfile(uint32_t netId, const TransportFaultProfile& profile,
                    uint64_t sessionSeed = kDefaultSessionSeed);

    // Remove the profile (and RNG) for a connection. Subsequent calls
    // to `hasProfile` return false; interceptor falls back to
    // passthrough for that connection.
    void clearProfile(uint32_t netId);

    // True if a profile is registered for `netId`.
    bool hasProfile(uint32_t netId) const {
        return _profiles.find(netId) != _profiles.end();
    }

    // Read-only access. Returns nullptr if no profile. The pointer is
    // valid until `clearProfile(netId)` or `setProfile(netId, ...)`.
    const TransportFaultProfile* getProfile(uint32_t netId) const {
        auto it = _profiles.find(netId);
        return it == _profiles.end() ? nullptr : &it->second;
    }

    // ----- RNG access -----
    // Returns the profile's RNG, creating one on first use (seeded from
    // profile.randomSeed or sessionSeed). The returned reference is
    // stable until `clearProfile` is called for the netId.
    std::mt19937_64& rngFor(uint32_t netId);

    // ----- Defaults -----
    // Stable fallback when no session seed is configured. Picked so that
    // tests that don't set a seed still produce deterministic draws.
    static constexpr uint64_t kDefaultSessionSeed = 0xC0FFEEULL;

private:
    std::unordered_map<uint32_t, TransportFaultProfile> _profiles;
    std::unordered_map<uint32_t, std::mt19937_64>      _rngs;
    // R6 C4 (2026-08-25): session-wide seed. Falls back to
    // kDefaultSessionSeed when no session seed is set explicitly.
    uint64_t _sessionSeed = kDefaultSessionSeed;
};

} // namespace ayt::net
