// AYNetwork/TransportFaultProfile.h - R5.4 (2026-08-25) transport fault
// profile. Header-only.
//
// A `TransportFaultProfile` describes how a single connection (or the
// default for connections without a profile) should misbehave on the wire.
// Per-connection profiles live in `TransportFaultController`; per-channel
// behaviour is encoded in `channelMask` (4 bits for the four transport
// channels CHANNEL_RELIABLE/UNRELIABLE/FRAGMENTED/ACK).
//
// Design intent: GNS itself ships a set of `Fake*` config knobs
// (FakePacketLoss/FakePacketLag/FakePacketReorder/FakePacketDup/
// FakeRateLimit_*_*), but the SDK explicitly states they are GLOBAL — see
// steamnetworkingtypes.h:1325-1328. The user requires per-conn +
// per-channel configuration, which the GNS knobs cannot provide. So
// R5.4 implements an AYNetwork-side interceptor at the sealed-bytes seam
// (between `PacketCodec::encode` and `s_gns->SendMessageToConnection`)
// that mirrors the GNS knob surface but is keyed by (netId, channel-mask).
//
// All fault knobs are zero / no-op by default — the interceptor is a pure
// pass-through when `isNoOp()` is true.

#pragma once

// TransportFaultProfile.h - R5.4 (2026-08-25) transport fault profile.
// Header-only, no AYNetwork includes (must be includable from
// TransportFaultController.h before INetwork.h is fully processed — the
// CHANNEL_* constants live inside `namespace ayt::net` in INetwork.h, so
// using them here creates a circular-include chicken-and-egg. Instead we
// hardcode the channel-mask bits using the known values:
//   CHANNEL_RELIABLE   = 0 → bit 0 → 0x01
//   CHANNEL_UNRELIABLE = 1 → bit 1 → 0x02
//   CHANNEL_FRAGMENTED = 2 → bit 2 → 0x04
//   CHANNEL_ACK        = 3 → bit 3 → 0x08
// `affectsChannel` is keyed off the same channel-index range (0..3) and
// does not require the INetwork constants to be in scope at this header's
// evaluation site.

#include <cstdint>

namespace ayt::net
{

// =============================================================================
// Channel mask bits. Indices match the runtime channel enum (0..3).
// Unset bits pass through unmodified.
// =============================================================================
constexpr uint8_t kFaultChannelReliable   = 0x01u;
constexpr uint8_t kFaultChannelUnreliable = 0x02u;
constexpr uint8_t kFaultChannelFragmented = 0x04u;
constexpr uint8_t kFaultChannelAck        = 0x08u;
constexpr uint8_t kFaultChannelAll        = 0x0Fu;
// Reserved bits for future channels. Documented as off-limits.
constexpr uint8_t kFaultChannelReserved  = 0xF0u;

// Channel index range constants (mirror INetwork.h).
// Duplicated here so this header is includable BEFORE INetwork.h.
constexpr uint8_t kChannelIndexMin = 0;
constexpr uint8_t kChannelIndexMax = 3; // CHANNEL_ACK

// =============================================================================
// TransportFaultProfile
// =============================================================================
// One profile = one connection's misbehaviour recipe. Apply via
// `INetworkSubSystem::setTransportFaultProfile(netId, profile)`.
//
// Latency: `latencyMeanMs` ± `latencyJitterMs` (uniform). Jitter alone
// without mean = 0±jitter (small symmetric noise).
//
// Loss/Dup/Reorder: Bernoulli with probability `*Percent / 100`. Range
// [0, 100]. Reorder swaps the new frame with the previous in-queue frame
// so both flow through the same delay pipeline. If there is no previous
// frame on a reorder hit, the new frame is held for a future swap.
//
// Rate limit: token bucket with `rateLimitBytesPerSec` refill rate and
// `rateLimitBurstBytes` capacity. 0 rate disables rate limiting entirely.
// Frames that cannot afford the tokens are re-enqueued at nowMs+1ms.
//
// randomSeed: seeds the per-profile RNG. 0 = use the session randomSeed
// (passed to `TransportFaultController::setProfile` separately, or
// defaults to a stable constant when no session is configured).
struct TransportFaultProfile {
    uint64_t randomSeed        = 0;
    uint8_t  channelMask       = kFaultChannelAll;

    // Latency (ms). mean=0, jitter=0 → no delay. mean=100, jitter=50 →
    // delay uniform in [50, 150]. Clamped to >= 0.
    uint32_t latencyMeanMs     = 0;
    uint32_t latencyJitterMs   = 0;

    // Bernoulli faults (range [0, 100]). 0 = off.
    float    lossPercent       = 0.0f;
    float    dupPercent        = 0.0f;
    float    reorderPercent    = 0.0f;

    // Bandwidth. 0 rate = unlimited.
    uint32_t rateLimitBytesPerSec = 0;
    uint32_t rateLimitBurstBytes  = 0;

    // True when this profile does nothing on the wire — interceptor
    // skips it entirely as a fast path. A profile is a no-op when ALL
    // numeric knobs are zero. The `channelMask` is NOT considered because
    // the default mask = kFaultChannelAll (everything), and an empty mask
    // is itself a no-op (no channels to act on). To explicitly disable
    // faults on every channel, just don't install a profile — call
    // `clearProfile(netId)` instead.
    bool isNoOp() const {
        return latencyMeanMs == 0 && latencyJitterMs == 0
            && lossPercent   == 0.0f
            && dupPercent    == 0.0f
            && reorderPercent == 0.0f
            && rateLimitBytesPerSec == 0;
    }

    // True if `channel` is affected by this profile. Out-of-range channel
    // returns false (interceptor passes through unchanged).
    bool affectsChannel(uint8_t channel) const {
        if (channel > kChannelIndexMax) return false;
        return (channelMask & (1u << channel)) != 0;
    }
};

} // namespace ayt::net
