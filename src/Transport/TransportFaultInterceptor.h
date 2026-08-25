// AYNetwork/Transport/TransportFaultInterceptor.h - R5.4 (2026-08-25)
// per-GnsConnection fault interceptor. Holds per-channel delay queue +
// token bucket + reorder swap state. Pumped from `GnsConnection::pump`
// (send + recv drain in one tick).
//
// Wire path:
//   send: GnsConnection::send → onSend(nowMs, sealed, channel) →
//         delay-queue → rate-limit-bucket → released frames
//         → caller flushes to _rawSend.
//   recv: GnsConnection::pump → onRecv(nowMs, raw, channel) →
//         delay-queue → rate-limit-bucket → released frames
//         → caller flushes to onRawData → PacketCodec::decode.

#pragma once

#include "DelayedFrameQueue.h"
#include "TokenBucket.h"
#include <AYNetwork/TransportFaultProfile.h>

#include <array>
#include <cstdint>
#include <random>
#include <utility>
#include <vector>

namespace ayt::net
{

class TransportFaultController; // fwd

class TransportFaultInterceptor
{
public:
    TransportFaultInterceptor(uint32_t netId, TransportFaultController& ctl);

    // ----- Send-side -----
    // Called by GnsConnection::send after PacketCodec::encode (or after
    // PacketAssembler::fragment) and before _rawSend. Consumes the
    // sealed frame, applies Bernoulli loss/dup/reorder, and queues the
    // survivors (with delay relative to nowMs) for later tick() release.
    void onSend(uint64_t nowMs, const uint8_t* sealed, size_t len, uint8_t channel);

    // ----- Recv-side -----
    // Called by GnsConnection::pump for each incoming message, before
    // onRawData → PacketCodec::decode. Mirrors onSend's logic on the
    // raw GNS payload.
    void onRecv(uint64_t nowMs, const uint8_t* sealed, size_t len, uint8_t channel);

    // ----- Tick pump -----
    // Drains both directions' delay queues. For each ready frame:
    //   - If rate-limit enabled and tryConsume fails, re-enqueue at
    //     nowMs + 1 ms (next tick retries).
    //   - Otherwise emit to `sendOut` / `recvOut` (caller flushes).
    // `dtSeconds` is the wall-clock delta since the previous tick
    // (used for token-bucket refill). Defaults to 0 for tests that
    // drive the bucket purely by frame-size consumption.
    size_t tick(uint64_t nowMs, double dtSeconds,
                std::vector<std::pair<std::vector<uint8_t>, uint8_t>>& sendOut,
                std::vector<std::pair<std::vector<uint8_t>, uint8_t>>& recvOut);

    size_t tickSend(uint64_t nowMs, double dtSeconds,
                    std::vector<std::pair<std::vector<uint8_t>, uint8_t>>& out);
    size_t tickRecv(uint64_t nowMs, double dtSeconds,
                    std::vector<std::pair<std::vector<uint8_t>, uint8_t>>& out);

    // True if any profile is installed for this netId AND any channel
    // mask intersects with the four runtime channels. Lets callers
    // skip the interceptor entirely when it would be a no-op.
    bool isEnabled() const;

    // ----- Test-only -----
    size_t sendQueueSize(uint8_t channel) const;
    size_t recvQueueSize(uint8_t channel) const;
    uint64_t sendDroppedCount();
    uint64_t recvDroppedCount();
    void     clearAll();

    // Cap each delay queue at N frames. 0 = uncapped (R5.4 default).
    void setQueueCap(size_t cap) { _queueCap = cap; }
    size_t queueCap() const { return _queueCap; }

private:
    // Per-channel state. Index = channel (0..3).
    struct ChannelState {
        DelayedFrameQueue delayQueue;
        TokenBucket       bucket;
    };

    void handleOne(uint64_t nowMs, const uint8_t* sealed, size_t len,
                   uint8_t channel,
                   DelayedFrameQueue& queue, TokenBucket& bucket,
                   uint64_t& droppedAcc);

    // Returns the sampled delay (ms) for this frame using the profile's
    // mean/jitter and the controller's RNG. Always >= 0.
    static uint64_t sampleLatencyMs(const TransportFaultProfile& prof,
                                    std::mt19937_64& rng);

    size_t drainDirection(
        uint64_t nowMs, double dtSeconds,
        std::array<ChannelState, 4>& channels,
        std::vector<std::pair<std::vector<uint8_t>, uint8_t>>& out);

    uint32_t _netId;
    TransportFaultController& _ctl;

    std::array<ChannelState, 4> _sendChannels;
    std::array<ChannelState, 4> _recvChannels;
    uint64_t _sendDropped = 0;
    uint64_t _recvDropped = 0;
    size_t   _queueCap    = 0;
};

} // namespace ayt::net
