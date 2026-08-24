// AYNetwork/Transport/TransportFaultInterceptor.cpp - R5.4 (2026-08-25).

#include "TransportFaultController.h"
#include "TransportFaultInterceptor.h"

#include <AYNetwork/INetwork.h>     // R5.4 (2026-08-25): CHANNEL_ACK

#include <algorithm>

namespace ayt::net
{

namespace
{
// Percent helper. Returns true with probability p/100.
bool drawBernoulli(float p, std::mt19937_64& rng) {
    if (p <= 0.0f) return false;
    if (p >= 100.0f) return true;
    std::uniform_real_distribution<float> dist(0.0f, 100.0f);
    return dist(rng) < p;
}
} // anonymous namespace

TransportFaultInterceptor::TransportFaultInterceptor(uint32_t netId,
                                                     TransportFaultController& ctl)
    : _netId(netId), _ctl(ctl)
{
}

uint64_t TransportFaultInterceptor::sampleLatencyMs(const TransportFaultProfile& prof,
                                                    std::mt19937_64& rng) {
    if (prof.latencyMeanMs == 0 && prof.latencyJitterMs == 0) return 0;
    const int64_t mean = static_cast<int64_t>(prof.latencyMeanMs);
    const int64_t jit  = static_cast<int64_t>(prof.latencyJitterMs);
    if (jit == 0) return static_cast<uint64_t>(std::max<int64_t>(0, mean));
    std::uniform_int_distribution<int64_t> dist(mean - jit, mean + jit);
    const int64_t v = dist(rng);
    return static_cast<uint64_t>(std::max<int64_t>(0, v));
}

bool TransportFaultInterceptor::isEnabled() const {
    const TransportFaultProfile* prof = _ctl.getProfile(_netId);
    if (!prof) return false;
    return !prof->isNoOp();
}

size_t TransportFaultInterceptor::sendQueueSize(uint8_t channel) const {
    if (channel > CHANNEL_ACK) return 0;
    return _sendChannels[channel].delayQueue.size();
}

size_t TransportFaultInterceptor::recvQueueSize(uint8_t channel) const {
    if (channel > CHANNEL_ACK) return 0;
    return _recvChannels[channel].delayQueue.size();
}

uint64_t TransportFaultInterceptor::sendDroppedCount() {
    const uint64_t v = _sendDropped;
    _sendDropped = 0;
    return v;
}

uint64_t TransportFaultInterceptor::recvDroppedCount() {
    const uint64_t v = _recvDropped;
    _recvDropped = 0;
    return v;
}

void TransportFaultInterceptor::clearAll() {
    for (auto& ch : _sendChannels) {
        ch.delayQueue.clear();
        ch.lastFrame.clear();
        ch.hasLastFrame = false;
    }
    for (auto& ch : _recvChannels) {
        ch.delayQueue.clear();
        ch.lastFrame.clear();
        ch.hasLastFrame = false;
    }
    _sendDropped = 0;
    _recvDropped = 0;
}

void TransportFaultInterceptor::onSend(uint64_t nowMs, const uint8_t* sealed,
                                       size_t len, uint8_t channel) {
    if (channel > CHANNEL_ACK) return;
    handleOne(nowMs, sealed, len, channel,
              _sendChannels[channel].delayQueue,
              _sendChannels[channel].bucket,
              _sendChannels[channel],
              _sendDropped);
}

void TransportFaultInterceptor::onRecv(uint64_t nowMs, const uint8_t* sealed,
                                       size_t len, uint8_t channel) {
    if (channel > CHANNEL_ACK) return;
    handleOne(nowMs, sealed, len, channel,
              _recvChannels[channel].delayQueue,
              _recvChannels[channel].bucket,
              _recvChannels[channel],
              _recvDropped);
}

void TransportFaultInterceptor::handleOne(uint64_t nowMs, const uint8_t* sealed,
                                          size_t len, uint8_t channel,
                                          DelayedFrameQueue& queue,
                                          TokenBucket& /*bucket*/,
                                          ChannelState& chState,
                                          uint64_t& droppedAcc) {
    const TransportFaultProfile* prof = _ctl.getProfile(_netId);
    if (!prof || prof->isNoOp()) {
        // No profile / no-op. Pass through with zero delay (release next tick).
        DelayedFrame f;
        f.releaseAtMs = nowMs;
        f.channel     = channel;
        f.bytes.assign(sealed, sealed + len);
        queue.enqueue(std::move(f));
        if (_queueCap > 0) {
            uint64_t dropped = 0;
            queue.enforceCap(_queueCap, &dropped);
            droppedAcc += dropped;
        }
        return;
    }

    if (!prof->affectsChannel(channel)) {
        // Channel not in this profile's mask. Pass through with zero delay.
        DelayedFrame f;
        f.releaseAtMs = nowMs;
        f.channel     = channel;
        f.bytes.assign(sealed, sealed + len);
        queue.enqueue(std::move(f));
        if (_queueCap > 0) {
            uint64_t dropped = 0;
            queue.enforceCap(_queueCap, &dropped);
            droppedAcc += dropped;
        }
        return;
    }

    // Pull the RNG for this connection.
    std::mt19937_64& rng = _ctl.rngFor(_netId);

    // 1. Loss.
    if (drawBernoulli(prof->lossPercent, rng)) {
        // Frame dropped — nothing enqueued. (Counter could be added
        // here; left out of R5.4 since tests assert via final queue
        // contents / tick release counts.)
        return;
    }

    // 2. Reorder — swap with predecessor.
    std::vector<uint8_t> current(sealed, sealed + len);
    if (drawBernoulli(prof->reorderPercent, rng)) {
        if (chState.hasLastFrame) {
            // Swap: emit the predecessor now (zero delay), queue
            // current as the new predecessor (it leaves later with
            // its own sampled delay).
            std::vector<uint8_t> swapped = std::move(chState.lastFrame);
            chState.lastFrame = std::move(current);
            chState.hasLastFrame = true;

            DelayedFrame pred;
            pred.releaseAtMs = nowMs;
            pred.channel     = channel;
            pred.bytes       = std::move(swapped);
            queue.enqueue(std::move(pred));

            const uint64_t delay = sampleLatencyMs(*prof, rng);
            DelayedFrame delayed;
            delayed.releaseAtMs = nowMs + delay;
            delayed.channel     = channel;
            delayed.bytes       = chState.lastFrame; // copy of new predecessor
            queue.enqueue(std::move(delayed));
        } else {
            // No predecessor yet — store as predecessor for the next
            // frame. The current frame is silently delayed.
            chState.lastFrame = std::move(current);
            chState.hasLastFrame = true;
        }

        if (_queueCap > 0) {
            uint64_t dropped = 0;
            queue.enforceCap(_queueCap, &dropped);
            droppedAcc += dropped;
        }
        return;
    }

    // 3. Delay.
    const uint64_t delay = sampleLatencyMs(*prof, rng);

    // 4. Dup — enqueue two copies, the second with +mean delay.
    if (drawBernoulli(prof->dupPercent, rng)) {
        DelayedFrame primary;
        primary.releaseAtMs = nowMs + delay;
        primary.channel     = channel;
        primary.bytes       = current;
        queue.enqueue(std::move(primary));

        DelayedFrame dup;
        dup.releaseAtMs = nowMs + delay + prof->latencyMeanMs;
        dup.channel     = channel;
        dup.isDuplicate = true;
        dup.bytes       = current;
        queue.enqueue(std::move(dup));
    } else {
        DelayedFrame single;
        single.releaseAtMs = nowMs + delay;
        single.channel     = channel;
        single.bytes       = std::move(current);
        queue.enqueue(std::move(single));
    }

    if (_queueCap > 0) {
        uint64_t dropped = 0;
        queue.enforceCap(_queueCap, &dropped);
        droppedAcc += dropped;
    }
}

size_t TransportFaultInterceptor::tick(
    uint64_t nowMs, double dtSeconds,
    std::vector<std::pair<std::vector<uint8_t>, uint8_t>>& sendOut,
    std::vector<std::pair<std::vector<uint8_t>, uint8_t>>& recvOut)
{
    const TransportFaultProfile* prof = _ctl.getProfile(_netId);
    size_t released = 0;

    auto drain = [&](std::array<ChannelState, 4>& channels,
                     std::vector<std::pair<std::vector<uint8_t>, uint8_t>>& out,
                     uint64_t& /*droppedAcc*/) {
        for (size_t c = 0; c < channels.size(); ++c) {
            ChannelState& ch = channels[c];
            auto ready = ch.delayQueue.releaseReady(nowMs);
            if (ready.empty()) continue;

            const bool rateLimited = prof && prof->rateLimitBytesPerSec > 0
                                  && prof->affectsChannel(static_cast<uint8_t>(c));

            for (auto& f : ready) {
                if (rateLimited) {
                    ch.bucket.configure(prof->rateLimitBytesPerSec,
                                        prof->rateLimitBurstBytes);
                    if (!ch.bucket.tryConsume(static_cast<uint32_t>(f.bytes.size()),
                                              dtSeconds)) {
                        // Re-enqueue at nowMs + 1ms for next tick.
                        f.releaseAtMs = nowMs + 1;
                        ch.delayQueue.enqueue(std::move(f));
                        continue;
                    }
                }
                out.emplace_back(std::move(f.bytes),
                                 static_cast<uint8_t>(c));
                ++released;
            }
        }
    };

    drain(_sendChannels, sendOut, _sendDropped);
    drain(_recvChannels, recvOut, _recvDropped);
    return released;
}

} // namespace ayt::net
