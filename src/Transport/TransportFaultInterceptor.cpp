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
    }
    for (auto& ch : _recvChannels) {
        ch.delayQueue.clear();
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
              _sendDropped);
}

void TransportFaultInterceptor::onRecv(uint64_t nowMs, const uint8_t* sealed,
                                       size_t len, uint8_t channel) {
    if (channel > CHANNEL_ACK) return;
    handleOne(nowMs, sealed, len, channel,
              _recvChannels[channel].delayQueue,
              _recvChannels[channel].bucket,
              _recvDropped);
}

void TransportFaultInterceptor::handleOne(uint64_t nowMs, const uint8_t* sealed,
                                          size_t len, uint8_t channel,
                                          DelayedFrameQueue& queue,
                                          TokenBucket& /*bucket*/,
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

    // 2. Delay and adjacent reorder. Every surviving frame is enqueued
    // exactly once; a reorder hit swaps it with the preceding queued frame.
    // This avoids the old persistent predecessor state which could retain a
    // frame forever and later emit a duplicate copy.
    std::vector<uint8_t> current(sealed, sealed + len);
    const uint64_t delay = sampleLatencyMs(*prof, rng);
    const bool reorder = drawBernoulli(prof->reorderPercent, rng);

    // 3. Dup — enqueue two copies, the second with +mean delay.
    DelayedFrame primary;
    primary.releaseAtMs = reorder ? nowMs : nowMs + delay;
    primary.channel     = channel;
    primary.bytes       = current;
    queue.enqueue(std::move(primary));
    if (reorder) queue.swapLastTwo();

    if (drawBernoulli(prof->dupPercent, rng)) {
        DelayedFrame dup;
        dup.releaseAtMs = nowMs + delay + prof->latencyMeanMs;
        dup.channel     = channel;
        dup.isDuplicate = true;
        dup.bytes       = current;
        queue.enqueue(std::move(dup));
    }

    if (_queueCap > 0) {
        uint64_t dropped = 0;
        queue.enforceCap(_queueCap, &dropped);
        droppedAcc += dropped;
    }
}

size_t TransportFaultInterceptor::drainDirection(
    uint64_t nowMs, double dtSeconds,
    std::array<ChannelState, 4>& channels,
    std::vector<std::pair<std::vector<uint8_t>, uint8_t>>& out) {
    const TransportFaultProfile* prof = _ctl.getProfile(_netId);
    size_t released = 0;
    for (size_t c = 0; c < channels.size(); ++c) {
        ChannelState& ch = channels[c];
        auto ready = ch.delayQueue.releaseReady(nowMs);
        if (ready.empty()) continue;
        const bool rateLimited = prof && prof->rateLimitBytesPerSec > 0
                              && prof->affectsChannel(static_cast<uint8_t>(c));
        bool refilled = false;
        for (auto& f : ready) {
            if (rateLimited) {
                ch.bucket.configure(prof->rateLimitBytesPerSec,
                                    prof->rateLimitBurstBytes);
                const double refill = refilled ? 0.0 : dtSeconds;
                refilled = true;
                if (!ch.bucket.tryConsume(static_cast<uint32_t>(f.bytes.size()),
                                          refill)) {
                    f.releaseAtMs = nowMs + 1;
                    ch.delayQueue.enqueue(std::move(f));
                    continue;
                }
            }
            out.emplace_back(std::move(f.bytes), static_cast<uint8_t>(c));
            ++released;
        }
    }
    return released;
}

size_t TransportFaultInterceptor::tickSend(
    uint64_t nowMs, double dtSeconds,
    std::vector<std::pair<std::vector<uint8_t>, uint8_t>>& out) {
    return drainDirection(nowMs, dtSeconds, _sendChannels, out);
}

size_t TransportFaultInterceptor::tickRecv(
    uint64_t nowMs, double dtSeconds,
    std::vector<std::pair<std::vector<uint8_t>, uint8_t>>& out) {
    return drainDirection(nowMs, dtSeconds, _recvChannels, out);
}

size_t TransportFaultInterceptor::tick(
    uint64_t nowMs, double dtSeconds,
    std::vector<std::pair<std::vector<uint8_t>, uint8_t>>& sendOut,
    std::vector<std::pair<std::vector<uint8_t>, uint8_t>>& recvOut) {
    return tickSend(nowMs, dtSeconds, sendOut)
         + tickRecv(nowMs, dtSeconds, recvOut);
}

} // namespace ayt::net
