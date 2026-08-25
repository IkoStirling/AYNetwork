// AYNetwork/Prediction/PredictionManager.cpp - R5.2 prediction orchestrator.

#include <AYNetwork/Prediction/PredictionManager.h>
#include <AYNetwork/Prediction/ClientInputCodec.h>

namespace ayt::net
{

void PredictionManager::setRingCapacity(uint32_t cap)
{
    _ringCapacity = cap > 0 ? cap : 1u;
    // Existing rings keep their capacity — only FUTURE connections pick
    // up the new size. This is documented behavior so callers can size
    // up without surprising mid-game state.
}

bool PredictionManager::onClientInput(uint32_t connectionId,
                                       const uint8_t* body, size_t bodySize)
{
    if (!body || bodySize == 0) return false;

    uint32_t inputSeq = 0;
    uint32_t serverTickAtSend = 0;
    const uint8_t* payload = nullptr;
    size_t payloadSize = 0;
    if (!ClientInputCodec::read(body, bodySize,
                                inputSeq, serverTickAtSend,
                                payload, payloadSize)) {
        return false;
    }

    auto [it, inserted] = _rings.try_emplace(connectionId, _ringCapacity);
    (void)inserted;
    InputRing& ring = it->second;

    ClientInputRecord rec;
    rec.inputSeq = inputSeq;
    rec.serverTickAtSend = serverTickAtSend;
    rec.payload.assign(payload, payload + payloadSize);
    return ring.push(std::move(rec));
}

void PredictionManager::consumeClientInputs(uint32_t simTick,
                                            const ApplyInputFn& apply)
{
    (void)simTick; // reserved for telemetry / deterministic ordering
    if (!apply) return;

    for (auto& kv : _rings) {
        const uint32_t connectionId = kv.first;
        InputRing& ring = kv.second;
        if (ring.empty()) continue;

        // Walk the live seq range from ackedSeq (next-not-yet-acked) to
        // newestSeq inclusive. ackedSeq is set to (last_acked + 1) so
        // the first candidate is one past it.
        const uint32_t hi = ring.newestSeq();
        uint32_t s = ring.ackedSeq();
        // Bound iteration by the seq distance (cap-32 worst-case). Even
        // on a full ring we never emit more than `_capacity` records.
        for (uint32_t i = 0; i < ring.capacity(); ++i) {
            ClientInputRecord rec;
            if (ring.tryGet(s, rec)) {
                apply(connectionId, rec.inputSeq,
                      rec.payload.data(), rec.payload.size());
                ring.ackUpTo(rec.inputSeq);
            }
            // Advance; stop after emitting `hi`.
            if (s == hi) break;
            s = s + 1u;
        }
    }
}

void PredictionManager::markAcked(uint32_t connectionId,
                                  uint32_t lastAckedInputTick)
{
    _ackedSeq[connectionId] = lastAckedInputTick;
    auto it = _rings.find(connectionId);
    if (it != _rings.end()) {
        it->second.ackUpTo(lastAckedInputTick);
    }
}

uint32_t PredictionManager::lastAckedInputTick(uint32_t connectionId) const
{
    auto it = _ackedSeq.find(connectionId);
    return it != _ackedSeq.end() ? it->second : 0u;
}

size_t PredictionManager::pendingInputCount(uint32_t connectionId) const
{
    auto it = _rings.find(connectionId);
    return it != _rings.end() ? it->second.pendingCount() : 0u;
}

void PredictionManager::predict(uint32_t netId,
                                const std::vector<ClientInputRecord>& inputs,
                                const PredictFn& step,
                                float dtSec)
{
    if (!step) return;
    for (const ClientInputRecord& rec : inputs) {
        step(netId, rec.payload.data(), rec.payload.size(), dtSec);
        auto git = _ghosts.find(netId);
        if (git != _ghosts.end()) {
            if (seqGreaterThan(rec.inputSeq, git->second.lastPredictedForInputSeq)) {
                git->second.lastPredictedForInputSeq = rec.inputSeq;
            }
        }
    }
}

void PredictionManager::onServerAck(uint32_t netId,
                                    uint32_t lastAckedInputTick,
                                    uint32_t /*serverCommandAge*/)
{
    auto it = _ghosts.find(netId);
    if (it == _ghosts.end()) return;
    it->second.lastAckedInputTick = lastAckedInputTick;
}

void PredictionManager::registerPredictedGhost(uint32_t netId, uint64_t layoutHash)
{
    PredictedGhost& g = _ghosts[netId];
    g.netId = netId;
    g.layoutHash = layoutHash;
}

void PredictionManager::unregisterPredictedGhost(uint32_t netId)
{
    _ghosts.erase(netId);
}

bool PredictionManager::isPredictedGhost(uint32_t netId) const
{
    return _ghosts.find(netId) != _ghosts.end();
}

void PredictionManager::setPredictedBytes(uint32_t netId,
                                          const uint8_t* data, size_t n)
{
    auto it = _ghosts.find(netId);
    if (it == _ghosts.end()) return;
    it->second.bytes.assign(data, data + n);
}

bool PredictionManager::tryGetPredictedBytes(uint32_t netId,
                                             std::vector<uint8_t>& out) const
{
    auto it = _ghosts.find(netId);
    if (it == _ghosts.end()) return false;
    out = it->second.bytes;
    return true;
}

uint64_t PredictionManager::getLayoutHash(uint32_t netId) const
{
    auto it = _ghosts.find(netId);
    return it != _ghosts.end() ? it->second.layoutHash : 0u;
}

void PredictionManager::setLayoutHash(uint32_t netId, uint64_t h)
{
    auto it = _ghosts.find(netId);
    if (it != _ghosts.end()) it->second.layoutHash = h;
}

uint32_t PredictionManager::lastPredictedInputSeq(uint32_t netId) const
{
    auto it = _ghosts.find(netId);
    return it != _ghosts.end() ? it->second.lastPredictedForInputSeq : 0u;
}

} // namespace ayt::net
