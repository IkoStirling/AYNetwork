// AYNetwork/Replay/NetworkReplayRecorderAdapter.cpp - R5.3 (2026-08-24).

#include <AYNetwork/Replay/NetworkReplayRecorderAdapter.h>
#include <AYNetwork/Replay/NetworkReplayTypes.h>

#include <AYReplay/ReplayHash.h>

#include <cstring>
#include <limits>

namespace ayt::net::replay
{

namespace
{
// Little-endian packers for the adapter payload headers.
inline void packU32LE(uint8_t* dst, uint32_t v) {
    dst[0] = static_cast<uint8_t>(v & 0xFF);
    dst[1] = static_cast<uint8_t>((v >> 8) & 0xFF);
    dst[2] = static_cast<uint8_t>((v >> 16) & 0xFF);
    dst[3] = static_cast<uint8_t>((v >> 24) & 0xFF);
}
inline void packU16LE(uint8_t* dst, uint16_t v) {
    dst[0] = static_cast<uint8_t>(v & 0xFF);
    dst[1] = static_cast<uint8_t>((v >> 8) & 0xFF);
}
inline void packU64LE(uint8_t* dst, uint64_t v) {
    for (int i = 0; i < 8; ++i) {
        dst[i] = static_cast<uint8_t>((v >> (i * 8)) & 0xFF);
    }
}
inline uint32_t totalPackedSize(size_t variable) {
    return static_cast<uint32_t>(variable);
}
} // anonymous namespace

NetworkReplayRecorderAdapter::NetworkReplayRecorderAdapter(
    std::unique_ptr<ayt::replay::IReplayRecorder> inner)
    : _inner(std::move(inner))
{
}

NetworkReplayRecorderAdapter::~NetworkReplayRecorderAdapter()
{
    if (_inner && _inner->isOpen()) {
        _inner->endSession();
    }
}

bool NetworkReplayRecorderAdapter::beginSession(
    const ayt::replay::ReplayFileHeader& h)
{
    if (!_inner) return false;
    return _inner->beginSession(h);
}

bool NetworkReplayRecorderAdapter::endSession()
{
    if (!_inner) return false;
    return _inner->endSession();
}

bool NetworkReplayRecorderAdapter::recordEvent(
    ayt::replay::ReplayTick tick,
    ayt::replay::ReplayEventType type,
    const uint8_t* payload, size_t size)
{
    if (!_authorityGateOk || !_inner) return true; // gated: silent no-op
    return _inner->recordEvent(tick, type, payload, size);
}

bool NetworkReplayRecorderAdapter::recordCheckpoint(
    ayt::replay::ReplayTick tick, uint64_t stateHash,
    const uint8_t* snapshotBytes, size_t snapshotSize)
{
    if (!_authorityGateOk || !_inner) return true;
    return _inner->recordCheckpoint(tick, stateHash, snapshotBytes, snapshotSize);
}

bool NetworkReplayRecorderAdapter::flush()
{
    if (!_inner) return false;
    return _inner->flush();
}

bool NetworkReplayRecorderAdapter::isOpen() const
{
    return _inner && _inner->isOpen();
}

size_t NetworkReplayRecorderAdapter::bytesWritten() const
{
    return _inner ? _inner->bytesWritten() : 0u;
}

uint32_t NetworkReplayRecorderAdapter::rotationIndex() const
{
    return _inner ? _inner->rotationIndex() : 0u;
}

// ---------------------------------------------------------------------------
// High-level network record methods
// ---------------------------------------------------------------------------

bool NetworkReplayRecorderAdapter::recordInitialFullSnapshot(
    uint32_t connectionId, uint32_t serverTick, uint8_t frameFlags,
    const uint8_t* bodyPayload, size_t size)
{
    if (!_authorityGateOk) return true;
    const size_t varLen = size;
    std::vector<uint8_t> buf(5 + varLen);
    packU32LE(buf.data(), connectionId);
    buf[4] = frameFlags;
    if (size > 0 && bodyPayload) {
        std::memcpy(buf.data() + 5, bodyPayload, size);
    }
    return _inner->recordEvent(serverTick, kEvtNet_InitialFullSnapshot,
                               buf.data(), buf.size());
}

bool NetworkReplayRecorderAdapter::recordSpawn(
    uint32_t connectionId, uint32_t serverTick,
    uint32_t netId, uint64_t schemaHash,
    const uint8_t* spawnPayload, size_t size)
{
    if (!_authorityGateOk) return true;
    std::vector<uint8_t> buf(16 + size);
    packU32LE(buf.data() + 0, connectionId);
    packU32LE(buf.data() + 4, netId);
    packU64LE(buf.data() + 8, schemaHash);
    if (size > 0 && spawnPayload) {
        std::memcpy(buf.data() + 16, spawnPayload, size);
    }
    return _inner->recordEvent(serverTick, kEvtNet_Spawn,
                               buf.data(), buf.size());
}

bool NetworkReplayRecorderAdapter::recordDespawn(
    uint32_t connectionId, uint32_t serverTick, uint32_t netId)
{
    if (!_authorityGateOk) return true;
    uint8_t buf[8];
    packU32LE(buf + 0, connectionId);
    packU32LE(buf + 4, netId);
    return _inner->recordEvent(serverTick, kEvtNet_Despawn, buf, sizeof(buf));
}

bool NetworkReplayRecorderAdapter::recordDeltaSnapshot(
    uint32_t connectionId, uint32_t serverTick, uint8_t frameFlags,
    const uint8_t* bodyPayload, size_t size)
{
    if (!_authorityGateOk) return true;
    std::vector<uint8_t> buf(5 + size);
    packU32LE(buf.data(), connectionId);
    buf[4] = frameFlags;
    if (size > 0 && bodyPayload) {
        std::memcpy(buf.data() + 5, bodyPayload, size);
    }
    return _inner->recordEvent(serverTick, kEvtNet_DeltaSnapshot,
                               buf.data(), buf.size());
}

bool NetworkReplayRecorderAdapter::recordInput(
    uint32_t connectionId, uint32_t serverTick,
    uint32_t inputSeq, uint32_t serverTickAtSend,
    const uint8_t* payload, size_t size)
{
    if (!_authorityGateOk) return true;
    std::vector<uint8_t> buf(16 + size);
    packU32LE(buf.data() + 0, connectionId);
    packU32LE(buf.data() + 4, inputSeq);
    packU32LE(buf.data() + 8, serverTickAtSend);
    packU32LE(buf.data() + 12, totalPackedSize(size));
    if (size > 0 && payload) {
        std::memcpy(buf.data() + 16, payload, size);
    }
    return _inner->recordEvent(serverTick, kEvtNet_InputBatch,
                               buf.data(), buf.size());
}

bool NetworkReplayRecorderAdapter::recordRpc(
    uint16_t messageType, uint32_t connectionId, uint32_t serverTick,
    const uint8_t* body, size_t size)
{
    if (!_authorityGateOk) return true;
    std::vector<uint8_t> buf(10 + size);
    packU16LE(buf.data() + 0, messageType);
    packU32LE(buf.data() + 2, connectionId);
    packU32LE(buf.data() + 6, totalPackedSize(size));
    if (size > 0 && body) {
        std::memcpy(buf.data() + 10, body, size);
    }
    return _inner->recordEvent(serverTick, kEvtNet_RpcBatch,
                               buf.data(), buf.size());
}

bool NetworkReplayRecorderAdapter::recordAuthorityChange(
    uint32_t netId, uint32_t serverTick,
    uint8_t oldKind, uint8_t newKind, uint32_t connectionId)
{
    if (!_authorityGateOk) return true;
    uint8_t buf[14];
    packU32LE(buf + 0, netId);
    buf[4] = oldKind;
    buf[5] = newKind;
    packU32LE(buf + 6, connectionId);
    buf[10] = buf[11] = buf[12] = buf[13] = 0; // reserved
    return _inner->recordEvent(serverTick, kEvtNet_AuthorityChange,
                               buf, sizeof(buf));
}

bool NetworkReplayRecorderAdapter::recordPeriodicCheckpoint(
    uint32_t serverTick,
    const std::vector<uint32_t>& registeredNetIds,
    const StateBytesProvider& provider)
{
    if (!_authorityGateOk) return true;
    if (!_inner || !provider) return false;

    // Store a restartable snapshot as [count] + repeated
    // [netId][bodySize][replicationBody] entries while hashing the exact
    // same identity and bytes.
    uint64_t h = ayt::replay::kFnv1a64Offset;
    std::vector<uint8_t> scratch;
    std::vector<uint8_t> snapshot(4, 0);
    uint32_t count = 0;
    for (uint32_t netId : registeredNetIds) {
        scratch.clear();
        if (!provider(netId, scratch)) continue;
        if (scratch.size() > std::numeric_limits<uint32_t>::max()) return false;
        const size_t oldSize = snapshot.size();
        if (oldSize > std::numeric_limits<size_t>::max() - 8u - scratch.size()) {
            return false;
        }
        snapshot.resize(oldSize + 8u + scratch.size());
        packU32LE(snapshot.data() + oldSize, netId);
        packU32LE(snapshot.data() + oldSize + 4u,
                  static_cast<uint32_t>(scratch.size()));
        if (!scratch.empty()) {
            std::memcpy(snapshot.data() + oldSize + 8u,
                        scratch.data(), scratch.size());
        }
        h = ayt::replay::fnv1a64Combine(h, snapshot.data() + oldSize, 4u);
        h = ayt::replay::fnv1a64Combine(h, scratch.data(), scratch.size());
        ++count;
    }
    if (count == 0) return false;
    packU32LE(snapshot.data(), count);
    return _inner->recordCheckpoint(serverTick, h,
                                    snapshot.data(), snapshot.size());
}

} // namespace ayt::net::replay
