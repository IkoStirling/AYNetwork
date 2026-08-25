// AYNetwork/Replay/NetworkReplayEventDecoder.cpp - R6.5-2 (2026-08-25).

#include <AYNetwork/Replay/NetworkReplayEventDecoder.h>

#include <cstring>

namespace ayt::net::replay
{

namespace
{
// Little-endian unpackers; mirror NetworkReplayRecorderAdapter's packers.
inline uint32_t unpackU32LE(const uint8_t* p) {
    return  static_cast<uint32_t>(p[0])        |
           (static_cast<uint32_t>(p[1]) <<  8) |
           (static_cast<uint32_t>(p[2]) << 16) |
           (static_cast<uint32_t>(p[3]) << 24);
}
inline uint16_t unpackU16LE(const uint8_t* p) {
    return  static_cast<uint16_t>(p[0])        |
           (static_cast<uint16_t>(p[1]) <<  8);
}
inline uint64_t unpackU64LE(const uint8_t* p) {
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i) {
        v |= static_cast<uint64_t>(p[i]) << (i * 8);
    }
    return v;
}

// Translate a recorded id to its live id via `remap`. On miss, return
// the literal `recorded` and (when caller asked) append to `unmappedIds`.
inline uint32_t translateConnId(uint32_t recorded,
                                const ConnectionIdRemap& remap,
                                std::vector<uint32_t>* unmappedIds)
{
    auto it = remap.find(recorded);
    if (it == remap.end()) {
        if (unmappedIds) unmappedIds->push_back(recorded);
        return recorded;
    }
    return it->second;
}
} // anonymous namespace

ConnectionIdRemap NetworkReplayEventDecoder::buildRemap(
    const std::vector<uint32_t>& liveConnectionIdsInAcceptOrder)
{
    // NetworkSubSystem::allocateNetId starts at one. The first accepted
    // connection therefore has recorded id 1, the second id 2, etc.
    // Callers that have accept-order
    // metadata from the live server can build a more sophisticated map
    // in R7+; the foundation player is agnostic of how ids are translated.
    ConnectionIdRemap m;
    for (size_t i = 0; i < liveConnectionIdsInAcceptOrder.size(); ++i) {
        m[static_cast<uint32_t>(i + 1u)] = liveConnectionIdsInAcceptOrder[i];
    }
    return m;
}

ayt::replay::IReplayPlayer::Error NetworkReplayEventDecoder::decodeNext(
    ayt::replay::IReplayPlayer& player,
    DecodedEvent& out,
    const ConnectionIdRemap& remap,
    std::vector<uint32_t>* unmappedIds)
{
    out = {};

    ayt::replay::ReplayEventHeader hdr{};
    std::vector<uint8_t> payload;
    bool isCheckpoint = false;
    const auto e = player.readNextEvent(hdr, payload, &isCheckpoint);
    if (e != ayt::replay::IReplayPlayer::Error::Ok) return e;

    out.tick = hdr.tick;
    out.eventType = hdr.eventType;

    if (isCheckpoint) {
        out.isCheckpoint = true;
        out.eventType = kEvtNet_Checkpoint;
        out.snapshot = std::move(payload);
        // stateHash is not surfaced via ReplayCheckpointHeader on the
        // foundation readNextEvent path (the foundation emits a sentinel
        // event header). Callers that need the checkpoint's hash should
        // read it via IReplayPlayer::seekToCheckpoint + a higher-level
        // helper that reads the 24-byte header directly. R6.5-3 may add
        // that seam if needed; for the decoder contract we expose only
        // what the foundation currently surfaces.
        return ayt::replay::IReplayPlayer::Error::Ok;
    }

    switch (hdr.eventType) {
        case kEvtNet_InitialFullSnapshot: {
            if (payload.size() < 5) {
                return ayt::replay::IReplayPlayer::Error::IoError;
            }
            auto& t = out.initialFullSnapshot;
            t.connectionId = translateConnId(unpackU32LE(payload.data()), remap, unmappedIds);
            t.frameFlags   = payload[4];
            if (payload.size() > 5) {
                t.payload.assign(payload.begin() + 5, payload.end());
            }
            return ayt::replay::IReplayPlayer::Error::Ok;
        }
        case kEvtNet_Spawn: {
            if (payload.size() < 16) {
                return ayt::replay::IReplayPlayer::Error::IoError;
            }
            auto& t = out.spawn;
            t.connectionId = translateConnId(unpackU32LE(payload.data()), remap, unmappedIds);
            t.netId        = unpackU32LE(payload.data() + 4);
            t.schemaHash   = unpackU64LE(payload.data() + 8);
            if (payload.size() > 16) {
                t.payload.assign(payload.begin() + 16, payload.end());
            }
            return ayt::replay::IReplayPlayer::Error::Ok;
        }
        case kEvtNet_Despawn: {
            if (payload.size() < 8) {
                return ayt::replay::IReplayPlayer::Error::IoError;
            }
            auto& t = out.despawn;
            t.connectionId = translateConnId(unpackU32LE(payload.data()), remap, unmappedIds);
            t.netId        = unpackU32LE(payload.data() + 4);
            return ayt::replay::IReplayPlayer::Error::Ok;
        }
        case kEvtNet_DeltaSnapshot: {
            if (payload.size() < 5) {
                return ayt::replay::IReplayPlayer::Error::IoError;
            }
            auto& t = out.deltaSnapshot;
            t.connectionId = translateConnId(unpackU32LE(payload.data()), remap, unmappedIds);
            t.frameFlags   = payload[4];
            if (payload.size() > 5) {
                t.payload.assign(payload.begin() + 5, payload.end());
            }
            return ayt::replay::IReplayPlayer::Error::Ok;
        }
        case kEvtNet_InputBatch: {
            if (payload.size() < 16) {
                return ayt::replay::IReplayPlayer::Error::IoError;
            }
            auto& t = out.inputBatch;
            t.connectionId     = translateConnId(unpackU32LE(payload.data()), remap, unmappedIds);
            t.inputSeq         = unpackU32LE(payload.data() +  4);
            t.serverTickAtSend = unpackU32LE(payload.data() +  8);
            const uint32_t declared = unpackU32LE(payload.data() + 12);
            const size_t avail = payload.size() - 16;
            if (declared > avail) {
                return ayt::replay::IReplayPlayer::Error::Truncated;
            }
            const size_t take = declared;
            if (take > 0) {
                t.payload.assign(payload.begin() + 16, payload.begin() + 16 + take);
            }
            return ayt::replay::IReplayPlayer::Error::Ok;
        }
        case kEvtNet_RpcBatch: {
            if (payload.size() < 10) {
                return ayt::replay::IReplayPlayer::Error::IoError;
            }
            auto& t = out.rpcBatch;
            t.messageType  = unpackU16LE(payload.data());
            t.connectionId = translateConnId(unpackU32LE(payload.data() + 2), remap, unmappedIds);
            const uint32_t declared = unpackU32LE(payload.data() + 6);
            const size_t avail = payload.size() - 10;
            if (declared > avail) {
                return ayt::replay::IReplayPlayer::Error::Truncated;
            }
            const size_t take = declared;
            if (take > 0) {
                t.body.assign(payload.begin() + 10, payload.begin() + 10 + take);
            }
            return ayt::replay::IReplayPlayer::Error::Ok;
        }
        case kEvtNet_AuthorityChange: {
            if (payload.size() < 14) {
                return ayt::replay::IReplayPlayer::Error::IoError;
            }
            auto& t = out.authorityChange;
            t.netId        = unpackU32LE(payload.data());
            t.oldKind      = payload[4];
            t.newKind      = payload[5];
            t.connectionId = translateConnId(unpackU32LE(payload.data() + 6), remap, unmappedIds);
            return ayt::replay::IReplayPlayer::Error::Ok;
        }
        default:
            // Foundation events (SessionBegin/End/TextMarker) and any
            // future adapter event types in the AYNetwork range are
            // passed through with eventType set but no typed payload.
            return ayt::replay::IReplayPlayer::Error::Ok;
    }
}

} // namespace ayt::net::replay
