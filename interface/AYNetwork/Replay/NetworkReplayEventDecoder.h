// AYNetwork/Replay/NetworkReplayEventDecoder.h - R6.5-2 (2026-08-25).
//
// Typed unpack of kEvtNet_* event payloads written by
// NetworkReplayRecorderAdapter. Reads from a foundation-level IReplayPlayer
// (FileReplayPlayer from AYReplay) and produces structured DecodedEvent
// values that the bridge in R6.5-3 can feed straight into the live
// AYNetworkSubSystem seams (ReplicationManager / RpcHandler / PredictionManager).
//
// Why a separate decoder (vs. folding into the bridge):
//   - Pure unit-testable without subsystem coupling.
//   - Defines the wire-format contract that R7+ tooling (replay inspector,
//     headless replay engine) can rely on independently of the live runtime.
//   - Keeps the bridge code short and focused on demux.

#pragma once
#include <cstdint>
#include <cstddef>
#include <map>
#include <vector>

#include <AYReplay/IReplayPlayer.h>
#include <AYNetwork/Replay/NetworkReplayTypes.h>

namespace ayt::net::replay
{

// Decoded form of one event read from the foundation player.
//
// `eventType` is the kEvtNet_* discriminator (the foundation layer
// reports it verbatim). The tagged payload is exposed via the variant
// members below; `isCheckpoint` flips for foundation checkpoints.
//
// connectionId fields are populated with the **recorded** id by default;
// the decoder consults `remap` and substitutes the live id when present.
// Missing entries fall back to literal (recorded) id and, when
// `unmappedIds` is non-null, append the literal id for diagnostics.
struct DecodedEvent
{
    ayt::replay::ReplayTick tick = 0;
    uint32_t eventType = 0;            // kEvtNet_*

    // Event payloads (mirrors NetworkReplayRecorderAdapter pack layouts).
    struct InitialFullSnapshotT {
        uint32_t connectionId = 0;
        uint8_t  frameFlags   = 0;
        std::vector<uint8_t> payload;
    } initialFullSnapshot;

    struct SpawnT {
        uint32_t connectionId = 0;
        uint32_t netId        = 0;
        uint64_t schemaHash   = 0;
        std::vector<uint8_t> payload;
    } spawn;

    struct DespawnT {
        uint32_t connectionId = 0;
        uint32_t netId        = 0;
    } despawn;

    struct DeltaSnapshotT {
        uint32_t connectionId = 0;
        uint8_t  frameFlags   = 0;
        std::vector<uint8_t> payload;
    } deltaSnapshot;

    struct InputBatchT {
        uint32_t connectionId      = 0;
        uint32_t inputSeq          = 0;
        uint32_t serverTickAtSend  = 0;
        std::vector<uint8_t> payload;
    } inputBatch;

    struct RpcBatchT {
        uint16_t messageType   = 0;
        uint32_t connectionId  = 0;
        std::vector<uint8_t> body;
    } rpcBatch;

    struct AuthorityChangeT {
        uint32_t netId        = 0;
        uint8_t  oldKind      = 0;
        uint8_t  newKind      = 0;
        uint32_t connectionId = 0;
    } authorityChange;

    // Foundation checkpoint fields (only valid when isCheckpoint == true).
    bool          isCheckpoint = false;
    uint64_t      stateHash    = 0;
    std::vector<uint8_t> snapshot;
};

// Per-replay-playback remap table: translates a recorded `connectionId`
// (captured at record time on server A) to the live id (this playback's
// server B). Keyed by the recorded id. Missing entries fall back to
// literal id.
using ConnectionIdRemap = std::map<uint32_t /*recorded*/, uint32_t /*live*/>;

class NetworkReplayEventDecoder
{
public:
    // Read the next event from `player`, type-discriminate, unpack the
    // adapter-prefix bytes, and populate `out`.
    //
    // Returns:
    //   * Ok                         — out is populated
    //   * PastEnd / IoError / Truncated — propagated from the player
    //   * BadMagic / UnsupportedVersion — propagated from the player
    //   * UnknownEventType           — adapter eventType we don't know
    //
    // `remap` is applied to every payload's `connectionId` field on output.
    // If a recorded id is missing from `remap`, the literal id is preserved
    // and `*unmappedIds` (when non-null) gets the literal appended.
    static ayt::replay::IReplayPlayer::Error decodeNext(
        ayt::replay::IReplayPlayer& player,
        DecodedEvent& out,
        const ConnectionIdRemap& remap = {},
        std::vector<uint32_t>* unmappedIds = nullptr);

    // Helper: build a remap from a captured "accept ordinal → live id"
    // table provided by the caller. R7+ may swap this for an automatic
    // translation driven by ReplicationManager's live state.
    static ConnectionIdRemap buildRemap(
        const std::vector<uint32_t>& liveConnectionIdsInAcceptOrder);
};

} // namespace ayt::net::replay