#pragma once
// AYNetwork/Prediction/PredictionManager.h - R5.2 prediction orchestrator.
//
// Design notes
// =============
// Centralizes R5.2 client-prediction / server-reconciliation state. The
// manager is main-thread only (no mutexes). It owns:
//   - One InputRing per known connection (the seq counter is per-conn).
//   - One PredictedGhost per AutonomousProxy ghost (the predicted-state
//     copy + last-predicted-for-seq tracker).
//
// Public surface is split into two halves:
//   - onClientInput / pendingInputCount — caller is AYNetworkSubSystem on
//     the SERVER side, after dispatching kMsgTypeClientInput.
//   - predict / onServerAck / tryGetPredictedState — caller is the CLIENT
//     side, driven by the phased GameLoop (FixedPrePhysics).
//   - consumeClientInputs is server-side and invoked once per fixed tick
//     BEFORE drainSimulationInbound.
//
// Authority model
// ---------------
// Server does NOT rewind. Client owns smoothing. See MispredictionResolver
// for the per-field reconcile function.

#include <AYNetwork/Prediction/InputRing.h>
#include <AYNetwork/INetwork.h>

#include <cstdint>
#include <cstddef>
#include <map>
#include <vector>
#include <functional>

namespace ayt::net
{

// Per-ghost predicted state. Predicted bytes are stored as a flat byte
// buffer (game-decoded opaque to the library) alongside a layout-hash
// the MispredictionResolver uses to bail to snap-on-mismatch.
struct PredictedGhost {
    uint32_t netId = 0;
    uint32_t lastPredictedForInputSeq = 0; // last seq used to advance prediction
    uint32_t lastAckedInputTick = 0;        // last ack the server confirmed
    std::vector<uint8_t> bytes;             // opaque predicted-state copy
    uint64_t layoutHash = 0;                // see setLayoutHash
};

// Callback shape for the gameplay-side input application hook. Passed
// PER-CALL to consumeClientInputs so the binding lifetime stays with the
// application (no manager-owned std::function on the hot path).
// The callback receives connectionId, inputSeq, payload, payloadSize.
// The application resolves connectionId → owned ghost via its own
// authority map (typically setObjectProxyKind in reverse).
using ApplyInputFn = std::function<void(uint32_t connectionId,
                                        uint32_t inputSeq,
                                        const uint8_t* payload,
                                        size_t payloadSize)>;

class PredictionManager {
public:
    explicit PredictionManager(uint32_t ringCapacity = 32)
        : _ringCapacity(ringCapacity > 0 ? ringCapacity : 1u)
    {}

    // Re-configure the ring capacity for FUTURE connections (existing
    // rings keep their current cap). Used by tests to exercise overflow
    // without changing the default.
    void setRingCapacity(uint32_t cap);

    // ---- SERVER SIDE ----

    // Called once per received kMsgTypeClientInput. Validates the wire
    // envelope via ClientInputCodec, pushes into the per-connection ring.
    // Returns true on success (record accepted), false on wire-format
    // error or duplicate seq.
    bool onClientInput(uint32_t connectionId,
                       const uint8_t* body, size_t bodySize);

    // Drain queued inputs for every known connection, invoking `apply`
    // once per record in seq order. `simTick` is the current server
    // fixed tick; recorded for telemetry / future deterministic ordering.
    // No-op when `apply` is empty.
    void consumeClientInputs(uint32_t simTick, const ApplyInputFn& apply);

    // After processing a Full Snapshot, advance the per-connection acked
    // cursor so subsequent consumeClientInputs doesn't re-emit. Called
    // by ReplicationManager after the ack tail is parsed.
    void markAcked(uint32_t connectionId, uint32_t lastAckedInputTick);

    // Read-only test seams.
    uint32_t lastAckedInputTick(uint32_t connectionId) const;
    size_t   pendingInputCount(uint32_t connectionId) const;
    size_t   trackedConnections() const { return _rings.size(); }

    // ---- CLIENT SIDE ----

    // Per-fixed-tick: apply `step(netId, payload, dtSec)` to each
    // AutonomousProxy ghost the local client owns. The caller supplies
    // the gameplay-side prediction step (read inputs, integrate, write
    // back into the predicted copy). `dtSec` is the fixed dt.
    using PredictFn = std::function<void(uint32_t netId,
                                         const uint8_t* payload,
                                         size_t payloadSize,
                                         float dtSec)>;
    // Drives one prediction pass. Inputs is the union of all unacked
    // records in the local ring; step is called once per record. Caller
    // MUST NOT mutate the ring during this call.
    void predict(uint32_t netId,
                 const std::vector<ClientInputRecord>& inputs,
                 const PredictFn& step,
                 float dtSec);

    // After receiving a Full Snapshot ack tail, update lastAckedInputTick
    // for the ghost and expose the value via getLastAckedInputTick (test).
    void onServerAck(uint32_t netId,
                     uint32_t lastAckedInputTick,
                     uint32_t serverCommandAge);

    // ---- Ghost registry (shared between server and client sides) ----
    //
    // Server uses it to know which ghosts the client owns. Client uses
    // it to look up its own predicted-state copy. The two sides
    // maintain independent PredictionManager instances; this is just the
    // local registry for whichever side owns this manager.

    void   registerPredictedGhost(uint32_t netId, uint64_t layoutHash = 0);
    void   unregisterPredictedGhost(uint32_t netId);
    bool   isPredictedGhost(uint32_t netId) const;
    void   setPredictedBytes(uint32_t netId, const uint8_t* data, size_t n);
    bool   tryGetPredictedBytes(uint32_t netId, std::vector<uint8_t>& out) const;
    uint64_t getLayoutHash(uint32_t netId) const;
    void   setLayoutHash(uint32_t netId, uint64_t h);
    uint32_t lastPredictedInputSeq(uint32_t netId) const;

private:
    uint32_t _ringCapacity;
    // R6 C2 (2026-08-25): std::map (was std::unordered_map) so iteration
    // order is deterministic — sorted by connectionId / netId. Same
    // [k]/find/erase/range-for API; copy/move semantics unchanged.
    std::map<uint32_t, InputRing>      _rings;    // per-connection
    std::map<uint32_t, uint32_t>       _ackedSeq; // per-connection last-acked
    std::map<uint32_t, PredictedGhost> _ghosts;   // per-netId
};

} // namespace ayt::net
