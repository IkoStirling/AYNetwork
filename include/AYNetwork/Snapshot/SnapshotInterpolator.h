#pragma once
// AYNetwork/Snapshot/SnapshotInterpolator.h - R5.0 client-side ghost
// interpolation registry.
//
// What it is
// ==========
// The interpolator is the on-receiver side of Unity NetCode's
// `SnapshotSystem`. It owns one SnapshotBuffer per registered ghost (netId
// → SnapshotBuffer) plus a per-ghost field-interpolation spec, and it
// exposes two APIs:
//
//   push(netId, serverTick, obj)
//     Called by ReplicationManager::onReceive right after a frame is
//     deserialized. The bytewise copy lands in the buffer's ring; out-of-
//     order and duplicate pushes are handled by SnapshotBuffer.
//
//   sample(netId, renderTimeSec, out)
//     Called by the game code (or by a render-system) every frame. Walks
//     the buffer's bracketing records, then blends per-field using the
//     spec captured at registerGhostKind time. Returns false if the buffer
//     is still warming up (no record, or target time is before the oldest).
//
// Why this lives at the registry level (and not on SnapshotBuffer)
// ================================================================
// SnapshotBuffer is bytewise-agnostic and can be tested without any AYReflect
// knowledge. The interpolator is the one place that joins "buffer holding
// raw bytes" with "AYReflect knows the field types" — keeping that coupling
// here means the buffer stays tiny and trivially unit-testable.
//
// Lifecycle / ownership
// =====================
// The interpolator owns its per-ghost buffers and field specs. Game code
// owns the interpolator; ReplicationManager holds a non-owning pointer
// (set via setSnapshotInterpolator) and pushes into it. Clearing or
// destroying the interpolator invalidates the manager's pointer.

#include <AYNetwork/Snapshot/SnapshotBuffer.h>
#include <AYNetwork/Snapshot/NetworkTime.h>

#include <cstdint>
#include <cstddef>
#include <unordered_map>
#include <vector>

namespace ayt::reflect { class ITypeInfo; }

namespace ayt::net
{

// Per-field spec the interpolator captured at registerGhostKind time.
// Used during sample() to decide lerp vs snap. We deliberately store
// (offset, wireTypeId, typeSize) rather than ayt::reflect::IFieldInfo*
// so the per-sample loop has no virtual dispatch.
struct FieldInterpSpec {
    uint32_t offset = 0;
    uint8_t  wireTypeId = 0;        // WireTypeId (0..15)
    uint8_t  sizeBytes  = 0;        // sizeof the field on the wire (1/2/4/8/var)
    bool     lerpable   = false;    // true for Float/Double only
};

class SnapshotInterpolator {
public:
    SnapshotInterpolator();
    ~SnapshotInterpolator();

    // ---- Time configuration ----
    // tickRate must match the authority's rate. interDelaySec is the render-
    // behind-server time; default 0.1 s matches Unity/Unreal defaults.
    void   setTickRate(double hz)        { _time.setTickRate(hz); }
    double getTickRate() const           { return _time.getTickRate(); }
    void   setInterpolationDelaySec(double d) { _time.setInterpolationDelaySec(d); }
    double getInterpolationDelaySec() const   { return _time.getInterpolationDelaySec(); }

    // ---- Per-frame clock advance ----
    // Bumps both clocks (server-side: at the configured rate; client-side:
    // by the elapsed frame delta). Game code calls this once per frame.
    void     advance(double dtSec);
    void     advanceClient(double dtSec) { _time.advanceClient(dtSec); }
    void     advanceServer(double dtSec) { _time.advance(dtSec); }

    uint32_t getServerTick() const { return _time.getServerTick(); }
    double   getClientTimeSec() const { return _time.getClientTimeSec(); }
    double   getInterpolationTimeSec() const { return _time.getInterpolationTimeSec(); }

    // ---- Ghost registry ----
    // Register a ghost kind. `recordBytes` is sizeof(T) for the ghost; the
    // interpolator walks type->getField() at registration time, picks every
    // NetReplicate field that resolves to a wire type, and builds a
    // FieldInterpSpec list.
    //
    // Safe to call multiple times for the same netId — re-registering
    // clears the buffer's history (the ghost has effectively been
    // respawned). Returns false if type is null or has no NetReplicate
    // fields.
    bool registerGhostKind(uint32_t netId,
                           size_t recordBytes,
                           const ayt::reflect::ITypeInfo* type);

    // Forget about a ghost. Called by ReplicationManager on EntityDespawn.
    void unregisterGhost(uint32_t netId);

    // ---- Data flow ----
    // Push a freshly-received snapshot into the buffer. No-op if netId is
    // not registered (a misordered frame that arrived before spawn).
    //
    // R5.1: `teleport=true` marks the new record as a snap — sample() will
    // return its bytes verbatim instead of lerping from the previous record.
    // Production callers pass the FrameHeader::isTeleport() value from the
    // wire header. Default false (normal interpolate).
    void push(uint32_t netId, uint32_t serverTick, const void* obj,
              bool teleport = false);

    // Sample the interpolated state at renderTimeSec (typically the value
    // from getInterpolationTimeSec()). Writes the per-field blended result
    // into `out` (must be >= recordBytes). Returns false if the buffer is
    // warming up or the netId is unknown.
    bool sample(uint32_t netId, double renderTimeSec, void* out) const;

    // Convenience: sample at the current interpolation time.
    bool sampleNow(uint32_t netId, void* out) const {
        return sample(netId, getInterpolationTimeSec(), out);
    }

    // ---- Diagnostics ----
    size_t ghostCount() const { return _ghosts.size(); }
    size_t recordCount(uint32_t netId) const;

    // Drop everything. Keeps the registered kinds but clears their buffers.
    void clearBuffers();
    // Drop both kinds and buffers.
    void clearAll();

    // ---- Test seam ----
    // Replace the entire NetworkTime (for tests that need deterministic
    // tick values). Production code never calls this.
    void setTimeForTesting(NetworkTime t) { _time = t; }
    const NetworkTime& timeForTesting() const { return _time; }

private:
    struct GhostEntry {
        size_t                              recordBytes = 0;
        std::vector<FieldInterpSpec>        fields;
        SnapshotBuffer                      buffer;
    };

    NetworkTime                                              _time;
    std::unordered_map<uint32_t, GhostEntry>                 _ghosts;
};

} // namespace ayt::net