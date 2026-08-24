#pragma once
// AYNetwork/Snapshot/SnapshotBuffer.h - R5.0 per-ghost ring buffer of snapshots.
//
// Design notes
// =============
// A SnapshotBuffer stores the last N snapshots of ONE replicated ghost
// type (one C++ struct / one reflection schema). The replication manager
// owns one buffer per registered netId; each push() copies the object
// bytes verbatim, so the buffer is layout-aware but not type-aware — the
// caller is responsible for passing the correct recordBytes.
//
// Wire layout & ownership:
//   - recordBytes is determined by SnapshotInterpolator at registerGhostKind
//     time. The buffer memcpy's that many bytes from `obj` into the next
//     slot; the caller must keep `obj` alive long enough to push.
//   - The buffer does NOT own type metadata. To sample, the caller passes
//     a destination buffer of the same size; the buffer fills it in.
//   - Snapshots are stored in chronological (tick-ascending) order. Out-of-
//     order pushes are inserted at the correct slot; older slots past the
//     ring's tail are dropped.
//
// Two-tier sampling API (matches the design.md §15.3 contract):
//   - sample(time, out)              : bytewise copy of the most relevant
//                                     record (lower bracket, or newest if
//                                     target is past the newest). Cheap,
//                                     field-agnostic.
//   - findBracket(time, lo, hi, a)   : locate the bracketing pair indices
//                                     + the alpha in [0,1]. Callers that
//                                     need per-field interpolation then use
//                                     readRecord(lo) / readRecord(hi) and
//                                     their own field-aware blend step.
//
// Interpolation strategy (mirrors Unity NetCode / Unreal):
//   1. Sample target = serverTimeSec (computed from serverTick / tickRate).
//   2. Bracketing pair = (t0, t1) where t0.serverTime <= target < t1.serverTime.
//   3. alpha = (target - t0.serverTime) / (t1.serverTime - t0.serverTime).
//   4. For Float/Double: per-field lerp(out = a + (b-a)*alpha).
//   5. For Int*/UInt*/Bool/String/Nested: snap to t0's value (lower bracket
//      wins). This is the Unreal/Unity convention: integer / enum state
//      never blends mid-tick, which avoids flicker.

#include <cstdint>
#include <cstddef>
#include <vector>

namespace ayt::net
{

// One snapshot record. serverTick is the unique key. objCopy is heap-
// allocated by init(recordBytes); callers do not own it directly.
//
// R5.1: `snap` is set by push(..., snap=true) to mark this record as a
// teleport. sample() treats any bracket whose upper record has snap=true
// as a snap-to-upper case (no lerp). See design §15.8.
struct SnapshotRecord {
    uint32_t serverTick = 0;
    double   serverTimeSec = 0.0;
    void*    objCopy = nullptr;     // heap-allocated, bytewise copy
    bool     valid = false;
    bool     snap = false;          // R5.1: teleport / no-lerp marker
};

class SnapshotBuffer {
public:
    // Default ring depth. 32 records at 30 Hz ≈ 1.07 s of history — enough
    // for a 100 ms interp delay plus jitter buffer.
    static constexpr size_t kDefaultCapacity = 32;
    // Sentinel returned by findBracket when the upper bound is "past
    // newest" — the caller should clamp to the newest record instead of
    // indexing into _records.
    static constexpr size_t kNoUpperBracket = static_cast<size_t>(-1);

    SnapshotBuffer();
    ~SnapshotBuffer();

    // Configure this buffer to hold records of `recordBytes` each. Resets
    // existing history. Safe to call multiple times.
    void init(size_t recordBytes, size_t capacity = kDefaultCapacity);

    // Push a new snapshot. `serverTick` is the unique key; `serverTimeSec`
    // is its float-seconds value (caller computes from serverTick / tickRate
    // so the buffer can stay tickRate-agnostic). The buffer bytewise-copies
    // recordBytes from `obj` into a freshly-allocated record slot. Out-of-
    // order pushes (older than the current tail) are inserted at the
    // correct position and DO NOT evict the newer ones. Same-tick pushes
    // overwrite in place (the wire shouldn't produce them, but if it does
    // we keep the most recent copy).
    //
    // R5.1: `snap=true` marks the new record as a teleport — sample() will
    // return its bytes verbatim instead of lerping from the previous
    // record. Used for Spawn resync, teleport, and any "do not blend this"
    // transition. Default false (normal interpolate).
    void push(uint32_t serverTick, double serverTimeSec, const void* obj,
              bool snap = false);

    // R5.1: predicate for the snap flag of the record at `idx`.
    // Returns false on out-of-range.
    bool isSnap(size_t idx) const;

    // Bytewise sample at serverTimeSec. Always copies the lower bracket
    // (or the newest if target is past the newest, or the oldest if
    // target is before the oldest). Returns false if the buffer is empty
    // OR if recordBytes is 0 (init() not called). No field knowledge is
    // used; this is a pure memcpy helper.
    bool sample(double serverTimeSec, void* out) const;

    // Locate bracketing records. Returns:
    //   - false (and lo/hi/alpha zeroed) if size() == 0
    //   - (size()-1, kNoUpperBracket, 0.0) if target >= newest (caller
    //     holds the newest — no upper pair available)
    //   - (0, 0, 0.0) if target <= oldest (caller is still warming up)
    //   - (lo, hi, [0,1]) for a normal bracketing pair
    //
    // The returned indices are pure timing brackets — the snap flag (if
    // any) on records[hi] is reported via isSnap(hi). Callers that want
    // teleport-snap behaviour must check the flag themselves after the
    // call. This keeps findBracket() orthogonal to teleport semantics.
    bool findBracket(double serverTimeSec,
                     size_t& loIdx, size_t& hiIdx, double& alpha) const;

    // Copy the bytes of record idx into the caller-provided buffer of
    // size >= recordBytes. Returns false if idx is out of range.
    bool readRecord(size_t idx, void* out) const;

    // Drop all records (heap frees the per-record objCopy allocations).
    void clear();

    // Diagnostics.
    size_t    size() const { return _records.size(); }
    size_t    capacity() const { return _capacity; }
    size_t    recordBytes() const { return _recordBytes; }
    uint32_t  tickAt(size_t idx) const;
    double    timeAt(size_t idx) const;
    uint32_t  oldestTick() const;
    uint32_t  newestTick() const;

private:
    size_t                      _recordBytes = 0;
    size_t                      _capacity    = kDefaultCapacity;
    std::vector<SnapshotRecord> _records;     // ascending by tick

    // Allocate a fresh objCopy buffer of size recordBytes (caller fills).
    void* allocateRecord();
};

} // namespace ayt::net