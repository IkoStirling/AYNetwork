#pragma once
// AYNetwork/Prediction/InputRing.h - R5.2 per-connection client input ring.
//
// Design notes
// =============
// One InputRing stores the last N client input records uploaded by ONE
// connection (the client's per-connection input sequence, not per-ghost).
// Capacity is configurable (default 32, matching SnapshotBuffer). On
// overflow the oldest record is dropped (Unity NetCode convention; the
// server's ack math still progresses because we keep the monotonic seq).
//
// Seq comparison rules
// --------------------
//   - All seq comparisons use wraparound-aware modulo math:
//       newer = (int32_t)(a - b) > 0   (i.e. signed difference > 0)
//   - Raw subtraction breaks at u32 wraparound (4B ticks ≈ 4.5 years at
//     30Hz; tests would hit it sooner). Centralized in seqGreaterThan().
//   - `ackedSeq` only advances forward (no future-ack).
//
// Drop semantics
// --------------
//   - When the ring is full and a new record arrives, the OLDEST record
//     (lowest live seq) is silently overwritten. The acked cursor still
//     advances via ackUpTo(seq) when the server confirms an input.
//   - tryGet(seq) returns false for any seq outside [oldestLiveSeq,
//     newestSeq]; callers MUST handle this as "input was lost".
//
// Thread model: main-thread only. Matches the rest of AYNetwork R5.x.

#include <cstdint>
#include <cstddef>
#include <vector>

namespace ayt::net
{

// One client input record. inputSeq is the unique monotonic key per
// connection. serverTickAtSend is what the client stamped at send-time
// (useful for RTT / sanity logging; not used for sequencing). payload is
// opaque to the library — gameplay-decoded.
struct ClientInputRecord {
    uint32_t inputSeq = 0;
    uint32_t serverTickAtSend = 0;
    std::vector<uint8_t> payload;
};

// Wraparound-aware seq comparison. Centralized to make the rule reviewable.
inline bool seqGreaterThan(uint32_t a, uint32_t b) {
    // signed subtraction in u32 space: positive iff a is strictly newer
    // than b in the circular sense.
    return static_cast<int32_t>(a - b) > 0;
}

class InputRing {
public:
    explicit InputRing(uint32_t capacity = 32)
        : _capacity(capacity > 0 ? capacity : 1u)
        , _slots(_capacity)
        , _head(0)
        , _tail(0)
        , _count(0)
        , _ackedSeq(0)
    {}

    // Push a new record. Caller is responsible for monotonic seq (push
    // refuses out-of-order arrivals; returns false on seq <= newestSeq).
    // On overflow, drops the OLDEST record and refills the slot at _head.
    // Returns true on success.
    bool push(ClientInputRecord&& rec);

    // Look up by seq. Returns true and fills `out` on hit; false on miss
    // (seq < oldestLiveSeq || seq > newestSeq). out's payload vector is
    // assigned (not appended) so the caller doesn't pay for a clear().
    bool tryGet(uint32_t inputSeq, ClientInputRecord& out) const;

    // Advance the acked cursor to seqInclusive (clamped forward — never
    // regresses). The acked cursor is the next-not-yet-acked seq.
    void ackUpTo(uint32_t seqInclusive);

    // NEWEST seq ever pushed, or 0 if the ring is empty.
    uint32_t newestSeq() const;

    // OLDEST live seq (lowest seq in the ring), or 0 if empty.
    // After drop-oldest, this returns the seq AFTER the dropped slot.
    uint32_t oldestLiveSeq() const;

    // The seq the server has confirmed (i.e. tryGet(seq) for any
    // seq > ackedSeq might succeed; seq <= ackedSeq is considered
    // ack-consumed and may be evicted). Starts at 0.
    uint32_t ackedSeq() const { return _ackedSeq; }

    size_t   size()     const { return _count; }
    uint32_t capacity() const { return _capacity; }
    bool     empty()    const { return _count == 0; }

    // Count of records in the ring whose seq is STRICTLY GREATER than
    // ackedSeq (i.e. not yet consumed). Used by PredictionManager as the
    // "pendingInputCount" surface. Returns 0 when the ring is empty.
    // (Counted in linear seq distance, not buffer slots — drop-oldest
    // overflow doesn't change the answer because dropped slots were
    // already acked.)
    uint32_t pendingCount() const {
        if (_count == 0) return 0u;
        const uint32_t lo = oldestLiveSeq();
        const uint32_t hi = newestSeq();
        // Distance = (hi - lo + 1) using wraparound-aware subtraction.
        // Then subtract already-acked from below if ackedSeq >= lo.
        const uint32_t total = static_cast<uint32_t>(
            static_cast<int32_t>(hi - lo) + 1);
        if (seqGreaterThan(_ackedSeq, lo)) {
            // Everything <= _ackedSeq is consumed.
            const uint32_t done = static_cast<uint32_t>(
                static_cast<int32_t>(_ackedSeq - lo));
            return done < total ? total - done : 0u;
        }
        return total;
    }

private:
    uint32_t _capacity;
    std::vector<ClientInputRecord> _slots;
    uint32_t _head;     // index of the next slot to WRITE (== oldest when full)
    uint32_t _tail;     // index of the next slot to READ (= oldest when not full)
    uint32_t _count;    // current # of valid records
    uint32_t _ackedSeq; // acked cursor (next-not-yet-acked seq)
};

} // namespace ayt::net
