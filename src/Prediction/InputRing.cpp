// AYNetwork/Prediction/InputRing.cpp - R5.2 per-connection client input ring.

#include <AYNetwork/Prediction/InputRing.h>

namespace ayt::net
{

bool InputRing::push(ClientInputRecord&& rec)
{
    // Reject non-monotonic arrivals (out-of-order packet replay).
    if (_count > 0 && !seqGreaterThan(rec.inputSeq, newestSeq())) {
        return false;
    }

    if (_count < _capacity) {
        // Room available: write at _tail, advance tail.
        const uint32_t slot = _tail;
        _slots[slot] = std::move(rec);
        _tail = (_tail + 1u) % _capacity;
        ++_count;
    } else {
        // Full: overwrite _head (oldest live), advance both head and tail.
        const uint32_t slot = _head;
        _slots[slot] = std::move(rec);
        _head = (_head + 1u) % _capacity;
        _tail = (_tail + 1u) % _capacity;
        // _count stays at _capacity.
        // The acked cursor must never fall below the oldest live seq,
        // otherwise the ring would expose records marked "already
        // consumed". Bump forward if needed.
        const uint32_t oldest = oldestLiveSeq();
        if (seqGreaterThan(oldest, _ackedSeq)) {
            _ackedSeq = oldest;
        }
    }
    return true;
}

bool InputRing::tryGet(uint32_t inputSeq, ClientInputRecord& out) const
{
    if (_count == 0) return false;
    const uint32_t lo = oldestLiveSeq();
    const uint32_t hi = newestSeq();
    // Inclusive range check using wraparound-aware modulo math.
    // "seq >= lo" in circular sense: !(lo > seq) i.e. !seqGreaterThan(lo, seq).
    // "seq <= hi" in circular sense: !(seq > hi) i.e. !seqGreaterThan(seq, hi).
    const bool geLo = !seqGreaterThan(lo, inputSeq);
    const bool leHi = !seqGreaterThan(inputSeq, hi);
    if (!(geLo && leHi)) return false;

    // Walk forward from _head (oldest live) through _count slots. For
    // capacity 32 a linear scan is trivial; no hash index needed.
    for (uint32_t i = 0; i < _count; ++i) {
        const uint32_t idx = (_head + i) % _capacity;
        const ClientInputRecord& r = _slots[idx];
        if (r.inputSeq == inputSeq) {
            out = r; // vector assignment (copy) keeps _slots immutable
            return true;
        }
    }
    return false;
}

void InputRing::ackUpTo(uint32_t seqInclusive)
{
    // Forward-only. The acked cursor is "next-not-yet-acked seq", so
    // ackUpTo(N) sets it to N+1 (the next seq we'd need to ack).
    const uint32_t next = seqInclusive + 1u;
    if (seqGreaterThan(next, _ackedSeq)) {
        _ackedSeq = next;
    }
}

uint32_t InputRing::newestSeq() const
{
    if (_count == 0) return 0;
    // newest lives at the slot just before _tail (when not full)
    // OR just before _head (when full). Both cases: the most-recently-
    // pushed slot.
    const uint32_t newestIdx = (_tail + _capacity - 1u) % _capacity;
    return _slots[newestIdx].inputSeq;
}

uint32_t InputRing::oldestLiveSeq() const
{
    if (_count == 0) return 0;
    return _slots[_head].inputSeq;
}

} // namespace ayt::net
