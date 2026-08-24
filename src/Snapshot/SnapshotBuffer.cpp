// SnapshotBuffer.cpp - R5.0 ring buffer of bytewise-copied snapshots.
//
// Implementation notes:
//   * Records are sorted ascending by serverTick. Push performs a linear
//     insertion because the wire is normally monotonic and N is small
//     (≤kDefaultCapacity); a sorted vector is friendlier to readRecord +
//     findBracket than a true ring.
//   * When capacity is exceeded we drop the oldest entry (FIFO eviction).
//     We never refuse a push.
//   * Same-tick pushes overwrite in place. The authority shouldn't emit
//     duplicates, but if retransmits collapse onto the same tick we keep
//     the latest copy rather than growing the ring with stale frames.

#include <AYNetwork/Snapshot/SnapshotBuffer.h>

#include <algorithm>
#include <cstring>
#include <new>

namespace ayt::net
{

SnapshotBuffer::SnapshotBuffer() = default;

SnapshotBuffer::~SnapshotBuffer() {
    clear();
}

void* SnapshotBuffer::allocateRecord() {
    if (_recordBytes == 0) return nullptr;
    // operator new returns raw memory; the buffer is treated as bytes
    // (the actual struct layout is opaque to us).
    return ::operator new(_recordBytes, std::nothrow);
}

void SnapshotBuffer::init(size_t recordBytes, size_t capacity) {
    clear();
    _recordBytes = recordBytes;
    _capacity    = (capacity > 0) ? capacity : kDefaultCapacity;
}

void SnapshotBuffer::clear() {
    for (auto& r : _records) {
        if (r.objCopy) {
            ::operator delete(r.objCopy);
            r.objCopy = nullptr;
        }
        r.valid = false;
    }
    _records.clear();
}

void SnapshotBuffer::push(uint32_t serverTick, double serverTimeSec, const void* obj,
                            bool snap) {
    if (_recordBytes == 0 || !obj) return;

    // Same-tick dedup: scan ascending; if any existing record matches the
    // tick, overwrite it in place. The new snap flag replaces the old one —
    // if the wire sends two records on the same tick (one teleport, one
    // not), the latest wins. The wire shouldn't produce this in practice
    // because the authority emits exactly one frame per tick per ghost.
    for (auto& r : _records) {
        if (r.valid && r.serverTick == serverTick) {
            std::memcpy(r.objCopy, obj, _recordBytes);
            r.serverTimeSec = serverTimeSec;
            r.snap          = snap;
            return;
        }
    }

    SnapshotRecord nr;
    nr.serverTick    = serverTick;
    nr.serverTimeSec = serverTimeSec;
    nr.objCopy       = allocateRecord();
    nr.valid         = false;
    nr.snap          = snap;
    if (!nr.objCopy) return;          // allocation failure — drop the push

    std::memcpy(nr.objCopy, obj, _recordBytes);
    nr.valid = true;

    // Locate insertion point (records are ascending).
    auto it = std::lower_bound(_records.begin(), _records.end(), serverTick,
        [](const SnapshotRecord& r, uint32_t t) { return r.serverTick < t; });

    _records.insert(it, nr);

    // Enforce capacity: drop oldest if over.
    while (_records.size() > _capacity) {
        SnapshotRecord& oldest = _records.front();
        if (oldest.objCopy) ::operator delete(oldest.objCopy);
        _records.erase(_records.begin());
    }
}

bool SnapshotBuffer::isSnap(size_t idx) const {
    if (idx == kNoUpperBracket) return false;
    if (idx >= _records.size()) return false;
    return _records[idx].snap;
}

bool SnapshotBuffer::sample(double serverTimeSec, void* out) const {
    if (_recordBytes == 0 || _records.empty() || !out) return false;
    size_t lo, hi;
    double alpha;
    const bool found = findBracket(serverTimeSec, lo, hi, alpha);
    if (!found) {
        // Buffer is empty — caller should have returned earlier.
        return false;
    }
    // Copy the lower bracket. If hi == kNoUpperBracket (past newest),
    // alpha was set to 0 and lo is the newest — same copy either way.
    return readRecord(lo, out);
}

bool SnapshotBuffer::findBracket(double serverTimeSec,
                                 size_t& loIdx, size_t& hiIdx, double& alpha) const {
    loIdx = hiIdx = 0;
    alpha = 0.0;
    if (_records.empty()) return false;

    const size_t n = _records.size();
    const double first = _records.front().serverTimeSec;
    const double last  = _records.back().serverTimeSec;

    if (serverTimeSec <= first) {
        loIdx = hiIdx = 0;
        alpha = 0.0;
        return true;        // warmup — caller should hold the oldest
    }
    if (serverTimeSec >= last) {
        loIdx = n - 1;
        hiIdx = kNoUpperBracket;
        alpha = 0.0;
        return true;        // past newest — caller should hold the newest
    }

    // Find first record strictly > target, then step back one to get lo.
    // Linear scan; n ≤ 32 in practice.
    size_t upper = 0;
    for (size_t i = 0; i < n; ++i) {
        if (_records[i].serverTimeSec > serverTimeSec) { upper = i; break; }
    }
    if (upper == 0) {
        // serverTimeSec == first exactly. Lower = first, upper = first+1.
        loIdx = 0;
        hiIdx = (n > 1) ? 1 : kNoUpperBracket;
        alpha = 0.0;
        return true;
    }
    loIdx = upper - 1;
    hiIdx = upper;
    const double t0 = _records[loIdx].serverTimeSec;
    const double t1 = _records[hiIdx].serverTimeSec;
    alpha = (t1 > t0) ? (serverTimeSec - t0) / (t1 - t0) : 0.0;
    if (alpha < 0.0) alpha = 0.0;
    if (alpha > 1.0) alpha = 1.0;
    return true;
}

bool SnapshotBuffer::readRecord(size_t idx, void* out) const {
    if (idx == kNoUpperBracket) return false;
    if (idx >= _records.size()) return false;
    const auto& r = _records[idx];
    if (!r.valid || !r.objCopy || !out) return false;
    std::memcpy(out, r.objCopy, _recordBytes);
    return true;
}

uint32_t SnapshotBuffer::tickAt(size_t idx) const {
    if (idx >= _records.size()) return 0;
    return _records[idx].serverTick;
}

double SnapshotBuffer::timeAt(size_t idx) const {
    if (idx >= _records.size()) return 0.0;
    return _records[idx].serverTimeSec;
}

uint32_t SnapshotBuffer::oldestTick() const {
    return _records.empty() ? 0u : _records.front().serverTick;
}

uint32_t SnapshotBuffer::newestTick() const {
    return _records.empty() ? 0u : _records.back().serverTick;
}

} // namespace ayt::net