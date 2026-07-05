// SequenceNumber.cpp - Sequence number manager

#include <SequenceNumber.h>

namespace ayt::net
{

SequenceNumber::SequenceNumber() {
    reset();
}

uint32_t SequenceNumber::next() {
    return _current.fetch_add(1);
}

bool SequenceNumber::expect(uint32_t seq) const {
    uint32_t cur = _current.load();
    return seq == cur || seq == cur + 1;
}

void SequenceNumber::ack(uint32_t seq) {
    uint32_t oldHighest = _highestAcked.load();
    if (seq > oldHighest) {
        _highestAcked.store(seq);
    }

    uint32_t index = seq % 8;
    uint32_t bit = seq / 8;
    _acked[index].store(_acked[index].load() | (1u << bit));
}

bool SequenceNumber::inWindow(uint32_t seq) const {
    uint32_t cur = _current.load();
    uint32_t diff = seq - cur;
    return diff < WINDOW_SIZE;
}

void SequenceNumber::reset() {
    _current.store(0);
    _highestAcked.store(0);
    for (auto& atomic : _acked) {
        atomic.store(0);
    }
}

} // namespace ayt::net