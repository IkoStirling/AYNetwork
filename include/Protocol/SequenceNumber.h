#pragma once
// SequenceNumber.h - Sequence number manager for reliable delivery

#include <AYCore.h>
#include <cstdint>
#include <atomic>

namespace ayt::net
{

// =============================================================================
// SequenceNumber - Manages packet sequence numbers
// =============================================================================
class SequenceNumber {
public:
    SequenceNumber();

    // Get next sequence number
    uint32_t next();

    // Get current sequence number
    uint32_t current() const { return _current.load(); }

    // Check if a sequence number is what we expect
    bool expect(uint32_t seq) const;

    // Acknowledge a sequence number
    void ack(uint32_t seq);

    // Get highest acked sequence
    uint32_t highestAcked() const { return _highestAcked.load(); }

    // Check if sequence is in our window
    bool inWindow(uint32_t seq) const;

    // Reset
    void reset();

private:
    static constexpr uint32_t WINDOW_SIZE = 256;

    std::atomic<uint32_t> _current{0};
    std::atomic<uint32_t> _highestAcked{0};
    std::atomic<uint32_t> _acked[8] = {};
};

} // namespace ayt::net