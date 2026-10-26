#pragma once

#include <AYNetwork/Replication/SharedObjectAuthority.h>

namespace ayt::net {

struct SharedObjectSnapshotPage {
    std::uint64_t epoch = 0;
    std::uint64_t snapshotId = 0;
    std::uint16_t index = 0;
    std::uint16_t count = 0;
    std::optional<SharedObjectState> state; // Empty only for the one-page empty snapshot.
};

struct SharedObjectDelta {
    std::uint64_t epoch = 0;
    SharedObjectState state;
};

/// Versioned, little-endian application bodies for a host-authenticated,
/// reliable network channel. The outer PacketHeader and transport identity are
/// owned by the host. A snapshot has one bounded object per page; page count
/// therefore never exceeds SharedObjectAuthority::MaxObjects.
class SharedObjectWireCodec {
public:
    static constexpr std::size_t MaxBodyBytes = 8 + 8 + 8 + 2 + 2 + 1 +
        8 + 4 + 8 + 1 + 2 + SharedObjectAuthority::MaxStateBytes;

    /// Encode a snapshot page without changing out on failure.
    static bool encodeSnapshotPage(const SharedObjectSnapshotPage& page,
                                   std::vector<std::uint8_t>& out) {
        if (!validPage(page)) return false;
        try {
            std::vector<std::uint8_t> next;
            next.reserve(29 + (page.state ? 23 + page.state->bytes.size() : 0));
            prefix(next, 1);
            put(next, page.epoch, 8);
            put(next, page.snapshotId, 8);
            put(next, page.index, 2);
            put(next, page.count, 2);
            next.push_back(page.state ? 1 : 0);
            if (page.state) putState(next, *page.state);
            out.swap(next);
            return true;
        } catch (const std::bad_alloc&) { return false; }
    }

    /// Decode an entire body; reject truncation, trailing bytes, unsupported
    /// versions and noncanonical tombstones before publishing the result.
    static bool decodeSnapshotPage(const std::uint8_t* data, std::size_t size,
                                   SharedObjectSnapshotPage& out) {
        if (!data || size > MaxBodyBytes) return false;
        std::size_t at = 0;
        if (!readPrefix(data, size, at, 1)) return false;
        SharedObjectSnapshotPage next;
        std::uint64_t value = 0;
        if (!take(data, size, at, 8, next.epoch) ||
            !take(data, size, at, 8, next.snapshotId) ||
            !take(data, size, at, 2, value)) return false;
        next.index = static_cast<std::uint16_t>(value);
        if (!take(data, size, at, 2, value)) return false;
        next.count = static_cast<std::uint16_t>(value);
        if (!take(data, size, at, 1, value) || value > 1) return false;
        if (value) {
            SharedObjectState state;
            if (!takeState(data, size, at, state)) return false;
            next.state = std::move(state);
        }
        if (at != size || !validPage(next)) return false;
        out = std::move(next);
        return true;
    }

    static bool encodeDelta(const SharedObjectDelta& delta,
                            std::vector<std::uint8_t>& out) {
        if (!delta.epoch || !validState(delta.state)) return false;
        try {
            std::vector<std::uint8_t> next;
            next.reserve(16 + 23 + delta.state.bytes.size());
            prefix(next, 2);
            put(next, delta.epoch, 8);
            putState(next, delta.state);
            out.swap(next);
            return true;
        } catch (const std::bad_alloc&) { return false; }
    }

    static bool decodeDelta(const std::uint8_t* data, std::size_t size,
                            SharedObjectDelta& out) {
        if (!data || size > MaxBodyBytes) return false;
        std::size_t at = 0;
        if (!readPrefix(data, size, at, 2)) return false;
        SharedObjectDelta next;
        if (!take(data, size, at, 8, next.epoch) || !next.epoch ||
            !takeState(data, size, at, next.state) || at != size) return false;
        out = std::move(next);
        return true;
    }

    static bool validPage(const SharedObjectSnapshotPage& page) noexcept {
        return page.epoch && page.snapshotId && page.count &&
            page.count <= SharedObjectAuthority::MaxObjects && page.index < page.count &&
            (page.state ? validState(*page.state) :
                          (page.count == 1 && page.index == 0));
    }

private:
    static bool validState(const SharedObjectState& state) noexcept {
        return state.objectId && state.revision &&
            state.bytes.size() <= SharedObjectAuthority::MaxStateBytes &&
            (!state.retired || (!state.ownerConnection && state.bytes.empty()));
    }
    static void put(std::vector<std::uint8_t>& bytes, std::uint64_t value,
                    unsigned width) {
        for (unsigned i = 0; i < width; ++i)
            bytes.push_back(static_cast<std::uint8_t>(value >> (i * 8)));
    }
    static bool take(const std::uint8_t* data, std::size_t size, std::size_t& at,
                     unsigned width, std::uint64_t& value) noexcept {
        if (at > size || width > size - at) return false;
        value = 0;
        for (unsigned i = 0; i < width; ++i)
            value |= static_cast<std::uint64_t>(data[at++]) << (i * 8);
        return true;
    }
    static void prefix(std::vector<std::uint8_t>& bytes, std::uint8_t kind) {
        bytes.insert(bytes.end(), {'A', 'Y', 'S', 'O', 1, kind, 0, 0});
    }
    static bool readPrefix(const std::uint8_t* data, std::size_t size,
                           std::size_t& at, std::uint8_t kind) noexcept {
        if (size < 8 || data[0] != 'A' || data[1] != 'Y' || data[2] != 'S' ||
            data[3] != 'O' || data[4] != 1 || data[5] != kind ||
            data[6] || data[7]) return false;
        at = 8;
        return true;
    }
    static void putState(std::vector<std::uint8_t>& out,
                         const SharedObjectState& state) {
        put(out, state.objectId, 8);
        put(out, state.ownerConnection, 4);
        put(out, state.revision, 8);
        out.push_back(state.retired ? 1 : 0);
        put(out, state.bytes.size(), 2);
        out.insert(out.end(), state.bytes.begin(), state.bytes.end());
    }
    static bool takeState(const std::uint8_t* data, std::size_t size,
                          std::size_t& at, SharedObjectState& out) {
        std::uint64_t value = 0;
        if (!take(data, size, at, 8, out.objectId) ||
            !take(data, size, at, 4, value)) return false;
        out.ownerConnection = static_cast<std::uint32_t>(value);
        if (!take(data, size, at, 8, out.revision) ||
            !take(data, size, at, 1, value) || value > 1) return false;
        out.retired = value != 0;
        if (!take(data, size, at, 2, value) ||
            value > SharedObjectAuthority::MaxStateBytes ||
            at > size || value > size - at) return false;
        try {
            out.bytes.assign(data + at, data + at + value);
        } catch (const std::bad_alloc&) { return false; }
        at += static_cast<std::size_t>(value);
        return validState(out);
    }
};

} // namespace ayt::net
