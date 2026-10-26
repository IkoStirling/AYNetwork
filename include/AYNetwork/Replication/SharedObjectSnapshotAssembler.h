#pragma once

#include <AYNetwork/Replication/SharedObjectReplica.h>
#include <AYNetwork/Replication/SharedObjectWireCodec.h>

namespace ayt::net {

enum class SharedSnapshotReceiveStatus {
    Pending, Replayed, Applied, Invalid, Stale, Capacity
};

/// Owner-thread staging for reliable, possibly reordered snapshot pages.
/// No page mutates the live replica until every page of the same snapshot is
/// present and the complete object set passes replica validation. The host
/// authenticates the sender, requests missing pages and calls clear on leave.
class SharedObjectSnapshotAssembler {
public:
    SharedSnapshotReceiveStatus accept(const SharedObjectSnapshotPage& page,
                                       SharedObjectReplica& replica) {
        if (!SharedObjectWireCodec::validPage(page))
            return SharedSnapshotReceiveStatus::Invalid;
        if (_completedEpoch && (page.epoch < _completedEpoch ||
            (page.epoch == _completedEpoch && page.snapshotId <= _completedId)))
            return SharedSnapshotReceiveStatus::Stale;
        if (_pendingEpoch && (page.epoch < _pendingEpoch ||
            (page.epoch == _pendingEpoch && page.snapshotId < _pendingId)))
            return SharedSnapshotReceiveStatus::Stale;
        if (!_pendingEpoch || page.epoch > _pendingEpoch ||
            page.snapshotId > _pendingId) {
            try {
                std::vector<std::optional<SharedObjectState>> next(page.count);
                _pages.swap(next);
            } catch (const std::bad_alloc&) {
                return SharedSnapshotReceiveStatus::Capacity;
            }
            _pendingEpoch = page.epoch;
            _pendingId = page.snapshotId;
            _received = 0;
            _empty = false;
        }
        if (page.count != _pages.size()) return SharedSnapshotReceiveStatus::Invalid;
        auto& slot = _pages[page.index];
        if (_empty && page.state) return SharedSnapshotReceiveStatus::Invalid;
        if (slot || (_empty && !page.state)) {
            return same(slot, page.state)
                ? SharedSnapshotReceiveStatus::Replayed
                : SharedSnapshotReceiveStatus::Invalid;
        }
        if (!page.state) {
            _empty = true;
        } else {
            try { slot = *page.state; }
            catch (const std::bad_alloc&) { return SharedSnapshotReceiveStatus::Capacity; }
        }
        ++_received;
        if (_received != _pages.size()) return SharedSnapshotReceiveStatus::Pending;

        std::vector<SharedObjectState> states;
        try {
            states.reserve(_empty ? 0 : _pages.size());
            if (!_empty)
                for (const auto& entry : _pages) states.push_back(*entry);
        } catch (const std::bad_alloc&) {
            dropPending();
            return SharedSnapshotReceiveStatus::Capacity;
        }
        const auto result = replica.installSnapshot(_pendingEpoch, std::move(states));
        if (result == SharedReplicaStatus::Capacity) {
            dropPending();
            return SharedSnapshotReceiveStatus::Capacity;
        }
        if (result == SharedReplicaStatus::Applied) {
            _completedEpoch = _pendingEpoch;
            _completedId = _pendingId;
        }
        dropPending();
        if (result == SharedReplicaStatus::Applied)
            return SharedSnapshotReceiveStatus::Applied;
        return result == SharedReplicaStatus::Stale
            ? SharedSnapshotReceiveStatus::Stale
            : SharedSnapshotReceiveStatus::Invalid;
    }

    void clear() noexcept {
        _pages.clear();
        _pendingEpoch = _pendingId = _completedEpoch = _completedId = 0;
        _received = 0;
        _empty = false;
    }
    std::size_t pendingPages() const noexcept { return _received; }
    std::size_t expectedPages() const noexcept { return _pages.size(); }

private:
    void dropPending() noexcept {
        _pages.clear();
        _pendingEpoch = _pendingId = 0;
        _received = 0;
        _empty = false;
    }
    static bool same(const std::optional<SharedObjectState>& a,
                     const std::optional<SharedObjectState>& b) noexcept {
        if (a.has_value() != b.has_value()) return false;
        if (!a) return true;
        return a->objectId == b->objectId && a->revision == b->revision &&
            a->ownerConnection == b->ownerConnection && a->retired == b->retired &&
            a->bytes == b->bytes;
    }
    std::vector<std::optional<SharedObjectState>> _pages;
    std::uint64_t _pendingEpoch = 0;
    std::uint64_t _pendingId = 0;
    std::uint64_t _completedEpoch = 0;
    std::uint64_t _completedId = 0;
    std::size_t _received = 0;
    bool _empty = false;
};

} // namespace ayt::net
