#pragma once

#include <cstddef>
#include <cstdint>
#include <deque>
#include <map>
#include <optional>
#include <utility>
#include <vector>

namespace ayt::net {

struct SharedObjectState {
    std::uint64_t objectId = 0;
    std::uint32_t ownerConnection = 0; // 0 is authority-owned/unclaimed.
    std::uint64_t revision = 0;
    std::vector<std::uint8_t> bytes;
};

enum class SharedObjectAction { Update, Claim, Transfer, Release };
struct SharedObjectCommand {
    std::uint64_t epoch = 0;
    std::uint32_t actorConnection = 0; // 0 is the authoritative server.
    std::uint64_t requestId = 0;
    std::uint64_t objectId = 0;
    std::uint64_t expectedRevision = 0;
    SharedObjectAction action = SharedObjectAction::Update;
    std::uint32_t newOwnerConnection = 0;
    std::vector<std::uint8_t> bytes;
    bool operator==(const SharedObjectCommand& other) const noexcept {
        return epoch == other.epoch && actorConnection == other.actorConnection &&
            requestId == other.requestId && objectId == other.objectId &&
            expectedRevision == other.expectedRevision && action == other.action &&
            newOwnerConnection == other.newOwnerConnection && bytes == other.bytes;
    }
};

enum class SharedObjectStatus {
    Applied, Replayed, Invalid, WrongEpoch, Missing, RevisionConflict,
    NotOwner, Capacity
};
struct SharedObjectResult {
    SharedObjectStatus status = SharedObjectStatus::Invalid;
    std::uint64_t revision = 0;
    std::uint32_t ownerConnection = 0;
};

/// Authority-owned object state for late join and compare-and-swap ownership.
/// The server maps authenticated connections to actorConnection; the registry
/// does not authenticate network packets or interpret gameplay state bytes.
class SharedObjectAuthority {
public:
    static constexpr std::size_t MaxObjects = 2048;
    static constexpr std::size_t MaxStateBytes = 16 * 1024;
    static constexpr std::size_t MaxReceipts = 1024;

    /// New authority epoch preserves object state and invalidates old commands
    /// and request receipts. The epoch must increase monotonically.
    bool beginEpoch(std::uint64_t epoch) {
        if (!epoch || epoch <= _epoch) return false;
        _epoch = epoch;
        _receipts.clear();
        return true;
    }
    std::uint64_t epoch() const noexcept { return _epoch; }

    /// Authority creates a stable object identity before replication.
    bool publish(std::uint64_t objectId, std::uint32_t ownerConnection,
                 std::vector<std::uint8_t> bytes) {
        if (!_epoch || !objectId || bytes.size() > MaxStateBytes ||
            _objects.size() >= MaxObjects || _objects.count(objectId)) return false;
        _objects.emplace(objectId,
                         SharedObjectState{objectId, ownerConnection, 1, std::move(bytes)});
        return true;
    }

    std::optional<SharedObjectState> get(std::uint64_t objectId) const {
        const auto it = _objects.find(objectId);
        if (it == _objects.end()) return std::nullopt;
        return it->second;
    }

    /// Sorted full state for late join/rejoin; caller sends it over its chosen
    /// reliable replication channel and applies newer revisions only.
    std::vector<SharedObjectState> snapshot() const {
        std::vector<SharedObjectState> result;
        result.reserve(_objects.size());
        for (const auto& [id, state] : _objects) result.push_back(state);
        return result;
    }

    /// One successful request consumes exactly one revision. Repeating the
    /// same actor/request ID within the bounded receipt window is idempotent.
    SharedObjectResult apply(const SharedObjectCommand& command) {
        if (!command.requestId || !command.objectId || command.epoch != _epoch ||
            !_epoch || command.bytes.size() > MaxStateBytes) {
            return {command.epoch != _epoch ? SharedObjectStatus::WrongEpoch
                                            : SharedObjectStatus::Invalid, 0, 0};
        }
        const auto key = std::make_pair(command.actorConnection, command.requestId);
        for (const auto& receipt : _receipts) {
            if (receipt.key != key) continue;
            if (!(receipt.command == command)) return {SharedObjectStatus::Invalid, 0, 0};
            return {SharedObjectStatus::Replayed, receipt.revision,
                    receipt.ownerConnection};
        }
        const auto it = _objects.find(command.objectId);
        if (it == _objects.end()) return {SharedObjectStatus::Missing, 0, 0};
        auto& state = it->second;
        if (state.revision != command.expectedRevision)
            return {SharedObjectStatus::RevisionConflict, state.revision,
                    state.ownerConnection};
        if (state.revision == UINT64_MAX)
            return {SharedObjectStatus::Capacity, state.revision, state.ownerConnection};
        if (command.action == SharedObjectAction::Claim) {
            if (!command.actorConnection || command.newOwnerConnection ||
                !command.bytes.empty()) return {SharedObjectStatus::Invalid, 0, 0};
            if (state.ownerConnection) return {SharedObjectStatus::NotOwner, state.revision,
                                               state.ownerConnection};
            state.ownerConnection = command.actorConnection;
        } else {
            if (command.actorConnection != 0 &&
                command.actorConnection != state.ownerConnection)
                return {SharedObjectStatus::NotOwner, state.revision,
                        state.ownerConnection};
            switch (command.action) {
            case SharedObjectAction::Update:
                if (command.newOwnerConnection) return {SharedObjectStatus::Invalid, 0, 0};
                state.bytes = command.bytes;
                break;
            case SharedObjectAction::Transfer:
                if (!command.newOwnerConnection || !command.bytes.empty())
                    return {SharedObjectStatus::Invalid, 0, 0};
                state.ownerConnection = command.newOwnerConnection;
                break;
            case SharedObjectAction::Release:
                if (command.newOwnerConnection || !command.bytes.empty())
                    return {SharedObjectStatus::Invalid, 0, 0};
                state.ownerConnection = 0;
                break;
            default: return {SharedObjectStatus::Invalid, 0, 0};
            }
        }
        ++state.revision;
        _receipts.push_back({key, command, state.revision, state.ownerConnection});
        if (_receipts.size() > MaxReceipts) _receipts.pop_front();
        return {SharedObjectStatus::Applied, state.revision, state.ownerConnection};
    }

    /// Release all objects held by a departed connection; repeated calls do
    /// not create new revisions. Returns affected object IDs in sorted order.
    std::vector<std::uint64_t> releaseConnection(std::uint32_t connectionId) {
        std::vector<std::uint64_t> changed;
        if (!connectionId) return changed;
        for (auto& [id, state] : _objects) {
            if (state.ownerConnection != connectionId || state.revision == UINT64_MAX)
                continue;
            state.ownerConnection = 0;
            ++state.revision;
            changed.push_back(id);
        }
        return changed;
    }

private:
    struct Receipt {
        std::pair<std::uint32_t, std::uint64_t> key;
        SharedObjectCommand command;
        std::uint64_t revision;
        std::uint32_t ownerConnection;
    };
    std::map<std::uint64_t, SharedObjectState> _objects;
    std::deque<Receipt> _receipts;
    std::uint64_t _epoch = 0;
};

} // namespace ayt::net
