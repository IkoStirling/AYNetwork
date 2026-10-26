#pragma once

#include <AYNetwork/Replication/SharedObjectAuthority.h>

namespace ayt::net {

enum class SharedReplicaStatus {
    Applied, Replayed, Invalid, WrongEpoch, Stale, Gap, Capacity
};

/// Owner-thread client mirror for authority snapshots and ordered revisions.
/// A gap rejects the delta and requires a fresh full snapshot; the transport
/// must authenticate the authority and deliver reliable object updates.
class SharedObjectReplica {
public:
    /// Full late-join/recovery snapshot. Same-epoch snapshots may only move
    /// existing object revisions forward and may not silently drop objects,
    /// including across authority migration where the table survives.
    /// Validation is transactional; clear only for a new unrelated session.
    SharedReplicaStatus installSnapshot(std::uint64_t epoch,
                                        std::vector<SharedObjectState> states) {
        if (!epoch || states.size()>SharedObjectAuthority::MaxObjects)
            return SharedReplicaStatus::Invalid;
        if (_epoch && epoch<_epoch) return SharedReplicaStatus::Stale;
        std::map<std::uint64_t,SharedObjectState> candidate;
        for (auto& state:states) {
            if (!valid(state) || !candidate.emplace(state.objectId,std::move(state)).second)
                return SharedReplicaStatus::Invalid;
        }
        if (_epoch) {
            for (const auto& [id,current]:_objects) {
                const auto incoming=candidate.find(id);
                if (incoming==candidate.end() || incoming->second.revision<current.revision)
                    return SharedReplicaStatus::Stale;
                if (incoming->second.revision==current.revision &&
                    !same(incoming->second,current))
                    return SharedReplicaStatus::Invalid;
            }
        }
        _objects.swap(candidate);
        _epoch=epoch;
        return SharedReplicaStatus::Applied;
    }

    /// One authority state revision. New objects must begin at revision 1;
    /// existing objects must advance by exactly one. Equal identical state
    /// is an idempotent replay. No deletion is inferred from absent updates.
    SharedReplicaStatus applyDelta(std::uint64_t epoch,
                                   SharedObjectState state) {
        if (!epoch || epoch!=_epoch) return SharedReplicaStatus::WrongEpoch;
        if (!valid(state)) return SharedReplicaStatus::Invalid;
        const auto it=_objects.find(state.objectId);
        if (it==_objects.end()) {
            if (state.revision!=1) return SharedReplicaStatus::Gap;
            if (_objects.size()>=SharedObjectAuthority::MaxObjects)
                return SharedReplicaStatus::Capacity;
            _objects.emplace(state.objectId,std::move(state));
            return SharedReplicaStatus::Applied;
        }
        if (state.revision==it->second.revision)
            return same(state,it->second) ? SharedReplicaStatus::Replayed
                                          : SharedReplicaStatus::Invalid;
        if (state.revision<it->second.revision) return SharedReplicaStatus::Stale;
        if (it->second.revision==UINT64_MAX ||
            state.revision!=it->second.revision+1) return SharedReplicaStatus::Gap;
        it->second=std::move(state);
        return SharedReplicaStatus::Applied;
    }

    void clear() noexcept { _objects.clear(); _epoch=0; }
    std::uint64_t epoch() const noexcept { return _epoch; }
    std::size_t size() const noexcept { return _objects.size(); }
    std::optional<SharedObjectState> get(std::uint64_t id) const {
        const auto it=_objects.find(id);
        return it==_objects.end() ? std::nullopt
                                  : std::optional<SharedObjectState>(it->second);
    }
    std::vector<SharedObjectState> snapshot() const {
        std::vector<SharedObjectState> result;
        result.reserve(_objects.size());
        for (const auto& [id,state]:_objects) result.push_back(state);
        return result;
    }
private:
    static bool valid(const SharedObjectState& state) noexcept {
        return state.objectId && state.revision &&
            state.bytes.size()<=SharedObjectAuthority::MaxStateBytes;
    }
    static bool same(const SharedObjectState& a,const SharedObjectState& b) noexcept {
        return a.objectId==b.objectId && a.ownerConnection==b.ownerConnection &&
            a.revision==b.revision && a.bytes==b.bytes;
    }
    std::map<std::uint64_t,SharedObjectState> _objects;
    std::uint64_t _epoch=0;
};

} // namespace ayt::net
