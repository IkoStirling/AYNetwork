#pragma once
// AYNetwork/Profiler/ProfilerRegistry.h - R5.5 (2026-08-25)
// Per-AYNetworkSubSystem profiler state. Owns per-connection counters,
// per-netId maps, and the periodic-dump machinery. NOT thread-safe — all
// record* / snapshot / tickWindow calls happen on the main thread.

#include <cstdint>
#include <atomic>
#include <functional>
#include <unordered_map>
#include <vector>

#include <AYNetwork/Profiler/ProfilerSnapshot.h>

namespace ayt::net
{

// Forward decl: the registry snapshots live connection state from
// GnsConnection via a small friend-style accessor set. The full type is
// only needed by ProfilerRegistry.cpp.
class GnsConnection;

class ProfilerRegistry
{
public:
    using Sink = std::function<void(const ProfilerSnapshot&)>;

    ProfilerRegistry();

    // ---- Recording (call from send/recv hooks, post-fault) ----
    // `connNetId` is the AYNetwork connection id (the GnsConnection owner).
    // `msgType` is the wire msgType constant (kMsgTypeReplication etc.).
    // `bytes` is the byte count actually handed to the wire — i.e. for
    // send-side hooks this is post-fault (after loss/dup/reorder/rate-limit),
    // and for recv-side hooks this is the decoded body size.
    // `ghostNetId` is the per-message ghost netId (or 0 for RPC / AppAck /
    // Handshake / ClientInput which aren't per-entity).
    void recordSend(uint32_t connNetId, uint16_t msgType, uint64_t bytes, uint32_t ghostNetId);
    void recordRecv(uint32_t connNetId, uint16_t msgType, uint64_t bytes, uint32_t ghostNetId);

    // ---- Per-netId lifecycle (called by ReplicationManager::registerObject / unregisterObject) ----
    // Pre-creates a per-netId entry so subsequent recordSend with a previously-
    // unseen ghostNetId doesn't grow the map on the hot path. The entry's
    // dirtyFieldCount is refreshed lazily from ReplicationManager at snapshot time.
    void registerNetId(uint32_t connNetId, uint32_t ghostNetId);
    void unregisterNetId(uint32_t connNetId, uint32_t ghostNetId);

    // ---- Snapshot API (read) ----
    // Returns true if the snapshot was filled (connNetId was known). False means
    // the connection has not been seen — out is left untouched.
    bool snapshotFor(uint32_t connNetId, ProfilerSnapshot& out) const;
    // Fills out with one entry per known connection. Connections are added to
    // the map on the first recordSend/recordRecv for that netId.
    void snapshotAll(std::vector<ProfilerSnapshot>& out) const;

    // ---- Tick / window management (called from AYNetworkSubSystem::update) ----
    // Resets currentTickSendBytes across all per-netId entries; advances
    // windowStartServerTick. Called once per frame.
    void tickWindow(uint32_t serverTick);

    // ---- Sink for tests ----
    void setSink(Sink sink);
    // Fire the sink for one snapshot. No-op when no sink is registered.
    // Called from AYNetworkSubSystem::fireProfilerSinkIfSet after snapshotAll.
    void fireSink(const ProfilerSnapshot& s);

    // ---- Periodic stderr dump ----
    // ticks > 0 enables periodic dump every `ticks` window-ticks. 0 disables.
    void setDumpEveryTicks(uint32_t ticks);
    // Called from AYNetworkSubSystem::update after tickWindow. Emits one
    // [AYProfiler] line per known connection when the window has any traffic.
    void dumpPeriodicIfDue();

    // ---- Test seams ----
    // For tests that don't construct a GnsConnection. The snapshot's ConnLiveStatus
    // is left with the default (zeros / -1) values when this returns nullptr.
    // Production callers pass the owning connection.
    using ConnAccessor = std::function<void(uint32_t connNetId, const GnsConnection*& out)>;
    void setConnAccessorForTesting(ConnAccessor accessor) { _connAccessor = std::move(accessor); }

private:
    struct ConnState {
        // Windowed (reset by tickWindow).
        MsgTypeBytes bySlot[kProfilerMsgTypeSlotCount]{};
        std::unordered_map<uint16_t, MsgTypeBytes> byExtras;
        std::unordered_map<uint32_t, NetIdWireCost> perNetId;
        uint64_t windowTotalSend = 0;
        uint64_t windowTotalRecv = 0;
        uint32_t windowStartTick = 0;
        uint32_t windowEndTick = 0;
        // Cumulative across the connection's lifetime — never reset.
        std::atomic<uint64_t> cumulativeSend{0};
        std::atomic<uint64_t> cumulativeRecv{0};
    };

    std::unordered_map<uint32_t, ConnState> _conns;
    Sink _sink;
    ConnAccessor _connAccessor;
    uint32_t _dumpEveryTicks = 0;
    uint32_t _ticksSinceLastDump = 0;

    // Read `live` from a connection — wraps the GNS-side getters.
    void fillLiveStatus(uint32_t connNetId, ConnLiveStatus& out) const;

    // Helpers for recordSend/recordRecv slot-vs-extras dispatch.
    static void addSlot(ConnState& c, uint16_t msgType, uint64_t bytes, bool send);
    static void addExtras(ConnState& c, uint16_t key, uint64_t bytes, bool send);
};

} // namespace ayt::net