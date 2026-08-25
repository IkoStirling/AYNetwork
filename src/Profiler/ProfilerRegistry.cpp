// AYNetwork/src/Profiler/ProfilerRegistry.cpp - R5.5 (2026-08-25)
// Per-AYNetworkSubSystem profiler accumulator + snapshot builder.
// Single-threaded; main thread only. See ProfilerRegistry.h.

#include <AYNetwork/Profiler/ProfilerRegistry.h>
#include <AYNetwork/Profiler/ProfilerMsgType.h>

#include <AYNetwork/Transport/GnsConnection.h>

// GNS forward-declares SteamNetConnectionRealTimeStatus_t / SteamNetConnectionInfo_t
// inside isteamnetworkingsockets.h (vcpkg's GameNetworkingSockets does NOT ship a
// separate isteamnetworkingtypes.h). Including this header brings in those
// struct definitions AND the ISteamNetworkingSockets interface.
#include <steam/isteamnetworkingsockets.h>

#include <algorithm>
#include <cstdio>

namespace ayt::net
{

ProfilerRegistry::ProfilerRegistry() = default;

void ProfilerRegistry::addSlot(ConnState& c, uint16_t msgType, uint64_t bytes, bool send)
{
    const uint8_t slot = profiler::slotForInGameMsgType(msgType);
    // Caller should have pre-checked isInGameMsgType; defensive guard.
    if (slot >= kProfilerMsgTypeSlotCount) return;
    auto& b = c.bySlot[slot];
    if (send) {
        b.sendBytes += bytes;
        b.sendCount += 1;
    } else {
        b.recvBytes += bytes;
        b.recvCount += 1;
    }
}

void ProfilerRegistry::addExtras(ConnState& c, uint16_t key, uint64_t bytes, bool send)
{
    auto& b = c.byExtras[key];
    if (send) {
        b.sendBytes += bytes;
        b.sendCount += 1;
    } else {
        b.recvBytes += bytes;
        b.recvCount += 1;
    }
}

void ProfilerRegistry::recordSend(uint32_t connNetId, uint16_t msgType, uint64_t bytes, uint32_t ghostNetId)
{
    if (connNetId == 0) return;
    auto& c = _conns[connNetId];
    if (profiler::isInGameMsgType(msgType)) {
        addSlot(c, msgType, bytes, /*send=*/true);
        if (ghostNetId != 0) {
            auto& e = c.perNetId[ghostNetId];
            e.netId = ghostNetId;
            e.cumulativeSendBytes += bytes;
            e.currentTickSendBytes += bytes;
        }
    } else {
        // Non-slot msgTypes come through here with the right key already
        // (e.g. kMsgTypeAppAck, kMsgTypeClientInput, or handshakeExtrasKey).
        addExtras(c, msgType, bytes, /*send=*/true);
    }
    c.windowTotalSend += bytes;
    c.cumulativeSend.fetch_add(bytes, std::memory_order_relaxed);
}

void ProfilerRegistry::recordRecv(uint32_t connNetId, uint16_t msgType, uint64_t bytes, uint32_t /*ghostNetId*/)
{
    if (connNetId == 0) return;
    auto& c = _conns[connNetId];
    if (profiler::isInGameMsgType(msgType)) {
        addSlot(c, msgType, bytes, /*send=*/false);
    } else {
        addExtras(c, msgType, bytes, /*send=*/false);
    }
    c.windowTotalRecv += bytes;
    c.cumulativeRecv.fetch_add(bytes, std::memory_order_relaxed);
}

void ProfilerRegistry::registerNetId(uint32_t connNetId, uint32_t ghostNetId)
{
    if (connNetId == 0 || ghostNetId == 0) return;
    auto& c = _conns[connNetId];
    auto& e = c.perNetId[ghostNetId];
    e.netId = ghostNetId;
    // Preserve cumulativeSendBytes if entry already exists.
}

void ProfilerRegistry::unregisterNetId(uint32_t connNetId, uint32_t ghostNetId)
{
    if (connNetId == 0 || ghostNetId == 0) return;
    auto it = _conns.find(connNetId);
    if (it == _conns.end()) return;
    it->second.perNetId.erase(ghostNetId);
}

void ProfilerRegistry::tickWindow(uint32_t serverTick)
{
    for (auto& [netId, c] : _conns) {
        (void)netId;
        c.windowStartTick = c.windowEndTick;
        c.windowEndTick = serverTick;
        // Roll the per-tick per-netId bytes; the bySlot/byExtras arrays
        // and windowTotalSend/Recv are ROLLED (not reset to 0) so the
        // snapshot still has access to the "this window" totals until the
        // next window boundary. Tests opt into the [windowEndTick-old] vs
        // [windowEndTick-new] deltas via two snapshots in a row.
        for (auto& [gnid, e] : c.perNetId) {
            (void)gnid;
            e.currentTickSendBytes = 0;
        }
        // Slide window forward — clear the by-slot/extras counters only at
        // the periodic-dump boundary (dumpPeriodicIfDue does it). tickWindow
        // itself only refreshes the tick label so the snapshot reader can
        // compute deltas; the counters persist until cleared by the dump.
    }
    ++_ticksSinceLastDump;
}

void ProfilerRegistry::setSink(Sink sink)
{
    _sink = std::move(sink);
}

void ProfilerRegistry::fireSink(const ProfilerSnapshot& s)
{
    if (_sink) _sink(s);
}

void ProfilerRegistry::setDumpEveryTicks(uint32_t ticks)
{
    _dumpEveryTicks = ticks;
    _ticksSinceLastDump = 0;
}

void ProfilerRegistry::fillLiveStatus(uint32_t connNetId, ConnLiveStatus& out) const
{
    // ConnAccessor is how production code gives us the GnsConnection*. We
    // call it; if it returns null (test path with no GnsConnection), the
    // caller already got the default-zero struct and we just leave it.
    const GnsConnection* conn = nullptr;
    if (_connAccessor) {
        _connAccessor(connNetId, conn);
    }
    if (!conn) return;

    out.pingMs = conn->getPing();

    // Pull a fresh real-time status snapshot. The GNS struct field names
    // differ across Steamworks versions; this build uses the
    // GameNetworkingSockets v1.x field set:
    //   m_flInBytesPerSec / m_flOutBytesPerSec (floats, not ints)
    //   m_cbPendingReliable / m_cbPendingUnreliable
    //   m_nSendRateBytesPerSecond (capacity)
    //   m_flConnectionQualityLocal/Remote
    // No m_nMsgsReceived/Sent counters in this GNS version — we leave
    // inMessageCount/outMessageCount at 0 (their default).
    if (GnsConnection::s_gns) {
        SteamNetConnectionRealTimeStatus_t status{};
        SteamNetConnectionRealTimeLaneStatus_t lane{};
        if (GnsConnection::s_gns->GetConnectionRealTimeStatus(
                conn->getInnerConnection(), &status, 1, &lane)) {
            out.qualityLocal    = status.m_flConnectionQualityLocal;
            out.qualityRemote   = status.m_flConnectionQualityRemote;
            out.inBytesPerSec   = static_cast<uint32_t>(status.m_flInBytesPerSec);
            out.outBytesPerSec  = static_cast<uint32_t>(status.m_flOutBytesPerSec);
            out.pendingReliable   = static_cast<uint32_t>(status.m_cbPendingReliable);
            out.pendingUnreliable = static_cast<uint32_t>(status.m_cbPendingUnreliable);
            // sendQueueBytes = unreliable + reliable pending bytes (closest
            // analogue to newer-GNS m_nSendQueueBytes).
            out.sendQueueBytes  = out.pendingReliable + out.pendingUnreliable;
        }

        // R6 C5 H-13: m_idPOPRelay varies run-to-run and would break snapshot
        // equality between recordings. Normalize both path fields to 0
        // unconditionally — path classification is forward-looking for
        // the v2 player and not used by state-equal hashing today.
        SteamNetConnectionInfo_t info{};
        if (GnsConnection::s_gns->GetConnectionInfo(conn->getInnerConnection(), &info)) {
            (void)info;
            out.pathLocal  = 0u;
            out.pathRemote = 0u;
        }
    }

    out.ackPending         = static_cast<uint32_t>(conn->pendingAckCountForTesting());
    out.fragmentQueueBytes = static_cast<uint32_t>(conn->pendingFragmentBytesForTesting());
    out.simInBytesUnprocessed = conn->getLastPumpBytes();
}

bool ProfilerRegistry::snapshotFor(uint32_t connNetId, ProfilerSnapshot& out) const
{
    auto it = _conns.find(connNetId);
    if (it == _conns.end()) return false;
    const ConnState& c = it->second;
    out = ProfilerSnapshot{};
    out.netId = connNetId;
    out.windowStartServerTick = c.windowStartTick;
    out.windowEndServerTick   = c.windowEndTick;
    out.windowTotalSendBytes  = c.windowTotalSend;
    out.windowTotalRecvBytes  = c.windowTotalRecv;
    for (uint8_t i = 0; i < kProfilerMsgTypeSlotCount; ++i) {
        out.byMsgType[i] = c.bySlot[i];
    }
    out.byMsgTypeExtras = c.byExtras;
    out.perNetId.reserve(c.perNetId.size());
    for (const auto& [gnid, e] : c.perNetId) {
        out.perNetId.push_back(e);
    }
    std::sort(out.perNetId.begin(), out.perNetId.end(),
              [](const NetIdWireCost& a, const NetIdWireCost& b) {
                  return a.cumulativeSendBytes > b.cumulativeSendBytes;
              });
    fillLiveStatus(connNetId, out.live);
    return true;
}

void ProfilerRegistry::snapshotAll(std::vector<ProfilerSnapshot>& out) const
{
    out.clear();
    out.reserve(_conns.size());
    for (const auto& [netId, _] : _conns) {
        (void)_;
        ProfilerSnapshot s;
        if (snapshotFor(netId, s)) {
            out.push_back(std::move(s));
        }
    }
}

void ProfilerRegistry::dumpPeriodicIfDue()
{
    if (_dumpEveryTicks == 0) return;
    if (_ticksSinceLastDump < _dumpEveryTicks) return;
    _ticksSinceLastDump = 0;

    for (const auto& [netId, c] : _conns) {
        (void)c;
        if (c.windowTotalSend == 0 && c.windowTotalRecv == 0) continue;
        ProfilerSnapshot s;
        if (!snapshotFor(netId, s)) continue;
        const double total = static_cast<double>(s.windowTotalSendBytes);
        auto slotPct = [&](uint8_t i) -> double {
            return total > 0.0
                ? 100.0 * static_cast<double>(s.byMsgType[i].sendBytes) / total
                : 0.0;
        };
        ::fprintf(stderr,
                  "[AYProfiler] netId=%u window=[%u,%u] in=%llu out=%llu "
                  "msgType[Replication]=%llu(%4.1f%%) "
                  "msgType[EntitySpawn]=%llu(%4.1f%%) "
                  "msgType[EntityDespawn]=%llu(%4.1f%%) "
                  "msgType[Delta]=%llu(%4.1f%%) "
                  "msgType[RpcRequest]=%llu(%4.1f%%) "
                  "msgType[RpcResponse]=%llu(%4.1f%%) "
                  "msgType[RpcReject]=%llu(%4.1f%%) "
                  "ping=%dms qLocal=%.2f qRemote=%.2f "
                  "pendingRel=%u pendingUnrel=%u sendQ=%u ackPending=%u fragQ=%u\n",
                  s.netId,
                  s.windowStartServerTick, s.windowEndServerTick,
                  static_cast<unsigned long long>(s.windowTotalRecvBytes),
                  static_cast<unsigned long long>(s.windowTotalSendBytes),
                  static_cast<unsigned long long>(s.byMsgType[0].sendBytes), slotPct(0),
                  static_cast<unsigned long long>(s.byMsgType[1].sendBytes), slotPct(1),
                  static_cast<unsigned long long>(s.byMsgType[2].sendBytes), slotPct(2),
                  static_cast<unsigned long long>(s.byMsgType[3].sendBytes), slotPct(3),
                  static_cast<unsigned long long>(s.byMsgType[4].sendBytes), slotPct(4),
                  static_cast<unsigned long long>(s.byMsgType[5].sendBytes), slotPct(5),
                  static_cast<unsigned long long>(s.byMsgType[6].sendBytes), slotPct(6),
                  s.live.pingMs,
                  s.live.qualityLocal, s.live.qualityRemote,
                  s.live.pendingReliable, s.live.pendingUnreliable,
                  s.live.sendQueueBytes, s.live.ackPending, s.live.fragmentQueueBytes);
    }

    // Reset window counters now that we've reported them.
    for (auto& [netId, c] : _conns) {
        (void)netId;
        for (auto& [_, m] : c.byExtras) { (void)_; m.sendBytes = 0; m.recvBytes = 0; m.sendCount = 0; m.recvCount = 0; }
        for (uint8_t i = 0; i < kProfilerMsgTypeSlotCount; ++i) {
            c.bySlot[i].sendBytes = 0;
            c.bySlot[i].recvBytes = 0;
            c.bySlot[i].sendCount = 0;
            c.bySlot[i].recvCount = 0;
        }
        c.windowTotalSend = 0;
        c.windowTotalRecv = 0;
    }
}

} // namespace ayt::net