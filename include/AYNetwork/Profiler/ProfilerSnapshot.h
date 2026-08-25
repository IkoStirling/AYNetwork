#pragma once
// AYNetwork/Profiler/ProfilerSnapshot.h - R5.5 (2026-08-25)
// Profiler pull-API snapshot data structures. Header-only, no AYNetwork deps
// beyond <cstdint>/<vector>/<unordered_map>/<array>. The application pulls
// these via INetworkSubSystem::getProfilerSnapshot / getProfilerSnapshots.

#include <cstdint>
#include <vector>
#include <unordered_map>
#include <array>

namespace ayt::net
{

// Number of in-game msgType slots (Replication, EntitySpawn, EntityDespawn,
// Delta, RpcRequest, RpcResponse, RpcReject). AppAck / ClientInput /
// Handshake variants are stored separately under byMsgTypeExtras keyed by
// the msgType constant or (msgType<<8 | subType) for Handshake.
constexpr uint8_t kProfilerMsgTypeSlotCount = 7;

// Per-(connection × msgType) byte + count accumulators.
struct MsgTypeBytes {
    uint64_t sendBytes = 0;
    uint64_t recvBytes = 0;
    uint64_t sendCount = 0;
    uint64_t recvCount = 0;
};

// Per-(connection × netId) cumulative and current-tick send byte cost.
// `cumulativeSendBytes` is monotonic across the connection's lifetime and
// `currentTickSendBytes` is reset at the start of every AYNetworkSubSystem
// update so the application can correlate per-tick hotspots with frames.
struct NetIdWireCost {
    uint32_t netId = 0;
    uint64_t cumulativeSendBytes = 0;
    uint64_t currentTickSendBytes = 0;
    uint32_t dirtyFieldCount = 0;   // snapshot of ReplicationManager::getDirtyFieldCount at snapshot time
};

// Snapshot of connection-level live state. All fields are read at the
// moment of snapshot build from the per-connection cache populated by
// GnsConnection::refreshRealTimeStatus(). Most fields come from
// SteamNetConnectionRealTimeStatus_t; the fragment/ack/simInBytes fields
// come from the AYNetwork-side queues and are populated via the GNS
// connection's existing test-seam getters.
struct ConnLiveStatus {
    int  pingMs = -1;                 // GnsConnection::getPing() (GNS direct)
    float qualityLocal = -1.f;        // GNS m_flConnectionQualityLocal
    float qualityRemote = -1.f;       // GNS m_flConnectionQualityRemote
    uint32_t inBytesPerSec = 0;       // GNS m_nBytesPerSec (recv direction)
    uint32_t outBytesPerSec = 0;      // GNS m_nBytesPerSec (send direction)
    uint32_t pendingReliable = 0;     // GNS m_nPendingReliableBytes
    uint32_t pendingUnreliable = 0;   // GNS m_nPendingUnreliableBytes
    uint32_t inMessageCount = 0;      // GNS m_nMsgsReceived in last interval
    uint32_t outMessageCount = 0;     // GNS m_nMsgsSent in last interval
    uint16_t pathLocal = 0;           // k_ESteamNetworkingConnectionType_Direct (0) / Relay (1) etc.
    uint16_t pathRemote = 0;          // same for remote side
    uint32_t sendQueueBytes = 0;      // GNS m_nSendQueueBytes
    uint32_t ackPending = 0;          // GnsConnection::pendingAckCountForTesting()
    uint32_t fragmentQueueBytes = 0;  // PacketAssembler::pendingBytes()
    uint32_t simInBytesUnprocessed = 0; // bytes from the most recent pump() result (inbound bytes awaiting dispatch)
};

// One per connection. Snapshot is copy-and-move friendly; the cost is
// roughly 200 B per snapshot plus the byMsgTypeExtras map and perNetId
// vector. Pull this from the main thread via getProfilerSnapshot(netId)
// or getProfilerSnapshots(out).
struct ProfilerSnapshot {
    uint32_t netId = 0;
    uint32_t windowStartServerTick = 0;
    uint32_t windowEndServerTick = 0;
    uint64_t windowTotalSendBytes = 0;
    uint64_t windowTotalRecvBytes = 0;
    // 7 in-game msgType slots — see ProfilerMsgType.h for the slot↔msgType table.
    std::array<MsgTypeBytes, kProfilerMsgTypeSlotCount> byMsgType{};
    // Extras: keyed by either the msgType constant directly (AppAck, ClientInput)
    // or (kMsgTypeHandshake<<8 | HandshakeMsgType) for the three Handshake variants.
    // Lookup helpers live in ProfilerMsgType.h.
    std::unordered_map<uint16_t, MsgTypeBytes> byMsgTypeExtras;
    // Per-netId wire cost. Sorted by cumulativeSendBytes desc when returned.
    std::vector<NetIdWireCost> perNetId;
    ConnLiveStatus live;
};

} // namespace ayt::net