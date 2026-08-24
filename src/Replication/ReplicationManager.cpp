// ReplicationManager.cpp - R3.0 Replication manager implementation

#include <AYNetwork/Replication/ReplicationManager.h>

#include <AYNetwork/Replication/ReflectSerializer.h>
#include <AYNetwork/Snapshot/SnapshotInterpolator.h>
#include <AYNetwork/Protocol/PacketCodec.h>
#include <AYNetwork/Transport/GnsConnection.h>
#include <AYNetwork/INetwork.h>

#include <AYNetwork/Prediction/PredictionManager.h>

#include <AYReplay/IReplayRecorder.h>
#include <AYNetwork/Replay/NetworkReplayTypes.h>

#include <AYReflect/IReflect.h>

#include <cstdio>
#include <cstring>

namespace ayt::net
{

// =============================================================================
// ReflectedEntry — single record kept in _objects.
// Both register paths (IReplicable* legacy and (void*, ITypeInfo*) primary)
// populate the same entry so dispatch in tick/onReceive is uniform.
//
// R3.1 (2026-07-27) dirty-tracking state:
//   - _netFieldSparseIndex[denseIdx] = type->getField() sparse index for the
//     corresponding NetReplicate field. The dense list has one entry per
//     NetReplicate field (in the order they appear in type->getField()).
//   - _peers[connectionId] stores visibility, initialization and the CRC32C
//     baseline independently for every receiving connection. A newly-visible
//     peer receives Spawn + Full before it can receive Delta frames.
// =============================================================================
struct ReplicationManager::ReflectedEntry {
    void*                          obj = nullptr;   // live object memory
    const ayt::reflect::ITypeInfo* type = nullptr;  // metadata for serializer
    IReplicable*                   iface = nullptr; // optional IReplicable adapter

    struct PeerState {
        std::vector<uint32_t> fieldHashes;
        bool visible = false;
        bool initialized = false;
    };

    // ---- per-connection dirty tracking ----
    std::vector<uint32_t> _netFieldSparseIndex;   // dense → type->getField() sparse
    std::unordered_map<uint32_t, PeerState> _peers;

    // ---- R4.1-B interest ----
    NetVec3 _location{};
    bool    _hasLocation = false;
};

// =============================================================================
// ctor / dtor
// =============================================================================
ReplicationManager::ReplicationManager(INetworkSubSystem* network)
    : _network(network)
    , _prediction(new PredictionManager(/*ringCapacity=*/ 32))
{
}

ReplicationManager::~ReplicationManager() {
    delete _prediction;
    _prediction = nullptr;
}

// =============================================================================
// R3.0 primary register path
// =============================================================================
namespace {

// Walk type fields once, collecting NetReplicate ones into two parallel arrays:
//   sparseIndex[k] = type->getField() index of the k-th NetReplicate field
// The size of the result equals the number of NetReplicate fields the type
// actually has (after filtering by NetReplicate AND by a supported WireTypeId).
//
// R3.1 dirty-tracking uses the same scan that serializeObject uses internally
// to compute fieldCount, so the wire layout stays in lock-step with the
// server-side hash baseline.
void buildNetFieldMap(const ayt::reflect::ITypeInfo* type,
                      std::vector<uint32_t>& sparseIndex) {
    sparseIndex.clear();
    if (!type) return;
    const uint32_t total = type->getFieldCount();
    for (uint32_t i = 0; i < total; ++i) {
        const auto* f = type->getField(i);
        if (!f) continue;
        if (!f->hasAttribute(ayt::reflect::FieldAttribute::NetReplicate)) continue;
        WireTypeId wid;
        if (!ReflectSerializer::resolveWireTypeId(f->getType(), wid)) continue;
        sparseIndex.push_back(i);
    }
}

float distanceSq(const NetVec3& a, const NetVec3& b) {
    const float dx = a.x - b.x;
    const float dy = a.y - b.y;
    const float dz = a.z - b.z;
    return dx * dx + dy * dy + dz * dz;
}

} // anonymous namespace

void ReplicationManager::setInterestRadius(float interestRadius) {
    _interestRadius = interestRadius;
    _interestRadiusSq = (interestRadius > 0.f) ? (interestRadius * interestRadius) : 0.f;
}

void ReplicationManager::setObjectLocation(uint32_t netId, NetVec3 location) {
    auto it = _objects.find(netId);
    if (it == _objects.end()) return;
    it->second._location = location;
    it->second._hasLocation = true;
}

bool ReplicationManager::getObjectLocation(uint32_t netId, NetVec3& out) const {
    auto it = _objects.find(netId);
    if (it == _objects.end() || !it->second._hasLocation) return false;
    out = it->second._location;
    return true;
}

namespace {

std::vector<NetConnection*> collectConnectedTargets(INetworkSubSystem* network) {
    std::vector<NetConnection*> targets;
    if (!network) return targets;
    for (NetConnection* conn : network->getConnections()) {
        if (conn && conn->isConnected()) targets.push_back(conn);
    }
    return targets;
}

// =============================================================================
// R5.3 (2026-08-24) Replay wire-tap helpers.
//
// These pack the small network-adapter payload headers (see
// NetworkReplayRecorderAdapter.h for the layout) and forward to the
// foundation IReplayRecorder. Doing the packing here keeps the adapter
// header out of ReplicationManager.cpp's include set — the wire-tap
// always talks to the recorder through the foundation interface only.
// =============================================================================
inline void packU32LE(uint8_t* dst, uint32_t v) {
    dst[0] = static_cast<uint8_t>(v & 0xFF);
    dst[1] = static_cast<uint8_t>((v >> 8) & 0xFF);
    dst[2] = static_cast<uint8_t>((v >> 16) & 0xFF);
    dst[3] = static_cast<uint8_t>((v >> 24) & 0xFF);
}

void recordFullSnapshot(ayt::replay::IReplayRecorder* rec,
                        uint32_t connectionId, uint32_t serverTick,
                        uint8_t frameFlags,
                        const uint8_t* sealedPayload, size_t size) {
    if (!rec) return;
    std::vector<uint8_t> buf(5 + size);
    packU32LE(buf.data(), connectionId);
    buf[4] = frameFlags;
    if (size > 0 && sealedPayload) std::memcpy(buf.data() + 5, sealedPayload, size);
    rec->recordEvent(serverTick, ayt::net::replay::kEvtNet_InitialFullSnapshot,
                     buf.data(), buf.size());
}

void recordDeltaSnapshot(ayt::replay::IReplayRecorder* rec,
                         uint32_t connectionId, uint32_t serverTick,
                         uint8_t frameFlags,
                         const uint8_t* sealedPayload, size_t size) {
    if (!rec) return;
    std::vector<uint8_t> buf(5 + size);
    packU32LE(buf.data(), connectionId);
    buf[4] = frameFlags;
    if (size > 0 && sealedPayload) std::memcpy(buf.data() + 5, sealedPayload, size);
    rec->recordEvent(serverTick, ayt::net::replay::kEvtNet_DeltaSnapshot,
                     buf.data(), buf.size());
}

void recordSpawn(ayt::replay::IReplayRecorder* rec,
                 uint32_t connectionId, uint32_t serverTick,
                 uint32_t netId, uint64_t schemaHash,
                 const uint8_t* spawnPayload, size_t size) {
    if (!rec) return;
    std::vector<uint8_t> buf(16 + size);
    packU32LE(buf.data() + 0, connectionId);
    packU32LE(buf.data() + 4, netId);
    for (int i = 0; i < 8; ++i) {
        buf[8 + i] = static_cast<uint8_t>((schemaHash >> (i * 8)) & 0xFF);
    }
    if (size > 0 && spawnPayload) std::memcpy(buf.data() + 16, spawnPayload, size);
    rec->recordEvent(serverTick, ayt::net::replay::kEvtNet_Spawn,
                     buf.data(), buf.size());
}

void recordDespawn(ayt::replay::IReplayRecorder* rec,
                   uint32_t connectionId, uint32_t serverTick, uint32_t netId) {
    if (!rec) return;
    uint8_t buf[8];
    packU32LE(buf + 0, connectionId);
    packU32LE(buf + 4, netId);
    rec->recordEvent(serverTick, ayt::net::replay::kEvtNet_Despawn,
                     buf, sizeof(buf));
}

} // anonymous namespace

std::vector<NetConnection*> ReplicationManager::buildInterestTargets(
    void* obj, const ayt::reflect::ITypeInfo* type, uint32_t netId,
    NetVec3 objLoc, bool hasObjLoc) const {
    std::vector<NetConnection*> targets = collectConnectedTargets(_network);
    if (targets.empty()) return targets;

    if (_interestRadiusSq > 0.f && hasObjLoc) {
        std::vector<NetConnection*> inRange;
        inRange.reserve(targets.size());
        for (NetConnection* conn : targets) {
            auto* viewerPos = static_cast<NetVec3*>(conn->getUserData());
            if (!viewerPos) {
                inRange.push_back(conn);
                continue;
            }
            if (distanceSq(*viewerPos, objLoc) <= _interestRadiusSq) {
                inRange.push_back(conn);
            }
        }
        targets = std::move(inRange);
    }

    if (_extension) {
        std::vector<NetConnection*> relevant;
        relevant.reserve(targets.size());
        for (NetConnection* conn : targets) {
            if (_extension->isRelevant(conn, obj, type, netId)) {
                relevant.push_back(conn);
            }
        }
        targets = std::move(relevant);
    }
    return targets;
}

bool ReplicationManager::sendSealedToTargets(
    void* obj, const ayt::reflect::ITypeInfo* type, uint32_t netId,
    NetVec3 objLoc, bool hasObjLoc,
    uint8_t channel, const void* data, size_t size) {
    std::vector<NetConnection*> targets =
        buildInterestTargets(obj, type, netId, objLoc, hasObjLoc);
    if (_extension && obj && type) {
        _extension->onPreReplicate(obj, type, netId, targets);
    }
    // R3.x test seam: broadcast sink tests have no INetworkSubSystem connections.
    if (targets.empty() && _broadcastSink) {
        _broadcastSink(channel, data, size);
        return true;
    }
    if (targets.empty()) return false;

    if (_broadcastSink) {
        if (_interestRadiusSq <= 0.f && targets.size() == collectConnectedTargets(_network).size()) {
            _broadcastSink(channel, data, size);
            return true;
        }
        for (NetConnection* conn : targets) {
            (void)conn;
            _broadcastSink(channel, data, size);
        }
        return true;
    }
    if (!_network) return false;

    const size_t allCount = collectConnectedTargets(_network).size();
    if (_interestRadiusSq <= 0.f && targets.size() == allCount) {
        _network->broadcastEncoded(channel, data, size);
        return true;
    }
    for (NetConnection* conn : targets) {
        _network->sendEncodedTo(conn, channel, data, size);
    }
    return true;
}

bool ReplicationManager::sendSealedToConnection(NetConnection* target, uint8_t channel,
                                                const void* data, size_t size) {
    if (!data || size == 0) return false;
    if (_broadcastSink) {
        _broadcastSink(channel, data, size);
        return true;
    }
    if (!_network || !target || !target->isConnected()) return false;
    _network->sendEncodedTo(target, channel, data, size);
    return true;
}

NetConnection* ReplicationManager::findConnectedTarget(uint32_t connectionId) const {
    if (!_network || connectionId == 0) return nullptr;
    for (NetConnection* conn : _network->getConnections()) {
        if (conn && conn->isConnected() && conn->getId() == connectionId) return conn;
    }
    return nullptr;
}

void ReplicationManager::registerObject(void* obj, const ayt::reflect::ITypeInfo* type, uint32_t netId) {
    if (!obj || !type || netId == 0) return;

    ReflectedEntry e;
    e.obj  = obj;
    e.type = type;

    // Build the dense→sparse mapping once. Per-peer hash baselines are
    // allocated lazily when a connection first becomes interested.
    buildNetFieldMap(type, e._netFieldSparseIndex);
    _objects[netId] = e;
    _spawnAnnouncements.erase(netId);

    // R5.2 (2026-08-24): default ProxyKind = SimulatedProxy. Keeps R3.x
    // byte-for-byte behavior for everyone who never calls
    // setObjectProxyKind(...). Authority flips to AutonomousProxy via
    // setObjectProxyKind() after registration.
    _proxyKinds.emplace(netId, ProxyKind::SimulatedProxy);
}

void ReplicationManager::unregisterObject(uint32_t netId) {
    auto it = _objects.find(netId);
    if (it == _objects.end()) return;
    ReflectedEntry& entry = it->second;
    if (isAuthority() && (_network || _broadcastSink)) {
        BitStream body;
        ReflectSerializer::writeEntityDespawn(body, netId);
        std::vector<uint8_t> sealed = PacketCodec::encode(
            static_cast<const uint8_t*>(body.getData()), body.getSize(),
            kMsgTypeEntityDespawn, kSchemaVersion,
            CHANNEL_RELIABLE, /*flags=*/ 0, /*timestampMs=*/ 0,
            /*compress=*/ false);
        for (const auto& [connectionId, peer] : entry._peers) {
            if (!peer.visible) continue;
            NetConnection* target = findConnectedTarget(connectionId);
            if (connectionId == 0 || target) {
                sendSealedToConnection(target, CHANNEL_RELIABLE, sealed.data(), sealed.size());
                // R5.3 (2026-08-24) Replay wire-tap: uninstantiate Despawn.
                recordDespawn(_replay, connectionId, _serverTick, netId);
            }
        }
    }
    _objects.erase(it);
    // R5.0: drop the interpolator's per-ghost state too so we don't keep
    // an orphaned ring around. Safe even when the interpolator is null.
    if (_snapshotInterpolator) _snapshotInterpolator->unregisterGhost(netId);
}

// =============================================================================
// R1 deprecated wrapper
// =============================================================================
void ReplicationManager::registerObject(IReplicable* obj, uint32_t netId) {
    if (!obj) return;
    if (obj->getNetId() == 0) {
        obj->setNetId(netId);
    }
    ReflectedEntry e;
    e.obj   = obj; // IReplicable* → void* (upcast)
    e.type  = nullptr; // legacy path has no AYReflect metadata; will skip reflection walk
    e.iface = obj;
    _objects[netId] = e;

    // Legacy IReplicable objects are NOT replicated via ReflectSerializer in
    // R3.0 — the user is expected to migrate to AYReflect-marked fields.
    // We still send EntitySpawn so the client can allocate a slot keyed by
    // netId, but with schemaHash=0 (client treats this as "untyped").
    if (isAuthority() && (_network || _broadcastSink)) {
        BitStream body;
        ReflectSerializer::writeEntitySpawn(body, netId, /*schemaHash=*/ 0);
        std::vector<uint8_t> sealed = PacketCodec::encode(
            static_cast<const uint8_t*>(body.getData()), body.getSize(),
            kMsgTypeEntitySpawn, kSchemaVersion,
            CHANNEL_RELIABLE, /*flags=*/ 0, /*timestampMs=*/ 0,
            /*compress=*/ false);
        if (_broadcastSink) _broadcastSink(CHANNEL_RELIABLE, sealed.data(), sealed.size());
        else                _network->broadcastEncoded(CHANNEL_RELIABLE, sealed.data(), sealed.size());
        // R5.3 (2026-08-24) Replay wire-tap: legacy broadcast EntitySpawn.
        recordSpawn(_replay, /*connectionId=*/ 0, _serverTick,
                    netId, /*schemaHash=*/ 0,
                    sealed.data(), sealed.size());
    }
}

// =============================================================================
// Lookups
// =============================================================================
const ayt::reflect::ITypeInfo* ReplicationManager::findType(uint32_t netId) const {
    auto it = _objects.find(netId);
    return (it != _objects.end()) ? it->second.type : nullptr;
}

void* ReplicationManager::findObject(uint32_t netId) const {
    auto it = _objects.find(netId);
    return (it != _objects.end()) ? it->second.obj : nullptr;
}

IReplicable* ReplicationManager::findIReplicable(uint32_t netId) const {
    auto it = _objects.find(netId);
    return (it != _objects.end()) ? it->second.iface : nullptr;
}

// =============================================================================
// tick — server-side only (authority gate). Iterates registered objects and
// broadcasts either a Full Snapshot (kMsgTypeReplication on CHANNEL_RELIABLE)
// or a Delta frame (kMsgTypeDelta on CHANNEL_UNRELIABLE), depending on the
// dirty-tracking state.
//
// R3.0 (pre-R3.1): Full Snapshot every tick — wasted bandwidth when nothing
//                  changed.
// R3.1+: each peer has an independent NetReplicate-field CRC32C baseline.
//        tick() emits Spawn + Full to newly-visible peers, Delta frames for
//        fields dirty against that peer's baseline, and Despawn on visibility
//        exit. Steady state with zero changes emits no frame.
// =============================================================================
void ReplicationManager::tick(float /*deltaTime*/) {
    if (!_network && !_broadcastSink) return;

    // Authority gate: only the server emits frames. Clients do nothing on
    // tick — they only deserialize frames received via onReceive.
    // R4.1: gate accepts Server (dedicated) AND ListenServer (host) via
    // isAuthority() helper.
    if (!isAuthority()) return;

    // R5.0: bump serverTick once per emitted tick so the wire body can be
    // stamped with a monotonic counter. The default rate is 30 Hz; production
    // code is expected to drive advanceServerTick() once per frame instead
    // of relying on the rate here — see AYNetworkSubSystem::update wiring.
    advanceServerTick(1.0 / (_serverTickRate > 0.0 ? _serverTickRate : 30.0));

    // Snapshot keys so callbacks may register/unregister without invalidating
    // this traversal.
    std::vector<uint32_t> netIds;
    netIds.reserve(_objects.size());
    for (const auto& kv : _objects) netIds.push_back(kv.first);

    for (uint32_t netId : netIds) {
        auto it = _objects.find(netId);
        if (it == _objects.end()) continue;
        ReflectedEntry& e = it->second;

        // Legacy entries have no reflected payload.
        if (!e.type) continue;

        // Compute current field hashes once; each peer compares them against
        // its own acknowledged/sent baseline.
        const uint32_t nFields = static_cast<uint32_t>(e._netFieldSparseIndex.size());
        std::vector<uint32_t> currentHashes(nFields);
        for (uint32_t k = 0; k < nFields; ++k) {
            const auto* field = e.type->getField(e._netFieldSparseIndex[k]);
            if (!field) continue;
            WireTypeId wid;
            if (!ReflectSerializer::resolveWireTypeId(field->getType(), wid)) continue;
            currentHashes[k] = ReflectSerializer::hashFieldValueEx(wid, field->getType(), field->get(e.obj));
        }

        std::vector<NetConnection*> targets = buildInterestTargets(
            e.obj, e.type, netId, e._location, e._hasLocation);
        if (_extension) _extension->onPreReplicate(e.obj, e.type, netId, targets);

        std::unordered_map<uint32_t, NetConnection*> targetById;
        for (NetConnection* target : targets) {
            if (target && target->isConnected()) targetById[target->getId()] = target;
        }
        if (targetById.empty() && _broadcastSink) targetById.emplace(0u, nullptr);

        // Interest/relevancy exit: a peer that previously saw the object gets
        // a targeted Despawn. Disconnected peers are simply forgotten.
        for (auto peerIt = e._peers.begin(); peerIt != e._peers.end();) {
            if (targetById.contains(peerIt->first)) {
                ++peerIt;
                continue;
            }
            NetConnection* previousTarget = findConnectedTarget(peerIt->first);
            if (peerIt->second.visible && (peerIt->first == 0 || previousTarget)) {
                BitStream despawnBody;
                ReflectSerializer::writeEntityDespawn(despawnBody, netId);
                auto wire = PacketCodec::encode(
                    static_cast<const uint8_t*>(despawnBody.getData()), despawnBody.getSize(),
                    kMsgTypeEntityDespawn, kSchemaVersion, CHANNEL_RELIABLE,
                    0, 0, false);
                sendSealedToConnection(previousTarget, CHANNEL_RELIABLE, wire.data(), wire.size());
                // R5.3 (2026-08-24) Replay wire-tap: capture Despawn for
                // the previous peer that lost visibility.
                recordDespawn(_replay, peerIt->first, _serverTick, netId);
            }
            peerIt = e._peers.erase(peerIt);
        }

        bool replicatedToAny = false;
        // R5.1: teleport marker drains after we emit the Full Snapshot for
        // this netId. Compute once before the peer loop so every peer sees
        // the same flag set in this tick's frame; clear after the loop.
        const bool teleport = (_teleportPending.count(netId) > 0);
        const uint8_t frameFlags = teleport ? kFlagTeleport : 0;

        for (const auto& [connectionId, target] : targetById) {
            ReflectedEntry::PeerState& peer = e._peers[connectionId];

            if (!peer.visible) {
                BitStream spawnBody;
                ReflectSerializer::writeEntitySpawn(
                    spawnBody, netId, ReflectSerializer::hashTypeSchema(e.type));
                auto spawnWire = PacketCodec::encode(
                    static_cast<const uint8_t*>(spawnBody.getData()), spawnBody.getSize(),
                    kMsgTypeEntitySpawn, kSchemaVersion, CHANNEL_RELIABLE,
                    0, 0, false);
                if (!sendSealedToConnection(target, CHANNEL_RELIABLE,
                                            spawnWire.data(), spawnWire.size())) {
                    continue;
                }
                // R5.3 (2026-08-24) Replay wire-tap: per-peer first-tick
                // EntitySpawn. connectionId is per-peer (not 0).
                recordSpawn(_replay, connectionId, _serverTick,
                            netId, ReflectSerializer::hashTypeSchema(e.type),
                            spawnWire.data(), spawnWire.size());
                peer.visible = true;
                peer.initialized = false;
                peer.fieldHashes.clear();
            }

            if (!peer.initialized) {
                BitStream fullBody;
                if (!ReflectSerializer::serializeObject(e.type, e.obj, netId, fullBody,
                                                        _serverTick, frameFlags)) continue;
                // R5.2 (2026-08-24): optional ack tail for connections that
                // own an AutonomousProxy ghost. Defaults to no-tail so the
                // R5.0/R5.1 byte layout is preserved for SimulatedProxy-only
                // destinations. The build helper enforces the gate.
                const AckTailInfo info = buildAckTailForConnection(
                    connectionId, /*serverCommandAge=*/ _serverTick);
                ReflectSerializer::AckTail wireTail;
                wireTail.lastAckedInputTick = info.lastAckedInputTick;
                wireTail.serverCommandAge   = info.serverCommandAge;
                wireTail.present            = info.present;
                ReflectSerializer::writeAckTail(fullBody, wireTail);
                auto fullWire = PacketCodec::encode(
                    static_cast<const uint8_t*>(fullBody.getData()), fullBody.getSize(),
                    kMsgTypeReplication, kSchemaVersion, CHANNEL_RELIABLE,
                    0, 0, false);
                if (sendSealedToConnection(target, CHANNEL_RELIABLE,
                                           fullWire.data(), fullWire.size())) {
                    peer.initialized = true;
                    peer.fieldHashes = currentHashes;
                    replicatedToAny = true;
                    // R5.3 (2026-08-24) Replay wire-tap: capture the
                    // authoritative Full Snapshot. The same `frameFlags`
                    // used by the receiver's interpolator (teleport vs
                    // normal) is preserved for v2 playback.
                    recordFullSnapshot(_replay, connectionId, _serverTick,
                                       frameFlags,
                                       fullWire.data(), fullWire.size());
                }
                continue;
            }

            std::vector<uint32_t> dirtyIndices;
            dirtyIndices.reserve(nFields);
            if (peer.fieldHashes.size() != currentHashes.size()) {
                peer.initialized = false;
                continue;
            }
            for (uint32_t k = 0; k < nFields; ++k) {
                if (currentHashes[k] != peer.fieldHashes[k]) dirtyIndices.push_back(k);
            }
            if (dirtyIndices.empty()) continue;

            BitStream deltaBody;
            if (!ReflectSerializer::serializeDirtyFields(
                    e.type, e.obj, netId, dirtyIndices, deltaBody, _serverTick)) continue;
            auto deltaWire = PacketCodec::encode(
                static_cast<const uint8_t*>(deltaBody.getData()), deltaBody.getSize(),
                kMsgTypeDelta, kSchemaVersion, CHANNEL_UNRELIABLE,
                0, 0, false);
            if (sendSealedToConnection(target, CHANNEL_UNRELIABLE,
                                       deltaWire.data(), deltaWire.size())) {
                for (uint32_t k : dirtyIndices) peer.fieldHashes[k] = currentHashes[k];
                replicatedToAny = true;
                // R5.3 (2026-08-24) Replay wire-tap: capture Delta frame.
                recordDeltaSnapshot(_replay, connectionId, _serverTick,
                                    /*frameFlags=*/ 0,
                                    deltaWire.data(), deltaWire.size());
            }
        }

        if (replicatedToAny && _extension) {
            _extension->onPostReplicate(e.obj, e.type, netId);
        }

        // R5.1: drain the teleport marker AFTER a successful emission so
        // the next tick returns to normal dirty tracking. We only drain
        // when at least one peer actually received the teleport frame —
        // otherwise (e.g. nobody is in range this tick) the marker
        // persists and the next tick with a visible peer still fires the
        // teleport. markTeleported() is idempotent so callers that
        // re-mark every tick are unaffected.
        if (teleport && replicatedToAny) {
            _teleportPending.erase(netId);
        }
    }

    // R5.3 (2026-08-24) Replay: end-of-tick flush ensures any wire-tap
    // recordEvent calls made above hit the .ayrp file before the next
    // tick begins. Idempotent on a null recorder.
    if (_replay) _replay->flush();
}

// =============================================================================
// onReceive — body-level dispatch by PacketHeader msgType. Authority gate: if this
// end is the server AND the sender is a client, replicate/spawn frames are
// dropped (§6.6 v1 = Server 权威). Client→server EntitySpawn/Replicate frames
// are silently logged at debug verbosity.
//
// The body has already been unsealed by PacketCodec (CRC checked, lz4
// decompressed, fragments reassembled). The envelope msgType is passed
// separately so the body contains payload only.
// =============================================================================
bool ReplicationManager::onReceive(uint16_t messageType, BitStream& stream, NetConnection* /*from*/) {
    switch (messageType) {
        case kMsgTypeReplication:
        case kMsgTypeDelta: {
            // R5.0 wire format: body starts with a 4-byte serverTick prefix
            // followed by the 14-byte FrameHeader. R3.x senders don't write
            // the prefix (the legacy serializer call with serverTick=0 emits
            // no prefix). We try the R5.0 layout first; if the resulting
            // netId/schemaHash don't validate against a known object, we
            // rewind and retry as R3.x.
            const size_t startPos = stream.getBitPosition();
            uint32_t serverTick = 0;
            bool triedPrefix = ReflectSerializer::readServerTick(stream, serverTick);
            ReflectSerializer::FrameHeader hdr;
            if (!ReflectSerializer::readReplicationFrameHeader(stream, hdr)) {
                // Truncated frame — try R3.x layout in case the first 4
                // bytes are the netId, not a serverTick.
                stream.setBitPosition(startPos);
                serverTick = 0;
                triedPrefix = false;
                if (!ReflectSerializer::readReplicationFrameHeader(stream, hdr)) return false;
            }
            // Look up the local object's type info
            const auto* type = findType(hdr.netId);
            if (!type) {
                // Unknown netId — might be R3.x frame read as R5.0 (we
                // accidentally consumed 4 bytes that were the netId).
                // Rewind and try the legacy path before giving up.
                if (triedPrefix) {
                    stream.setBitPosition(startPos);
                    serverTick = 0;
                    triedPrefix = false;
                    if (!ReflectSerializer::readReplicationFrameHeader(stream, hdr)) return false;
                    type = findType(hdr.netId);
                    if (!type) return false;
                } else {
                    return false;
                }
            }
            void* obj = findObject(hdr.netId);
            if (!obj) return false;
            const uint64_t expectedSchema = ReflectSerializer::hashTypeSchema(type);
            if (expectedSchema == 0 || hdr.schemaHash != expectedSchema) {
                // Schema mismatch — also try the R3.x fallback.
                if (triedPrefix) {
                    stream.setBitPosition(startPos);
                    serverTick = 0;
                    triedPrefix = false;
                    if (!ReflectSerializer::readReplicationFrameHeader(stream, hdr)) return false;
                    type = findType(hdr.netId);
                    if (!type) return false;
                    obj = findObject(hdr.netId);
                    if (!obj) return false;
                    const uint64_t retrySchema = ReflectSerializer::hashTypeSchema(type);
                    if (retrySchema == 0 || hdr.schemaHash != retrySchema) return false;
                } else {
                    return false;
                }
            }
            ReflectSerializer::FieldAppliedFn onFieldApplied;
            if (_extension) {
                onFieldApplied = [this, obj, type, netId = hdr.netId](const ayt::reflect::IFieldInfo* field) {
                    if (!field) return;
                    if (!field->hasAttribute(ayt::reflect::FieldAttribute::RepNotify)) return;
                    _extension->onRepNotify(obj, type, netId, field->getName());
                };
            }
            const bool decoded = ReflectSerializer::deserializeObject(type, obj, stream, hdr.fieldCount, onFieldApplied);

            // R5.2 (2026-08-24): Full Snapshot bodies may carry an optional
            // 8-byte ack tail when the destination connection owns an
            // AutonomousProxy ghost. We always ATTEMPT to read it; if the
            // sender didn't emit one, the byte stream is exhausted exactly
            // at the end and readAckTail returns true with present=false.
            // Delta frames (R5.2 decision: do not carry the tail) reach
            // here with no remaining bytes and also no-op cleanly.
            //
            // The "always attempt" semantic is safe because the writer's
            // emit gate is also strict: no AutonomousProxy ghost in frame
            // → no tail bytes. A sender bug that emits a tail to a
            // SimulatedProxy-only destination would surface as 8 trailing
            // bytes that readAckTail happily consumes — but the
            // client-side ack progress is harmless (no client ever
            // uploaded inputs in that case).
            ReflectSerializer::AckTail ackTail;
            ackTail.present = true; // ask the reader to attempt
            const bool tailReadOk = ReflectSerializer::readAckTail(stream, ackTail);
            if (!tailReadOk) ackTail.present = false; // sender didn't emit one
            if (ackTail.present && _prediction && !isAuthority()) {
                // Client-side: forward the ack to the prediction manager
                // so the local input ring advances its acked cursor.
                _prediction->onServerAck(hdr.netId,
                                         ackTail.lastAckedInputTick,
                                         ackTail.serverCommandAge);
            }

            // R5.0: hand the freshly-decoded object to the interpolator so
            // the per-ghost history buffer accumulates ticks. The object
            // pointer is the same one we just deserialized into, so the
            // interpolator memcpy's its bytes into the ring. serverTick==0
            // is treated as "unknown" and skipped — it indicates a legacy
            // R3.x sender that didn't stamp the prefix.
            //
            // R5.1: forward the FrameHeader's teleport flag so the
            // interpolator marks this record as snap. Production teleport
            // frames are always Full Snapshots, but Delta frames also
            // carry the flag and we honour it on either envelope so the
            // wire contract stays orthogonal.
            if (decoded && _snapshotInterpolator && serverTick != 0) {
                _snapshotInterpolator->push(hdr.netId, serverTick, obj,
                                            hdr.isTeleport());
            }
            return decoded;
        }
        case kMsgTypeEntitySpawn: {
            uint32_t netId; uint64_t schemaHash;
            if (!ReflectSerializer::readEntitySpawn(stream, netId, schemaHash)) return false;
            if (isAuthority()) {
                // Clients must not spawn authoritative objects on the server.
                return false;
            }
            _spawnAnnouncements[netId] = schemaHash;
            return true;
        }
        case kMsgTypeEntityDespawn: {
            uint32_t netId;
            if (!ReflectSerializer::readEntityDespawn(stream, netId)) return false;
            // Drop the local registration if present.
            _objects.erase(netId);
            // R5.0: tell the interpolator to forget about this ghost too.
            if (_snapshotInterpolator) _snapshotInterpolator->unregisterGhost(netId);
            return true;
        }
        default:
            return false;
    }
}

// =============================================================================
// forceReplicate marks every known peer as not-yet-initialized so its next
// relevant tick emits a Full Snapshot regardless of dirty state.
//
// Each peer returns to initialized after its Full Snapshot is sent
// successfully; subsequent ticks resume per-peer dirty tracking.
// =============================================================================
void ReplicationManager::forceReplicate(uint32_t netId) {
    auto it = _objects.find(netId);
    if (it == _objects.end()) return;
    for (auto& [connectionId, peer] : it->second._peers) {
        (void)connectionId;
        peer.initialized = false;
    }
}

// =============================================================================
// R5.1 (2026-08-24) markTeleported.
//
// Sets a one-shot marker on netId. The next tick() observes the marker,
// emits a Full Snapshot with the kFlagTeleport flag set (instead of a
// normal Full or Delta), and clears the marker. Clients receiving the
// frame push it into the interpolator with teleport=true, which causes
// sample() to skip the lerp from the previous snapshot.
//
// Markers are drained independently per-netId so multiple teleports in
// the same tick emit one teleport frame each (no coalescing — a
// teleport that fires every tick should be re-marked every tick).
//
// markTeleported() forces the per-peer init reset the same way
// forceReplicate does. Without it, a teleport marker on an idle object
// (no dirty fields) would still need a Full to convey the new state;
// forceReplicate's reset handles that uniformly with the rest of the
// dirty-tracking machinery.
// =============================================================================
void ReplicationManager::markTeleported(uint32_t netId) {
    auto it = _objects.find(netId);
    if (it == _objects.end()) return;
    _teleportPending.insert(netId);
    // Mirror forceReplicate: teleport implies "send the whole object
    // state again", which is what an uninitialized peer expects. This
    // also lets the dirty-tracking code path stay single-branch.
    for (auto& [connectionId, peer] : it->second._peers) {
        (void)connectionId;
        peer.initialized = false;
    }
}

bool ReplicationManager::rebroadcastEntitySpawn(uint32_t netId, NetConnection* targetConn) {
    // R4.1-A late-join fix (fe21587): resend EntitySpawn for an already-registered
    // netId to a single late-joining connection (targetConn != null) OR to all
    // connected clients (targetConn == nullptr).
    //
    // Audit (2026-08-02) closed two issues:
    //   1. sealed bytes were constructed BEFORE the isAuthority/network guards,
    //      wasting a CRC32C pass on every early-return path. Build sealed only
    //      after guards pass.
    //   2. When both _broadcastSink and targetConn were set, the sink path
    //      fired once WITHOUT honouring targetConn (broadcast semantics),
    //      contradicting the documented "late joiner" contract. Split the
    //      broadcastSink branch per-target so the late-joiner's test capture
    //      is isolated from a real netId-targeted send.
    auto it = _objects.find(netId);
    if (it == _objects.end() || !it->second.type) {
        return false;
    }
    if (!isAuthority()) {
        return false;
    }
    const bool hasNetwork   = (_network != nullptr);
    const bool hasSink      = (_broadcastSink != nullptr);
    if (!hasNetwork && !hasSink) {
        return false;
    }

    if (targetConn && !targetConn->isConnected()) return false;
    ReflectedEntry& e = it->second;

    // Build the spawn frame AFTER the guards pass — saves the CRC + alloc
    // on misrouted calls (the common hot path; see design §6.6 v1).
    BitStream body;
    ReflectSerializer::writeEntitySpawn(body, netId, ReflectSerializer::hashTypeSchema(e.type));
    std::vector<uint8_t> sealed = PacketCodec::encode(
        static_cast<const uint8_t*>(body.getData()), body.getSize(),
        kMsgTypeEntitySpawn, kSchemaVersion,
        CHANNEL_RELIABLE, /*flags=*/ 0, /*timestampMs=*/ 0,
        /*compress=*/ false);

    std::vector<NetConnection*> targets;
    if (targetConn) {
        targets.push_back(targetConn);
    } else {
        targets = buildInterestTargets(e.obj, e.type, netId, e._location, e._hasLocation);
    }
    if (targets.empty() && hasSink) targets.push_back(nullptr);

    BitStream fullBody;
    if (!ReflectSerializer::serializeObject(e.type, e.obj, netId, fullBody, _serverTick)) return false;
    auto fullWire = PacketCodec::encode(
        static_cast<const uint8_t*>(fullBody.getData()), fullBody.getSize(),
        kMsgTypeReplication, kSchemaVersion, CHANNEL_RELIABLE, 0, 0, false);

    bool delivered = false;
    for (NetConnection* target : targets) {
        if (!sendSealedToConnection(target, CHANNEL_RELIABLE, sealed.data(), sealed.size())) continue;
        if (!sendSealedToConnection(target, CHANNEL_RELIABLE, fullWire.data(), fullWire.size())) continue;
        const uint32_t connectionId = target ? target->getId() : 0u;
        // R5.3 (2026-08-24) Replay wire-tap: rebroadcast EntitySpawn.
        recordSpawn(_replay, connectionId, _serverTick,
                    netId, ReflectSerializer::hashTypeSchema(e.type),
                    sealed.data(), sealed.size());
        recordFullSnapshot(_replay, connectionId, _serverTick, /*frameFlags=*/ 0,
                           fullWire.data(), fullWire.size());
        auto& peer = e._peers[connectionId];
        peer.visible = true;
        peer.initialized = true;
        peer.fieldHashes.resize(e._netFieldSparseIndex.size());
        for (uint32_t k = 0; k < e._netFieldSparseIndex.size(); ++k) {
            const auto* field = e.type->getField(e._netFieldSparseIndex[k]);
            WireTypeId wid;
            if (!field || !ReflectSerializer::resolveWireTypeId(field->getType(), wid)) continue;
            peer.fieldHashes[k] = ReflectSerializer::hashFieldValueEx(
                wid, field->getType(), field->get(e.obj));
        }
        delivered = true;
    }
    return delivered;
}

// =============================================================================
// getDirtyFieldCount — debug / test helper. Returns the count of NetReplicate
// fields currently considered dirty. 0 = steady state (no frame on next tick).
// SIZE_MAX signals "netId not registered" or "legacy IReplicable path" (no
// type info, no dirty tracking). The value is computed by walking each field
// and hashing it, just like tick() — caller should treat this as O(N) per call.
// =============================================================================
size_t ReplicationManager::getDirtyFieldCount(uint32_t netId) const {
    auto it = _objects.find(netId);
    if (it == _objects.end()) return SIZE_MAX;
    const ReflectedEntry& e = it->second;
    if (!e.type) return SIZE_MAX;
    if (e._peers.empty()) return e._netFieldSparseIndex.size();

    size_t dirtyCount = 0;
    const uint32_t nFields = static_cast<uint32_t>(e._netFieldSparseIndex.size());
    for (uint32_t k = 0; k < nFields; ++k) {
        const auto* field = e.type->getField(e._netFieldSparseIndex[k]);
        if (!field) continue;
        WireTypeId wid;
        if (!ReflectSerializer::resolveWireTypeId(field->getType(), wid)) continue;
        const uint32_t cur = ReflectSerializer::hashFieldValueEx(wid, field->getType(), field->get(e.obj));
        for (const auto& [connectionId, peer] : e._peers) {
            (void)connectionId;
            if (!peer.initialized || peer.fieldHashes.size() != nFields || cur != peer.fieldHashes[k]) {
                ++dirtyCount;
                break;
            }
        }
    }
    return dirtyCount;
}

void ReplicationManager::setExtension(INetworkExtension* ext) {
    _extension = ext;
}

// =============================================================================
// isAuthority — R4.1 helper. Server (dedicated) AND ListenServer (host) are
// both server-authoritative for replication (design §6.6 v1). R4.0 hard-coded
// `mode == ConnectionMode::Server` everywhere, which silently dropped
// ListenServer tick/spawn/despawn because AYNetworkSubSystem::listen() set
// Server (it should have set ListenServer — also fixed in R4.1). R4.1
// canonicalizes the check into this helper so all 4 gates stay in lockstep.
// =============================================================================
bool ReplicationManager::isAuthority() const {
    const auto m = getEffectiveMode();
    return m == ConnectionMode::Server || m == ConnectionMode::ListenServer;
}

bool ReplicationManager::peekSpawnAnnouncement(uint32_t netId, uint64_t& schemaHashOut) const {
    auto it = _spawnAnnouncements.find(netId);
    if (it == _spawnAnnouncements.end()) return false;
    schemaHashOut = it->second;
    return true;
}

// =============================================================================
// R5.0 (2026-08-24) Snapshot Interpolation server-side helpers.
//
// The manager exposes a monotonic tick counter that the replication layer
// stamps into every emitted Full Snapshot / Delta frame body. The counter
// advances once per tick() at the configured rate; the default 30 Hz
// matches Unity NetCode's SnapshotSystem default.
//
// setServerTickRate validates the input (≤0 falls back to 30 Hz) and is
// safe to call while the manager is in use — the rate only affects
// advanceServerTick()'s accumulator math.
//
// setSnapshotInterpolator hands the manager a non-owning pointer to a
// client-side interpolator. onReceive will push every successfully
// decoded frame into it; EntityDespawn unregisters the ghost. The
// interpolator must outlive the manager.
// =============================================================================
void ReplicationManager::setServerTickRate(double hz) {
    _serverTickRate = (hz > 0.0) ? hz : 30.0;
    _serverTickAccumulator = 0.0;
}

void ReplicationManager::advanceServerTick(double dtSec) {
    if (dtSec <= 0.0 || _serverTickRate <= 0.0) return;
    _serverTickAccumulator += dtSec * _serverTickRate;
    uint32_t wholeTicks = static_cast<uint32_t>(_serverTickAccumulator);
    if (wholeTicks > 64u) wholeTicks = 64u;
    _serverTickAccumulator -= static_cast<double>(wholeTicks);
    _serverTick += wholeTicks;
}

void ReplicationManager::setSnapshotInterpolator(SnapshotInterpolator* si) {
    _snapshotInterpolator = si;
}

// =============================================================================
// R5.2 (2026-08-24) Client Prediction accessors + consumeClientInputs.
//
// The default ProxyKind for any newly registered netId is SimulatedProxy
// (set in registerObject). Authority flips to AutonomousProxy via
// setObjectProxyKind after registration. The setter is a no-op when the
// netId isn't registered — prevents typos from creating phantom rows.
// =============================================================================
void ReplicationManager::setObjectProxyKind(uint32_t netId, ProxyKind kind) {
    auto it = _objects.find(netId);
    if (it == _objects.end()) return; // only valid for registered ghosts
    const ProxyKind old = getObjectProxyKind(netId);
    _proxyKinds[netId] = kind;

    // R5.3 (2026-08-24) Replay wire-tap: capture Ownership/Authority flips.
    // Authority gate is implicit — this method is meaningful on both
    // authority and client, but ownership transfers only happen on the
    // server side, so the recorder records the event either way. The
    // payload layout: [u32 netId][u8 oldKind][u8 newKind][u32 connectionId].
    if (_replay && old != kind) {
        uint8_t buf[14];
        packU32LE(buf + 0, netId);
        buf[4] = static_cast<uint8_t>(old);
        buf[5] = static_cast<uint8_t>(kind);
        // connectionId field: we don't track per-kind owning conn here
        // (would require expanding ReflectedEntry); emit 0 for the
        // common "no specific owner" case. Future: plumb in the owning
        // connection when setObjectProxyKind gains that parameter.
        packU32LE(buf + 6, /*connectionId=*/ 0);
        buf[10] = buf[11] = buf[12] = buf[13] = 0;
        _replay->recordEvent(_serverTick,
                             ayt::net::replay::kEvtNet_AuthorityChange,
                             buf, sizeof(buf));
    }
}

ProxyKind ReplicationManager::getObjectProxyKind(uint32_t netId) const {
    auto it = _proxyKinds.find(netId);
    if (it == _proxyKinds.end()) return ProxyKind::SimulatedProxy;
    return it->second;
}

bool ReplicationManager::isLocallyControlled(uint32_t netId) const {
    return getObjectProxyKind(netId) == ProxyKind::AutonomousProxy
        && !isAuthority();
}

uint32_t ReplicationManager::getLastAckedInputTick(uint32_t connectionId) const {
    auto it = _lastAckedInputTick.find(connectionId);
    return it != _lastAckedInputTick.end() ? it->second : 0u;
}

void ReplicationManager::consumeClientInputs(uint32_t simTick) {
    if (!isAuthority()) return; // clients never consume inputs
    if (!_prediction) return;

    // The application callback records (connectionId, inputSeq) per consume
    // so the next Full Snapshot can carry them in the ack tail. The capture
    // is by value: the std::function copy is one heap alloc per fixed tick,
    // off the hot path.
    ApplyInputFn fn = [this](uint32_t connectionId,
                             uint32_t inputSeq,
                             const uint8_t* payload,
                             size_t payloadSize) {
        if (_inputApplicationFn) {
            _inputApplicationFn(connectionId, inputSeq, payload, payloadSize);
        }
        // Always advance the per-connection last-acked seq so the wire
        // ack tail advances even when the app didn't bind a callback
        // (test/server-only case).
        auto it = _lastAckedInputTick.find(connectionId);
        if (it == _lastAckedInputTick.end()
            || seqGreaterThan(inputSeq, it->second)) {
            _lastAckedInputTick[connectionId] = inputSeq;
        }
    };
    _prediction->consumeClientInputs(simTick, fn);
}

bool ReplicationManager::onClientInput(uint32_t connectionId,
                                       const uint8_t* body, size_t bodySize) {
    if (!isAuthority()) return false; // only server receives inputs
    if (!_prediction)   return false;
    return _prediction->onClientInput(connectionId, body, bodySize);
}

ReplicationManager::AckTailInfo
ReplicationManager::buildAckTailForConnection(uint32_t connectionId,
                                              uint32_t serverCommandAge) const {
    AckTailInfo info;
    if (!isAuthority()) return info; // present=false default
    // Only emit the tail if at least one AutonomousProxy ghost is
    // registered on the manager (server-side gating). Per-connection
    // ownership metadata is intentionally not tracked in R5.2 — would
    // require an extra map. Heuristic: any AutonomousProxy ghost ⇒ emit
    // tail; clients ignore unknown tails safely.
    bool ownsAutonomous = false;
    for (const auto& kv : _proxyKinds) {
        if (kv.second != ProxyKind::AutonomousProxy) continue;
        ownsAutonomous = true;
        break;
    }
    if (!ownsAutonomous) return info; // present=false
    auto it = _lastAckedInputTick.find(connectionId);
    if (it == _lastAckedInputTick.end()) return info; // no inputs seen
    info.lastAckedInputTick = it->second;
    info.serverCommandAge   = serverCommandAge;
    info.present            = true;
    return info;
}

} // namespace ayt::net
