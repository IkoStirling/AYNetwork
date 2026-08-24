// ReplicationManager.cpp - R3.0 Replication manager implementation

#include <AYNetwork/Replication/ReplicationManager.h>

#include <AYNetwork/Replication/ReflectSerializer.h>
#include <AYNetwork/Protocol/PacketCodec.h>
#include <AYNetwork/Transport/GnsConnection.h>
#include <AYNetwork/INetwork.h>

#include <AYReflect/IReflect.h>

#include <cstdio>

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
//   - _fieldHashes[denseIdx] = CRC32C of the last successfully broadcast
//     value for that field. The invariant after each tick() is:
//       _fieldHashes[i] == hash of the receiver-side current value of field i
//   - _initialized is false until the first tick has emitted the Full
//     Snapshot; the next tick uses _fieldHashes as the comparison baseline.
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
{
}

ReplicationManager::~ReplicationManager() {
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
        _network->broadcast(channel, data, size);
        return true;
    }
    for (NetConnection* conn : targets) {
        _network->sendTo(conn, channel, data, size);
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
    _network->sendTo(target, channel, data, size);
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

    // R3.1: build the dense→sparse mapping once, sized to the NetReplicate
    // field count. _fieldHashes stays zero-initialized so the first tick
    // comparison forces a Full Snapshot (every field's current hash != 0
    // sentinel, OR _initialized==false gate below — see tick()).
    buildNetFieldMap(type, e._netFieldSparseIndex);
    _objects[netId] = e;
    _spawnAnnouncements.erase(netId);
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
            }
        }
    }
    _objects.erase(it);
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
    // netId, but with typeHash=0 (client treats this as "untyped").
    if (isAuthority() && (_network || _broadcastSink)) {
        BitStream body;
        ReflectSerializer::writeEntitySpawn(body, netId, /*schemaHash=*/ 0);
        std::vector<uint8_t> sealed = PacketCodec::encode(
            static_cast<const uint8_t*>(body.getData()), body.getSize(),
            kMsgTypeEntitySpawn, kSchemaVersion,
            CHANNEL_RELIABLE, /*flags=*/ 0, /*timestampMs=*/ 0,
            /*compress=*/ false);
        if (_broadcastSink) _broadcastSink(CHANNEL_RELIABLE, sealed.data(), sealed.size());
        else                _network->broadcast(CHANNEL_RELIABLE, sealed.data(), sealed.size());
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
// R3.1 (2026-07-27): per-NetReplicate-field CRC32C baseline in
//                   ReflectedEntry::_fieldHashes; tick() compares current
//                   hashes against the baseline and emits ONLY the dirty
//                   fields. First tick (or after forceReplicate) emits Full.
//                   Steady state with zero changes emits NO frame.
// =============================================================================
void ReplicationManager::tick(float /*deltaTime*/) {
    if (!_network && !_broadcastSink) return;

    // Authority gate: only the server emits frames. Clients do nothing on
    // tick — they only deserialize frames received via onReceive.
    // R4.1: gate accepts Server (dedicated) AND ListenServer (host) via
    // isAuthority() helper.
    if (!isAuthority()) return;

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
            }
            peerIt = e._peers.erase(peerIt);
        }

        bool replicatedToAny = false;
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
                peer.visible = true;
                peer.initialized = false;
                peer.fieldHashes.clear();
            }

            if (!peer.initialized) {
                BitStream fullBody;
                if (!ReflectSerializer::serializeObject(e.type, e.obj, netId, fullBody)) continue;
                auto fullWire = PacketCodec::encode(
                    static_cast<const uint8_t*>(fullBody.getData()), fullBody.getSize(),
                    kMsgTypeReplication, kSchemaVersion, CHANNEL_RELIABLE,
                    0, 0, false);
                if (sendSealedToConnection(target, CHANNEL_RELIABLE,
                                           fullWire.data(), fullWire.size())) {
                    peer.initialized = true;
                    peer.fieldHashes = currentHashes;
                    replicatedToAny = true;
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
                    e.type, e.obj, netId, dirtyIndices, deltaBody)) continue;
            auto deltaWire = PacketCodec::encode(
                static_cast<const uint8_t*>(deltaBody.getData()), deltaBody.getSize(),
                kMsgTypeDelta, kSchemaVersion, CHANNEL_UNRELIABLE,
                0, 0, false);
            if (sendSealedToConnection(target, CHANNEL_UNRELIABLE,
                                       deltaWire.data(), deltaWire.size())) {
                for (uint32_t k : dirtyIndices) peer.fieldHashes[k] = currentHashes[k];
                replicatedToAny = true;
            }
        }

        if (replicatedToAny && _extension) {
            _extension->onPostReplicate(e.obj, e.type, netId);
        }
    }
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
            // R3.1: Delta frames share the wire format with Full Snapshots
            // (same 8B header, same field records). The receiver doesn't
            // need to distinguish — just walk the same deserializeObject.
            ReflectSerializer::FrameHeader hdr;
            if (!ReflectSerializer::readReplicationFrameHeader(stream, hdr)) return false;
            // Look up the local object's type info
            const auto* type = findType(hdr.netId);
            if (!type) {
                // Server side receiving a client-emitted snapshot — drop silently.
                // (R3.0 server should never see this; clients don't tick().)
                return false;
            }
            void* obj = findObject(hdr.netId);
            if (!obj) return false;
            const uint64_t expectedSchema = ReflectSerializer::hashTypeSchema(type);
            if (expectedSchema == 0 || hdr.schemaHash != expectedSchema) return false;
            ReflectSerializer::FieldAppliedFn onFieldApplied;
            if (_extension) {
                onFieldApplied = [this, obj, type, netId = hdr.netId](const ayt::reflect::IFieldInfo* field) {
                    if (!field) return;
                    if (!field->hasAttribute(ayt::reflect::FieldAttribute::RepNotify)) return;
                    _extension->onRepNotify(obj, type, netId, field->getName());
                };
            }
            return ReflectSerializer::deserializeObject(type, obj, stream, hdr.fieldCount, onFieldApplied);
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
            return true;
        }
        default:
            return false;
    }
}

// =============================================================================
// forceReplicate — R3.1 real implementation: marks the entry as not-yet-
// initialized so the next tick emits a Full Snapshot regardless of dirty
// state. Useful after a client reconnects, after a teleport, or after the
// user explicitly changes a server-side field that all clients must observe.
//
// One-shot behavior: the next tick() sets _initialized=true after emitting
// the Full Snapshot; subsequent ticks resume dirty-tracking. Callers that
// want repeated Full Snapshots must call forceReplicate each tick.
// =============================================================================
void ReplicationManager::forceReplicate(uint32_t netId) {
    auto it = _objects.find(netId);
    if (it == _objects.end()) return;
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
    if (!ReflectSerializer::serializeObject(e.type, e.obj, netId, fullBody)) return false;
    auto fullWire = PacketCodec::encode(
        static_cast<const uint8_t*>(fullBody.getData()), fullBody.getSize(),
        kMsgTypeReplication, kSchemaVersion, CHANNEL_RELIABLE, 0, 0, false);

    bool delivered = false;
    for (NetConnection* target : targets) {
        if (!sendSealedToConnection(target, CHANNEL_RELIABLE, sealed.data(), sealed.size())) continue;
        if (!sendSealedToConnection(target, CHANNEL_RELIABLE, fullWire.data(), fullWire.size())) continue;
        const uint32_t connectionId = target ? target->getId() : 0u;
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

} // namespace ayt::net
