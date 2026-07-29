// ReplicationManager.cpp - R3.0 Replication manager implementation

#include <Replication/ReplicationManager.h>

#include <Replication/ReflectSerializer.h>
#include <Protocol/PacketCodec.h>
#include <Transport/GnsConnection.h>
#include <IAYNetwork.h>

#include <ayreflect/IReflect.h>

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

    // ---- R3.1 dirty-tracking ----
    std::vector<uint32_t> _fieldHashes;            // dense-indexed CRC32C cache
    std::vector<uint32_t> _netFieldSparseIndex;   // dense → type->getField() sparse
    bool                  _initialized = false;   // false until first Full Snapshot emitted

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
    e._fieldHashes.assign(e._netFieldSparseIndex.size(), 0u);
    e._initialized = false;

    _objects[netId] = e;
    _spawnAnnouncements.erase(netId);

    // R3.0: server authority also broadcasts an EntitySpawn announcement so
    // the client can allocate a matching slot. On a client (no active
    // connections or no server mode), this is a no-op — local registration
    // only.
    if (isAuthority() && (_network || _broadcastSink)) {
        BitStream body;
        body.writeUInt16(kMsgTypeEntitySpawn);
        ReflectSerializer::writeEntitySpawn(body, netId,
            static_cast<uint16_t>(type->getId() & 0xFFFFu));
        std::vector<uint8_t> sealed = PacketCodec::encode(
            static_cast<const uint8_t*>(body.getData()), body.getSize(),
            kMsgTypeEntitySpawn, kSchemaVersion,
            CHANNEL_RELIABLE, /*flags=*/ 0, /*timestampMs=*/ 0,
            /*compress=*/ false);
        sendSealedToTargets(obj, type, netId,
                            _objects[netId]._location, _objects[netId]._hasLocation,
                            CHANNEL_RELIABLE, sealed.data(), sealed.size());
    }
}

void ReplicationManager::unregisterObject(uint32_t netId) {
    auto it = _objects.find(netId);
    if (it == _objects.end()) return;
    void* obj = it->second.obj;
    const ayt::reflect::ITypeInfo* type = it->second.type;
    NetVec3 loc = it->second._location;
    const bool hasLoc = it->second._hasLocation;
    _objects.erase(it);

    if (isAuthority() && (_network || _broadcastSink)) {
        BitStream body;
        body.writeUInt16(kMsgTypeEntityDespawn);
        ReflectSerializer::writeEntityDespawn(body, netId);
        std::vector<uint8_t> sealed = PacketCodec::encode(
            static_cast<const uint8_t*>(body.getData()), body.getSize(),
            kMsgTypeEntityDespawn, kSchemaVersion,
            CHANNEL_RELIABLE, /*flags=*/ 0, /*timestampMs=*/ 0,
            /*compress=*/ false);
        sendSealedToTargets(obj, type, netId, loc, hasLoc,
                            CHANNEL_RELIABLE, sealed.data(), sealed.size());
    }
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
        body.writeUInt16(kMsgTypeEntitySpawn);
        ReflectSerializer::writeEntitySpawn(body, netId, /*typeHash=*/ 0);
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

    // Snapshot the keys to allow register/unregister during iteration without
    // invalidating iterators (we don't, but defensive).
    std::vector<uint32_t> netIds;
    netIds.reserve(_objects.size());
    for (const auto& kv : _objects) netIds.push_back(kv.first);

    for (uint32_t netId : netIds) {
        auto it = _objects.find(netId);
        if (it == _objects.end()) continue;
        ReflectedEntry& e = it->second;

        // Skip legacy IReplicable entries (type==nullptr). R3.0 sends only
        // EntitySpawn for them — full Snapshot frames require AYReflect metadata.
        if (!e.type) continue;

        // R4.1: FrameKind 提到循环顶部 (避免在 if-block 内定义然后下面引用
        // 超出 scope 编译错)。None = 稳态不广播；Full/Delta = 需要 emit。
        enum class FrameKind { None, Full, Delta };
        FrameKind frameKind = FrameKind::None;

        // Compute current CRC32C for each NetReplicate field. We always walk
        // every field even in steady state because we need the hash to compare
        // against _fieldHashes.
        const uint32_t nFields = static_cast<uint32_t>(e._netFieldSparseIndex.size());
        std::vector<uint32_t> currentHashes(nFields);
        std::vector<uint32_t> dirtyIndices; dirtyIndices.reserve(nFields);
        for (uint32_t k = 0; k < nFields; ++k) {
            const auto* field = e.type->getField(e._netFieldSparseIndex[k]);
            if (!field) continue; // shouldn't happen, but defensive
            WireTypeId wid;
            if (!ReflectSerializer::resolveWireTypeId(field->getType(), wid)) continue;
            // R3.2: hashFieldValueEx walks nested types (NestedStruct /
            // FixedArray / DynamicArray / StringMap). For primitives it
            // returns the same hash as hashFieldValue.
            currentHashes[k] = ReflectSerializer::hashFieldValueEx(wid, field->getType(), field->get(e.obj));
            if (!e._initialized || currentHashes[k] != e._fieldHashes[k]) {
                dirtyIndices.push_back(k);
            }
        }

        // Decision:
        //   - Not yet initialized → emit Full Snapshot (R3.0 path, RELIABLE)
        //   - No dirty fields (steady state) → emit nothing
        //   - Some dirty fields → emit Delta (R3.1 path, UNRELIABLE)
        //   - forceReplicate → set _initialized=false → next tick goes Full
        //
        // R4.1: 提取到 FrameKind enum 以便 tick 末尾按是否 emit 决定是否
        // 触发 onPostReplicate。Full 路径里先不直接 emit（把序列化 + emit
        // 提到外层统一处理），但保留 set/unset _initialized 语义。
        // (frameKind + FrameKind 已在循环顶部定义)
        BitStream body;
        if (!e._initialized) {
            body.writeUInt16(kMsgTypeReplication);
            if (ReflectSerializer::serializeObject(e.type, e.obj, netId, body)) {
                frameKind = FrameKind::Full;
            }
        }
        else if (!dirtyIndices.empty()) {
            body.writeUInt16(kMsgTypeDelta);
            if (ReflectSerializer::serializeDirtyFields(e.type, e.obj, netId, dirtyIndices, body)) {
                frameKind = FrameKind::Delta;
            }
        }

        if (frameKind != FrameKind::None) {
            const uint8_t channel = (frameKind == FrameKind::Full) ? CHANNEL_RELIABLE : CHANNEL_UNRELIABLE;
            const uint16_t envMsgType = (frameKind == FrameKind::Full) ? kMsgTypeReplication : kMsgTypeDelta;
            std::vector<uint8_t> sealed = PacketCodec::encode(
                static_cast<const uint8_t*>(body.getData()), body.getSize(),
                envMsgType, kSchemaVersion,
                channel, /*flags=*/ 0, /*timestampMs=*/ 0,
                /*compress=*/ false);
            const bool delivered = sendSealedToTargets(
                e.obj, e.type, netId, e._location, e._hasLocation,
                channel, sealed.data(), sealed.size());
            if (delivered) {
                if (frameKind == FrameKind::Full) {
                    e._initialized = true;
                    e._fieldHashes = currentHashes;
                } else {
                    for (uint32_t k : dirtyIndices) e._fieldHashes[k] = currentHashes[k];
                }
                if (_extension) {
                    _extension->onPostReplicate(e.obj, e.type, netId);
                }
            }
        }
    }
}

// =============================================================================
// onReceive — body-level dispatch by inner msgType. Authority gate: if this
// end is the server AND the sender is a client, replicate/spawn frames are
// dropped (§6.6 v1 = Server 权威). Client→server EntitySpawn/Replicate frames
// are silently logged at debug verbosity.
//
// The body has already been unsealed by GnsConnection (CRC checked, lz4
// decompressed, fragments reassembled). What's left is the inner [u16
// msgType][payload] prefix.
// =============================================================================
bool ReplicationManager::onReceive(BitStream& stream, NetConnection* /*from*/) {
    // Need at least 2 bytes for inner msgType
    if (stream.getBitPosition() + 16 > stream.getBitCount()) return false;

    const uint16_t innerMsg = stream.readUInt16();

    switch (innerMsg) {
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
            uint32_t netId; uint16_t typeHash;
            if (!ReflectSerializer::readEntitySpawn(stream, netId, typeHash)) return false;
            if (isAuthority()) {
                // Clients must not spawn authoritative objects on the server.
                return false;
            }
            _spawnAnnouncements[netId] = typeHash;
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
    it->second._initialized = false;
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
    if (!e._initialized) return e._fieldHashes.size(); // first tick = full

    size_t dirtyCount = 0;
    const uint32_t nFields = static_cast<uint32_t>(e._netFieldSparseIndex.size());
    for (uint32_t k = 0; k < nFields; ++k) {
        const auto* field = e.type->getField(e._netFieldSparseIndex[k]);
        if (!field) continue;
        WireTypeId wid;
        if (!ReflectSerializer::resolveWireTypeId(field->getType(), wid)) continue;
        const uint32_t cur = ReflectSerializer::hashFieldValueEx(wid, field->getType(), field->get(e.obj));
        if (cur != e._fieldHashes[k]) ++dirtyCount;
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

bool ReplicationManager::peekSpawnAnnouncement(uint32_t netId, uint16_t& typeHashOut) const {
    auto it = _spawnAnnouncements.find(netId);
    if (it == _spawnAnnouncements.end()) return false;
    typeHashOut = it->second;
    return true;
}

} // namespace ayt::net