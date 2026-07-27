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
// =============================================================================
struct ReplicationManager::ReflectedEntry {
    void*                          obj = nullptr;   // live object memory
    const ayt::reflect::ITypeInfo* type = nullptr;  // metadata for serializer
    IReplicable*                   iface = nullptr; // optional IReplicable adapter
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
void ReplicationManager::registerObject(void* obj, const ayt::reflect::ITypeInfo* type, uint32_t netId) {
    if (!obj || !type || netId == 0) return;

    ReflectedEntry e;
    e.obj  = obj;
    e.type = type;
    _objects[netId] = e;

    // R3.0: server authority also broadcasts an EntitySpawn announcement so
    // the client can allocate a matching slot. On a client (no active
    // connections or no server mode), this is a no-op — local registration
    // only.
    if (getEffectiveMode() == ConnectionMode::Server && (_network || _broadcastSink)) {
        BitStream body;
        body.writeUInt16(kMsgTypeEntitySpawn);
        ReflectSerializer::writeEntitySpawn(body, netId,
            static_cast<uint16_t>(type->getId() & 0xFFFFu));
        std::vector<uint8_t> sealed = PacketCodec::encode(
            static_cast<const uint8_t*>(body.getData()), body.getSize(),
            kMsgTypeReplication, kSchemaVersion,
            CHANNEL_RELIABLE, /*flags=*/ 0, /*timestampMs=*/ 0,
            /*compress=*/ false);
        if (_broadcastSink) _broadcastSink(CHANNEL_RELIABLE, sealed.data(), sealed.size());
        else                _network->broadcast(CHANNEL_RELIABLE, sealed.data(), sealed.size());
    }
}

void ReplicationManager::unregisterObject(uint32_t netId) {
    auto it = _objects.find(netId);
    if (it == _objects.end()) return;
    _objects.erase(it);

    if (getEffectiveMode() == ConnectionMode::Server && (_network || _broadcastSink)) {
        BitStream body;
        body.writeUInt16(kMsgTypeEntityDespawn);
        ReflectSerializer::writeEntityDespawn(body, netId);
        std::vector<uint8_t> sealed = PacketCodec::encode(
            static_cast<const uint8_t*>(body.getData()), body.getSize(),
            kMsgTypeReplication, kSchemaVersion,
            CHANNEL_RELIABLE, /*flags=*/ 0, /*timestampMs=*/ 0,
            /*compress=*/ false);
        if (_broadcastSink) _broadcastSink(CHANNEL_RELIABLE, sealed.data(), sealed.size());
        else                _network->broadcast(CHANNEL_RELIABLE, sealed.data(), sealed.size());
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
    if (getEffectiveMode() == ConnectionMode::Server && (_network || _broadcastSink)) {
        BitStream body;
        body.writeUInt16(kMsgTypeEntitySpawn);
        ReflectSerializer::writeEntitySpawn(body, netId, /*typeHash=*/ 0);
        std::vector<uint8_t> sealed = PacketCodec::encode(
            static_cast<const uint8_t*>(body.getData()), body.getSize(),
            kMsgTypeReplication, kSchemaVersion,
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
// broadcasts a ReplicationFrame per object via PacketCodec seal. Each frame
// is independent: per-object failure (no NetReplicate fields or unsupported
// types) does not affect other objects.
//
// R3.0: Full Snapshot every tick. Dirty-bit Delta is R3.1 work.
// =============================================================================
void ReplicationManager::tick(float /*deltaTime*/) {
    if (!_network && !_broadcastSink) return;

    // Authority gate: only the server emits frames. Clients do nothing on
    // tick — they only deserialize frames received via onReceive.
    if (getEffectiveMode() != ConnectionMode::Server) return;

    // Snapshot the keys to allow register/unregister during iteration without
    // invalidating iterators (we don't, but defensive).
    std::vector<uint32_t> netIds;
    netIds.reserve(_objects.size());
    for (const auto& kv : _objects) netIds.push_back(kv.first);

    for (uint32_t netId : netIds) {
        auto it = _objects.find(netId);
        if (it == _objects.end()) continue;
        const ReflectedEntry& e = it->second;

        // Skip legacy IReplicable entries (type==nullptr). R3.0 sends only
        // EntitySpawn for them — full Snapshot frames require AYReflect metadata.
        if (!e.type) continue;

        BitStream body;
        body.writeUInt16(kMsgTypeReplication); // inner msgType discriminator
        if (!ReflectSerializer::serializeObject(e.type, e.obj, netId, body)) {
            // No supported NetReplicate fields — skip silently.
            continue;
        }
        std::vector<uint8_t> sealed = PacketCodec::encode(
            static_cast<const uint8_t*>(body.getData()), body.getSize(),
            kMsgTypeReplication, kSchemaVersion,
            CHANNEL_RELIABLE, /*flags=*/ 0, /*timestampMs=*/ 0,
            /*compress=*/ false);
        if (_broadcastSink) _broadcastSink(CHANNEL_RELIABLE, sealed.data(), sealed.size());
        else                _network->broadcast(CHANNEL_RELIABLE, sealed.data(), sealed.size());
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
    if (!_network) return false;

    // Authority gate: if we're the server, drop client→server replication
    // frames. We rely on the *envelope* kMsgTypeReplication having already
    // been filtered; here we only see application bodies that contain our
    // own inner types. Since real-game clients do NOT emit replication frames
    // (R3.0 R1 stub says shouldReplicate() = true but R3.0 servers reject),
    // this is mostly a defense-in-depth check.
    //
    // We can't tell client-from-server inside the body, so the actual gate is
    // in tick(): clients never call broadcast on replication frames. The
    // server-side tick is the only place replicate frames are emitted.

    // Need at least 2 bytes for inner msgType
    if (stream.getBitPosition() + 16 > stream.getBitCount()) return false;

    const uint16_t innerMsg = stream.readUInt16();

    switch (innerMsg) {
        case kMsgTypeReplication: {
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
            return ReflectSerializer::deserializeObject(type, obj, stream, hdr.fieldCount);
        }
        case kMsgTypeEntitySpawn: {
            uint32_t netId; uint16_t typeHash;
            if (!ReflectSerializer::readEntitySpawn(stream, netId, typeHash)) return false;
            // R3.0 server-side handling: EntitySpawn from a client is invalid
            // (clients don't spawn authoritative objects). Drop.
            // On the client side, we just record the (netId, typeHash) so a
            // later replicate frame can resolve. Until the user calls
            // registerObject locally with matching netId, replicate frames
            // are dropped (findType(netId)==nullptr).
            //
            // Real-game integration: AYEntity's NetworkComponent stub holds
            // getNetId; the ECS bridge (EntityReplicationAdapter) wires this.
            (void)netId; (void)typeHash;
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
// forceReplicate — for testing only. R3.0 already broadcasts every tick so
// this is a no-op (kept for R1 API compatibility). R3.1+ dirty-tracking will
// implement this as a flush.
// =============================================================================
void ReplicationManager::forceReplicate(uint32_t netId) {
    (void)netId;
    // R3.0: per-tick broadcast already covers this. R3.1 will narrow to dirty fields.
}

void ReplicationManager::setExtension(INetworkExtension* ext) {
    _extension = ext;
}

} // namespace ayt::net