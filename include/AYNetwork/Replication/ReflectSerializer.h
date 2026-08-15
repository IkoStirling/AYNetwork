#pragma once
// AYNetwork/Replication/AYNetwork/Replication/AYNetwork/Replication/ReflectSerializer.h - R3.0/R3.1 AYReflect-driven wire format serializer
//
// R3.0 (2026-07-27): serialization is driven entirely by ayt::reflect::ITypeInfo
// metadata. The user tags fields with FieldAttribute::NetReplicate; this header
// walks them and packs into the wire format below. The user does NOT need to
// implement any virtual.
//
// R3.1 (2026-07-27): adds serializeDirtyFields + hashFieldValue so the server
// can detect which fields actually changed since the last broadcast and pack
// only those into a Delta frame. Wire format is identical to the R3.0 Full
// Snapshot frame; receivers don't distinguish.
//
// Wire format (server → client):
//
//   ReplicationFrame / DeltaFrame body (one entity per frame):
//     [u32 netId]                       // 4 B
//     [u16 typeHash]                    // 2 B — ayt::reflect::ITypeInfo::getId()
//     [u8  fieldCount]                  // 1 B — number of fields in this frame
//     [u8  reserved]                    // 1 B — 0
//     [field records × fieldCount]
//
//   Field record:
//     [u16 fieldNameHash]               // 2 B — ayt::reflect::IFieldInfo::getName() hashed (FNV-1a 32, low 16 bits)
//     [u8  fieldTypeId]                 // 1 B — WireTypeId
//     [value bytes]                     // per WireTypeId (see BitStream raw helpers)
//
//   EntitySpawn body (server announces new replicated entity):
//     [u32 netId] [u16 typeHash]
//
//   EntityDespawn body:
//     [u32 netId]
//
// Unsupported field types (any ITypeInfo not in WireTypeId, including nested
// structs, arrays, pointers, std::string-in-struct-nested, etc.) cause
// serializeObject to return false — the field is silently skipped (logged at
// debug verbosity). R3.2+ extends WireTypeId range to cover more cases.

#include <AYNetwork/INetwork.h>

#include <cstdint>
#include <cstddef>
#include <functional>

namespace ayt::reflect { class ITypeInfo; }

namespace ayt::net
{

class ReflectSerializer {
public:
    // Pack a complete ReplicationFrame (header + records) for one entity into `s`.
    // Walks type's fields, filters by NetReplicate, resolves each to a WireTypeId,
    // emits records. The frame header's netId and typeHash come from the caller —
    // they're required because the wire format identifies which entity this is.
    // Returns false if obj/type is null or no NetReplicate field has a supported
    // type (frame is NOT written in that case — caller can drop safely).
    static bool serializeObject(const ayt::reflect::ITypeInfo* type, const void* obj,
                                uint32_t netId, BitStream& s);

    // R3.1 (2026-07-27): serialize ONLY the NetReplicate fields whose
    // dense-indices are listed in `denseIndices`. Same wire format as
    // serializeObject (same 8B header, same field records). `denseIndices`
    // is the position within the NetReplicate-only subset of type->getField()
    // (i.e. denseIndices[0] refers to the first NetReplicate field, NOT the
    // first type field in general — see ReplicationManager::_netFieldSparseIndex
    // for the mapping).
    //
    // Returns false if denseIndices is empty, obj/type is null, or none of
    // the listed fields have a supported WireTypeId. Frame header's fieldCount
    // equals denseIndices.size() in the success case.
    static bool serializeDirtyFields(const ayt::reflect::ITypeInfo* type, const void* obj,
                                     uint32_t netId, const std::vector<uint32_t>& denseIndices,
                                     BitStream& s);

    // R3.1 (2026-07-27): compute the CRC32C of a single field's current
    // in-memory value. Returns the hash so the server can detect whether a
    // field changed since the last broadcast (compare against
    // ReflectedEntry::_fieldHashes[denseIdx]).
    //
    // Uses PacketCodec::computeCrc32c on the raw bytes (with a fallback for
    // std::string which is not trivially-copyable).
    static uint32_t hashFieldValue(WireTypeId id, const void* fieldPtr);

    // R3.2 (2026-07-28): hash function that walks nested wire types
    // (NestedStruct / FixedArray / DynamicArray / StringMap). The whole
    // nested field is hashed as a single value (per-element hash granularity
    // is out of scope per design §13 R3.2). For primitive WireTypeIds the
    // result matches hashFieldValue() above.
    //
    // `type` is required for nested types so we can dispatch into the
    // element type's WireTypeId; for primitives the type is unused.
    static uint32_t hashFieldValueEx(WireTypeId wid, const ayt::reflect::ITypeInfo* type, const void* fieldPtr);

    // R3.2 (2026-07-28): recursive write/read. The serializer calls
    // writeWireValue for each field record after the [u8 WireTypeId] byte;
    // the deserializer mirrors it. For WireTypeId 0..11 these delegate to
    // the existing R3.0 writeFieldValue/readFieldValue paths; for 12..15
    // they emit the nested wire prefix and recurse.
    static bool writeWireValue(BitStream& s, WireTypeId wid,
                               const ayt::reflect::ITypeInfo* type, const void* fieldPtr);
    static bool readWireValue(BitStream& s, WireTypeId wid,
                              const ayt::reflect::ITypeInfo* type, void* fieldPtr);

    // R4.1-B: optional per-field callback after a field record is applied.
    // Used by ReplicationManager to fire INetworkExtension::onRepNotify for
    // fields tagged FieldAttribute::RepNotify.
    using FieldAppliedFn = std::function<void(const ayt::reflect::IFieldInfo* field)>;

    // Inverse of serializeObject. Reads `expectedFieldCount` field records
    // (from the frame header fieldCount byte) and writes values back into
    // `obj`. Returns false on:
    //   - obj or type is null
    //   - truncated frame (ran out of bytes mid-record)
    //   - unknown WireTypeId
    //   - field name hash not found in this type's NetReplicate set
    // On false, obj is partially mutated — caller should treat state as
    // indeterminate (typical real-game reaction: snapshot is dropped, next
    // tick's snapshot will heal).
    //
    // R3.1 note: this path is also used by kMsgTypeDelta. Wire format is
    // identical to kMsgTypeReplication so a single deserializeObject
    // implementation covers both.
    static bool deserializeObject(const ayt::reflect::ITypeInfo* type, void* obj, BitStream& s,
                                  uint8_t expectedFieldCount,
                                  FieldAppliedFn onFieldApplied = nullptr);

    // ---- Body frame helpers (called by ReplicationManager) ----

    // Write ReplicationFrame header. After return, the BitStream cursor is
    // positioned just past the 8-byte header, ready for field records.
    static void writeReplicationFrameHeader(BitStream& s, uint32_t netId, uint16_t typeHash, uint8_t fieldCount);

    // Read ReplicationFrame header. Returns false if stream has < 8 bytes left.
    struct FrameHeader { uint32_t netId; uint16_t typeHash; uint8_t fieldCount; uint8_t reserved; };
    static bool readReplicationFrameHeader(BitStream& s, FrameHeader& out);

    // Write/read EntitySpawn / EntityDespawn bodies (compact 6B / 4B).
    static void writeEntitySpawn(BitStream& s, uint32_t netId, uint16_t typeHash);
    static bool  readEntitySpawn(BitStream& s, uint32_t& netId, uint16_t& typeHash);
    static void writeEntityDespawn(BitStream& s, uint32_t netId);
    static bool  readEntityDespawn(BitStream& s, uint32_t& netId);

    // ---- Type-id resolution ----
    // Map a C++ primitive type's typeid(T).hash_code() to a WireTypeId.
    // Returns false if the type is not in the R3.0 set (caller should skip
    // the field). Adding support for new types is a matter of extending the
    // switch inside resolveWireTypeId.
    static bool resolveWireTypeId(const ayt::reflect::ITypeInfo* fieldType, WireTypeId& outId);

    // FNV-1a 32-bit hash of a C-string, returned low-16-bits (so it fits in
    // the wire's 2-byte fieldNameHash slot). Stable across runs on the same
    // platform; not cryptographic.
    static uint16_t hashFieldName(const char* name);
};

} // namespace ayt::net