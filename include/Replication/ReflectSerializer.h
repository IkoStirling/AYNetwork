#pragma once
// ReflectSerializer.h - R3.0 AYReflect-driven wire format serializer
//
// R3.0 (2026-07-27): serialization is driven entirely by ayt::reflect::ITypeInfo
// metadata. The user tags fields with FieldAttribute::NetReplicate; this header
// walks them and packs into the wire format below. The user does NOT need to
// implement any virtual.
//
// Wire format (server → client):
//
//   ReplicationFrame body (one entity per frame):
//     [u32 netId]                       // 4 B
//     [u16 typeHash]                    // 2 B — ayt::reflect::ITypeInfo::getId()
//     [u8  fieldCount]                  // 1 B — number of NetReplicate fields
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
// debug verbosity). R3.1+ extends WireTypeId range to cover more cases.

#include <IAYNetwork.h>

#include <cstdint>
#include <cstddef>

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
    static bool deserializeObject(const ayt::reflect::ITypeInfo* type, void* obj, BitStream& s, uint8_t expectedFieldCount);

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