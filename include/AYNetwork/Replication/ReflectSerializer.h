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
//     [u32 serverTick]                  // 4 B — authority-side monotonic tick (R5.0+)
//     [u32 netId]                       // 4 B
//     [u64 schemaHash]                  // 8 B — stable reflected wire schema fingerprint
//     [u8  fieldCount]                  // 1 B — number of fields in this frame
//     [u8  flags]                       // 1 B — bit 0x01 = kFlagTeleport (R5.1+); bit 0x02+ reserved
//     [field records × fieldCount]
//
//   R5.1 wire compatibility: the `flags` byte is the same byte that was
//   reserved in R5.0. Old writers always emit 0; old readers ignore the
//   byte entirely. New writers may set bit 0x01 to mark a teleport frame;
//   new readers consult FrameHeader::isTeleport() and forward it to the
//   interpolator so it skips the lerp between the previous snapshot and
//   this one. See design §15.8.
//
//   Field record:
//     [u32 fieldNameHash]               // 4 B — full FNV-1a 32 field-name hash
//     [u8  fieldTypeId]                 // 1 B — WireTypeId
//     [value bytes]                     // per WireTypeId (see BitStream raw helpers)
//
//   EntitySpawn body (server announces new replicated entity):
//     [u32 netId] [u64 schemaHash]
//
//   EntityDespawn body:
//     [u32 netId]
//
// Nested structs, fixed/dynamic arrays and string-keyed maps are supported.
// Pointer-like and otherwise unsupported reflected fields are excluded from
// the network schema and are not serialized.
//
// R5.0 wire compatibility: serverTick is a NEW prefix byte added in front of
// the existing 14-byte header. Receivers must read it before invoking
// readReplicationFrameHeader / deserializeObject. R3.x receivers see the
// new prefix as 4 garbage bytes followed by an out-of-spec frame; they
// silently drop the frame. R5.0 receivers tolerate R3.x frames that lack
// the prefix by treating serverTick=0 as "uninitialized" (server never
// stamped it). See design §15.5.

#include <AYNetwork/INetwork.h>

#include <cstdint>
#include <cstddef>
#include <functional>

namespace ayt::reflect { class ITypeInfo; }

namespace ayt::net
{

// =============================================================================
// R5.1 (2026-08-24): Frame-level flag bits. Carried in the FrameHeader byte
// that R5.0 reserved as 0. Adding bits here does NOT break the wire: old
// readers ignore the byte; new readers mask off the bits they understand.
// =============================================================================
constexpr uint8_t kFlagTeleport = 0x01;   // set when the snapshot is a teleport; client skips lerp

class ReflectSerializer {
public:
    // Pack a complete ReplicationFrame (header + records) for one entity into `s`.
    // Walks type's fields, filters by NetReplicate, resolves each to a WireTypeId,
    // emits records. The frame header carries caller-provided netId plus the
    // stable reflected schema hash computed from `type`.
    // Returns false if obj/type is null or no NetReplicate field has a supported
    // type (frame is NOT written in that case — caller can drop safely).
    // R5.0: serverTick is stamped into the body prefix; pass 0 to keep the
    // legacy R3.x wire layout (no prefix). Production callers always pass
    // the current authority tick (see ReplicationManager::_serverTick).
    //
    // R5.1: `flags` is OR'd into the FrameHeader's reserved byte. Use the
    // kFlagTeleport bit to mark this snapshot as a teleport — clients skip
    // interpolation between the previous snapshot and this one (see design
    // §15.8). Pass 0 for normal frames.
    static bool serializeObject(const ayt::reflect::ITypeInfo* type, const void* obj,
                                uint32_t netId, BitStream& s,
                                uint32_t serverTick = 0, uint8_t flags = 0);

    // R3.1 (2026-07-27): serialize ONLY the NetReplicate fields whose
    // dense-indices are listed in `denseIndices`. Same wire format as
    // serializeObject (same 14B header, same field records). `denseIndices`
    // is the position within the NetReplicate-only subset of type->getField()
    // (i.e. denseIndices[0] refers to the first NetReplicate field, NOT the
    // first type field in general — see ReplicationManager::_netFieldSparseIndex
    // for the mapping).
    //
    // Returns false if denseIndices is empty, obj/type is null, or none of
    // the listed fields have a supported WireTypeId. Frame header's fieldCount
    // equals denseIndices.size() in the success case.
    //
    // R5.1: `flags` is forwarded into the FrameHeader; pass kFlagTeleport to
    // mark a Delta as a teleport (rare — Full Snapshot is the canonical
    // teleport frame; ReplicationManager::markTeleported forces a Full).
    static bool serializeDirtyFields(const ayt::reflect::ITypeInfo* type, const void* obj,
                                     uint32_t netId, const std::vector<uint32_t>& denseIndices,
                                     BitStream& s,
                                     uint32_t serverTick = 0, uint8_t flags = 0);

    // R3.1 (2026-07-27): compute the CRC32C of a single field's current
    // in-memory value. Returns the hash so the server can detect whether a
    // field changed since the last send to a peer (compare against that
    // peer's fieldHashes[denseIdx] baseline).
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
    // implementation covers both. R5.0 callers must have already consumed
    // the serverTick prefix; this function reads [u32 netId][u64 schema][u8
    // fieldCount][u8 reserved] then field records.
    static bool deserializeObject(const ayt::reflect::ITypeInfo* type, void* obj, BitStream& s,
                                  uint8_t expectedFieldCount,
                                  FieldAppliedFn onFieldApplied = nullptr);

    // ---- Body frame helpers (called by ReplicationManager) ----

    // R5.0: write/read a 4-byte serverTick prefix that precedes the
    // ReplicationFrame / DeltaFrame body. Authority always emits; receiver
    // reads it before calling readReplicationFrameHeader. Wire layout:
    //   [u32 serverTick][u32 netId][u64 schemaHash][u8 fieldCount][u8 reserved]
    static void writeServerTick(BitStream& s, uint32_t serverTick);
    static bool readServerTick (BitStream& s, uint32_t& serverTick);

    // Write ReplicationFrame header (R5.0 — excludes the serverTick prefix;
    // call writeServerTick first). After return, the BitStream cursor is
    // positioned just past the 14-byte header, ready for field records.
    //
    // R5.1: `flags` is written into the formerly-reserved byte (see
    // kFlagTeleport). Old callers passing the default 0 emit the exact
    // R5.0 layout.
    static void writeReplicationFrameHeader(BitStream& s, uint32_t netId, uint64_t schemaHash,
                                            uint8_t fieldCount, uint8_t flags = 0);

    // Read ReplicationFrame header. Returns false if stream has < 14 bytes left.
    //
    // R5.1: the `reserved` byte carries R5.1+ frame flags. Use
    // FrameHeader::isTeleport() instead of inspecting the byte directly so
    // callers don't need to know the bit layout.
    struct FrameHeader {
        uint32_t netId;
        uint64_t schemaHash;
        uint8_t  fieldCount;
        uint8_t  reserved;     // raw byte; FrameHeader::flags() for portable access
        uint8_t  flags() const { return reserved; }
        bool     isTeleport() const { return (reserved & kFlagTeleport) != 0; }
    };
    static bool readReplicationFrameHeader(BitStream& s, FrameHeader& out);

    // Write/read EntitySpawn / EntityDespawn bodies (12B / 4B).
    static void writeEntitySpawn(BitStream& s, uint32_t netId, uint64_t schemaHash);
    static bool  readEntitySpawn(BitStream& s, uint32_t& netId, uint64_t& schemaHash);
    static void writeEntityDespawn(BitStream& s, uint32_t netId);
    static bool  readEntityDespawn(BitStream& s, uint32_t& netId);

    // ---- R5.2 (2026-08-24) optional ack tail ----
    //
    // When the snapshot is destined for a connection that owns an
    // AutonomousProxy ghost, the server appends an 8-byte tail to the
    // FULL SNAPSHOT body (after the field records):
    //
    //   [u32 lastAckedInputTick][u32 serverCommandAge]
    //
    // Delta frames and SimulatedProxy-only Full Snapshots do NOT emit
    // the tail (8 B savings on the common path). Caller (ReplicationManager)
    // decides whether to write; the matching reader reads only when
    // AckTail::present is true. Default path (AckTail{present=false})
    // preserves R5.0/R5.1 byte layout exactly.
    struct AckTail {
        uint32_t lastAckedInputTick = 0;
        uint32_t serverCommandAge   = 0;
        bool     present            = false;
    };
    static void writeAckTail(BitStream& s, const AckTail& tail);
    static bool readAckTail (BitStream& s, AckTail& tail);

    // ---- Type-id resolution ----
    // Map a C++ primitive type's typeid(T).hash_code() to a WireTypeId.
    // Returns false if the type is not in the R3.0 set (caller should skip
    // the field). Adding support for new types is a matter of extending the
    // switch inside resolveWireTypeId.
    static bool resolveWireTypeId(const ayt::reflect::ITypeInfo* fieldType, WireTypeId& outId);

    // Full FNV-1a 32-bit field-name hash. Serializers reject a reflected type
    // when two NetReplicate fields collide instead of routing ambiguously.
    static uint32_t hashFieldName(const char* name);

    // Stable 64-bit fingerprint of the wire-visible schema: declared type
    // name/version plus every NetReplicate field name, WireTypeId and nested
    // element schema. It intentionally does not use typeid/hash_code.
    static uint64_t hashTypeSchema(const ayt::reflect::ITypeInfo* type);
};

} // namespace ayt::net
