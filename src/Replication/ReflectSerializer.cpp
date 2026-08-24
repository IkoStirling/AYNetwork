// ReflectSerializer.cpp - R3.0 AYReflect-driven wire format serializer
//
// Implementation strategy:
//   1. serializeObject: walk type->getFieldCount(), filter by NetReplicate,
//      resolve each field's type to a WireTypeId, write field record, emit
//      the value bytes via BitStream raw helpers.
//   2. resolveWireTypeId: compare field->getType()->getId() against
//      typeid(T).hash_code() for the 12 R3.0 primitives.
//   3. deserializeObject: read field records and write back through the
//      field offset (memcpy via get(void*)). Field matching uses FNV-1a hash
//      of the field name (low 16 bits) for a deterministic lookup.
//
// Pitfalls observed during R3.0:
//   - ITypeInfo::getField(i) returns nullptr for out-of-range indices; bail.
//   - IFieldInfo::getName() returns a stable const char*; we FNV-1a it.
//   - IFieldInfo::get(void* obj) returns a pointer to the field memory;
//     we read/write through it without per-type switch (memcpy is enough
//     for trivially-copyable primitives; std::string is special-cased).
//   - On a writer-side unknown type, we DO NOT increment fieldCount — we
//     decrement it back. This means the wire header fieldCount always
//     reflects fields actually emitted; receivers don't see a mismatch.

#include <AYNetwork/Replication/ReflectSerializer.h>

#include <AYReflect/IReflect.h>
#include <AYNetwork/Protocol/PacketCodec.h>
#include <algorithm>
#include <cstring>
#include <string>
#include <unordered_set>
#include <vector>

namespace ayt::net
{

// =============================================================================
// Local helpers
// =============================================================================
namespace {

// FNV-1a 32-bit. Matches hashFieldName() expectations.
constexpr uint32_t kFnvOffsetBasis = 0x811C9DC5u;
constexpr uint32_t kFnvPrime       = 0x01000193u;
uint32_t fnv1a32(const char* s) {
    uint32_t h = kFnvOffsetBasis;
    while (*s) {
        h ^= static_cast<uint8_t>(*s++);
        h *= kFnvPrime;
    }
    return h;
}

} // anonymous namespace

uint32_t ReflectSerializer::hashFieldName(const char* name) {
    if (!name) return 0;
    return fnv1a32(name);
}

// =============================================================================
// resolveWireTypeId — maps AYReflect ITypeInfo::getId() (= typeid(T).hash_code())
// to a WireTypeId. R3.0 supported the 12 primitives only. R3.2 (2026-07-28)
// extends to cover NestedStruct (12), FixedArray (13), DynamicArray (14),
// and StringMap (15).
// =============================================================================
bool ReflectSerializer::resolveWireTypeId(const ayt::reflect::ITypeInfo* fieldType, WireTypeId& outId) {
    if (!fieldType) return false;
    const size_t id = fieldType->getId();

    // Direct typeid compares against the 12 primitives. Hash codes are
    // process-stable (MSVC guarantees; GCC/Clang same TU = same hash), so
    // server/client built from the same compiler produce matching ids.
    if      (id == typeid(bool).hash_code())        outId = WireTypeId::Bool;
    else if (id == typeid(int8_t).hash_code())       outId = WireTypeId::Int8;
    else if (id == typeid(int16_t).hash_code())      outId = WireTypeId::Int16;
    else if (id == typeid(int32_t).hash_code())      outId = WireTypeId::Int32;
    else if (id == typeid(int64_t).hash_code())      outId = WireTypeId::Int64;
    else if (id == typeid(uint8_t).hash_code())      outId = WireTypeId::UInt8;
    else if (id == typeid(uint16_t).hash_code())     outId = WireTypeId::UInt16;
    else if (id == typeid(uint32_t).hash_code())     outId = WireTypeId::UInt32;
    else if (id == typeid(uint64_t).hash_code())     outId = WireTypeId::UInt64;
    else if (id == typeid(float).hash_code())        outId = WireTypeId::Float;
    else if (id == typeid(double).hash_code())       outId = WireTypeId::Double;
    else if (id == typeid(std::string).hash_code())  outId = WireTypeId::String;
    else {
        // R3.2: nested type detection.
        //   - std::map<string,V> MUST be checked before IContainerTypeInfo:
        //     MapTypeInfo also implements IContainerTypeInfo (isFixedSize=false)
        //     and would otherwise be misclassified as DynamicArray.
        //   - Containers that ARE IContainerTypeInfo → FixedArray or DynamicArray
        //     depending on isFixedSize().
        //   - Anything else → NestedStruct (assumed registered struct).
        if (dynamic_cast<const ayt::reflect::MapTypeInfoBase*>(fieldType)) {
            outId = WireTypeId::StringMap;
            return true;
        }
        const auto* ctn = dynamic_cast<const ayt::reflect::IContainerTypeInfo*>(fieldType);
        if (ctn) {
            outId = ctn->isFixedSize() ? WireTypeId::FixedArray : WireTypeId::DynamicArray;
            return true;
        }
        outId = WireTypeId::NestedStruct;
        return true;
    }
    return true;
}

namespace {

constexpr uint64_t kFnv64OffsetBasis = 0xCBF29CE484222325ull;
constexpr uint64_t kFnv64Prime = 0x100000001B3ull;

void fnv1a64Append(uint64_t& hash, const void* data, size_t size) {
    const auto* bytes = static_cast<const uint8_t*>(data);
    for (size_t i = 0; i < size; ++i) {
        hash ^= bytes[i];
        hash *= kFnv64Prime;
    }
}

void fnv1a64AppendString(uint64_t& hash, const char* text) {
    if (!text) {
        const uint8_t zero = 0;
        fnv1a64Append(hash, &zero, sizeof(zero));
        return;
    }
    fnv1a64Append(hash, text, std::strlen(text) + 1);
}

bool appendTypeSchema(uint64_t& hash, const ayt::reflect::ITypeInfo* type,
                      std::unordered_set<const ayt::reflect::ITypeInfo*>& visiting) {
    if (!type) return false;

    WireTypeId typeWid;
    if (!ReflectSerializer::resolveWireTypeId(type, typeWid)) return false;
    const uint8_t typeWidByte = static_cast<uint8_t>(typeWid);
    fnv1a64Append(hash, &typeWidByte, sizeof(typeWidByte));
    fnv1a64AppendString(hash, type->getName());
    const uint32_t version = type->getVersion();
    fnv1a64Append(hash, &version, sizeof(version));

    if (!visiting.insert(type).second) {
        const uint8_t recursiveMarker = 0xFF;
        fnv1a64Append(hash, &recursiveMarker, sizeof(recursiveMarker));
        return true;
    }

    bool ok = true;
    if (typeWid == WireTypeId::FixedArray || typeWid == WireTypeId::DynamicArray ||
        typeWid == WireTypeId::StringMap) {
        const auto* container = dynamic_cast<const ayt::reflect::IContainerTypeInfo*>(type);
        ok = container && appendTypeSchema(hash, container->getElementType(), visiting);
    } else if (typeWid == WireTypeId::NestedStruct) {
        std::vector<const ayt::reflect::IFieldInfo*> fields;
        for (uint32_t i = 0; i < type->getFieldCount(); ++i) {
            const auto* field = type->getField(i);
            if (!field || !field->hasAttribute(ayt::reflect::FieldAttribute::NetReplicate)) continue;
            WireTypeId fieldWid;
            if (ReflectSerializer::resolveWireTypeId(field->getType(), fieldWid)) fields.push_back(field);
        }
        std::sort(fields.begin(), fields.end(), [](const auto* lhs, const auto* rhs) {
            return std::strcmp(lhs->getName(), rhs->getName()) < 0;
        });
        std::unordered_set<uint32_t> fieldHashes;
        for (const auto* field : fields) {
            const uint32_t fieldHash = ReflectSerializer::hashFieldName(field->getName());
            if (!fieldHashes.insert(fieldHash).second) { ok = false; break; }
            fnv1a64AppendString(hash, field->getName());
            if (!appendTypeSchema(hash, field->getType(), visiting)) { ok = false; break; }
        }
    }

    visiting.erase(type);
    return ok;
}

bool collectNetFields(const ayt::reflect::ITypeInfo* type,
                      std::vector<const ayt::reflect::IFieldInfo*>& fields) {
    if (!type) return false;
    std::unordered_set<uint32_t> hashes;
    for (uint32_t i = 0; i < type->getFieldCount(); ++i) {
        const auto* field = type->getField(i);
        if (!field || !field->hasAttribute(ayt::reflect::FieldAttribute::NetReplicate)) continue;
        WireTypeId wid;
        if (!ReflectSerializer::resolveWireTypeId(field->getType(), wid)) continue;
        if (!hashes.insert(ReflectSerializer::hashFieldName(field->getName())).second) return false;
        fields.push_back(field);
    }
    return fields.size() <= 255;
}

} // anonymous namespace

uint64_t ReflectSerializer::hashTypeSchema(const ayt::reflect::ITypeInfo* type) {
    uint64_t hash = kFnv64OffsetBasis;
    std::unordered_set<const ayt::reflect::ITypeInfo*> visiting;
    return appendTypeSchema(hash, type, visiting) ? hash : 0;
}

// =============================================================================
// Per-type readers/writers — single switch, no virtuals.
// =============================================================================
namespace {

void writeFieldValue(BitStream& s, WireTypeId id, const void* fieldPtr) {
    // R3.0/R3.1: only 12 primitives. R3.2 extends to 12..15 via writeWireValue
    // (called from serializeObject). This stays as the primitive path.
    switch (id) {
        case WireTypeId::Bool: {
            bool v; std::memcpy(&v, fieldPtr, sizeof(v)); s.writeBool(v); break;
        }
        case WireTypeId::Int8:  { int8_t  v; std::memcpy(&v, fieldPtr, sizeof(v)); s.writeInt8(v);  break; }
        case WireTypeId::Int16: { int16_t v; std::memcpy(&v, fieldPtr, sizeof(v)); s.writeInt16(v); break; }
        case WireTypeId::Int32: { int32_t v; std::memcpy(&v, fieldPtr, sizeof(v)); s.writeInt32Raw(v); break; }
        case WireTypeId::Int64: { int64_t v; std::memcpy(&v, fieldPtr, sizeof(v)); s.writeInt64(v); break; }
        case WireTypeId::UInt8:  { uint8_t  v; std::memcpy(&v, fieldPtr, sizeof(v)); s.writeUInt8(v);  break; }
        case WireTypeId::UInt16: { uint16_t v; std::memcpy(&v, fieldPtr, sizeof(v)); s.writeUInt16(v); break; }
        case WireTypeId::UInt32: { uint32_t v; std::memcpy(&v, fieldPtr, sizeof(v)); s.writeUInt32(v); break; }
        case WireTypeId::UInt64: { uint64_t v; std::memcpy(&v, fieldPtr, sizeof(v)); s.writeUInt64(v); break; }
        case WireTypeId::Float:  { float  v; std::memcpy(&v, fieldPtr, sizeof(v)); s.writeFloatRaw(v); break; }
        case WireTypeId::Double: { double v; std::memcpy(&v, fieldPtr, sizeof(v)); s.writeDouble(v);   break; }
        case WireTypeId::String: {
            // std::string is NOT trivially-copyable — copy via constructor.
            const auto* strPtr = static_cast<const std::string*>(fieldPtr);
            uint16_t len = static_cast<uint16_t>(strPtr->size());
            s.writeUInt16(len);
            for (uint16_t i = 0; i < len; ++i) {
                s.writeByte(static_cast<uint8_t>((*strPtr)[i]));
            }
            break;
        }
        default: break; // R3.2 nested types handled by writeWireValue
    }
}

bool readFieldValue(BitStream& s, WireTypeId id, void* fieldPtr) {
    // Track current byte position; if we underflow, fail.
    size_t posBefore = s.getBitPosition();
    switch (id) {
        case WireTypeId::Bool:   { bool    v = s.readBool();    std::memcpy(fieldPtr, &v, sizeof(v)); break; }
        case WireTypeId::Int8:   { int8_t  v = s.readInt8();    std::memcpy(fieldPtr, &v, sizeof(v)); break; }
        case WireTypeId::Int16:  { int16_t v = s.readInt16();   std::memcpy(fieldPtr, &v, sizeof(v)); break; }
        case WireTypeId::Int32:  { int32_t v = s.readInt32Raw();std::memcpy(fieldPtr, &v, sizeof(v)); break; }
        case WireTypeId::Int64:  { int64_t v = s.readInt64();   std::memcpy(fieldPtr, &v, sizeof(v)); break; }
        case WireTypeId::UInt8:  { uint8_t  v = s.readUInt8();  std::memcpy(fieldPtr, &v, sizeof(v)); break; }
        case WireTypeId::UInt16: { uint16_t v = s.readUInt16(); std::memcpy(fieldPtr, &v, sizeof(v)); break; }
        case WireTypeId::UInt32: { uint32_t v = s.readUInt32(); std::memcpy(fieldPtr, &v, sizeof(v)); break; }
        case WireTypeId::UInt64: { uint64_t v = s.readUInt64(); std::memcpy(fieldPtr, &v, sizeof(v)); break; }
        case WireTypeId::Float:  { float    v = s.readFloatRaw();std::memcpy(fieldPtr, &v, sizeof(v)); break; }
        case WireTypeId::Double: { double   v = s.readDouble(); std::memcpy(fieldPtr, &v, sizeof(v)); break; }
        case WireTypeId::String: {
            uint16_t len = s.readUInt16();
            if (s.getBitPosition() + size_t(len) * 8 > s.getBitCount()) return false;
            auto* strPtr = static_cast<std::string*>(fieldPtr);
            strPtr->resize(len);
            for (uint16_t i = 0; i < len; ++i) {
                (*strPtr)[i] = static_cast<char>(s.readByte());
            }
            break;
        }
        default: return false; // R3.2 nested types handled by readWireValue
    }
    (void)posBefore;
    return true;
}

} // anonymous namespace

// =============================================================================
// ReplicationFrame header writers/readers
// =============================================================================
void ReflectSerializer::writeReplicationFrameHeader(BitStream& s, uint32_t netId, uint64_t schemaHash, uint8_t fieldCount) {
    s.writeUInt32(netId);
    s.writeUInt64(schemaHash);
    s.writeUInt8(fieldCount);
    s.writeUInt8(0); // reserved
}

bool ReflectSerializer::readReplicationFrameHeader(BitStream& s, FrameHeader& out) {
    if (s.getBitPosition() + 112 > s.getBitCount()) return false;
    out.netId      = s.readUInt32();
    out.schemaHash = s.readUInt64();
    out.fieldCount = s.readUInt8();
    out.reserved   = s.readUInt8();
    return true;
}

void ReflectSerializer::writeEntitySpawn(BitStream& s, uint32_t netId, uint64_t schemaHash) {
    s.writeUInt32(netId);
    s.writeUInt64(schemaHash);
}

bool ReflectSerializer::readEntitySpawn(BitStream& s, uint32_t& netId, uint64_t& schemaHash) {
    if (s.getBitPosition() + 96 > s.getBitCount()) return false;
    netId    = s.readUInt32();
    schemaHash = s.readUInt64();
    return true;
}

void ReflectSerializer::writeEntityDespawn(BitStream& s, uint32_t netId) {
    s.writeUInt32(netId);
}

bool ReflectSerializer::readEntityDespawn(BitStream& s, uint32_t& netId) {
    if (s.getBitPosition() + 32 > s.getBitCount()) return false;
    netId = s.readUInt32();
    return true;
}

// =============================================================================
// serializeObject / deserializeObject
// =============================================================================
bool ReflectSerializer::serializeObject(const ayt::reflect::ITypeInfo* type, const void* obj,
                                         uint32_t netId, BitStream& s) {
    if (!type || !obj) return false;

    std::vector<const ayt::reflect::IFieldInfo*> fields;
    if (!collectNetFields(type, fields)) return false;
    const uint32_t fieldCount = static_cast<uint32_t>(fields.size());

    // Nothing to send — caller may skip broadcasting this entity.
    if (fieldCount == 0) return false;

    const uint64_t schemaHash = hashTypeSchema(type);
    if (schemaHash == 0) return false;
    writeReplicationFrameHeader(s, netId, schemaHash, static_cast<uint8_t>(fieldCount));

    for (const auto* field : fields) {
        WireTypeId wid;
        if (!resolveWireTypeId(field->getType(), wid)) return false;

        const uint32_t nameHash = hashFieldName(field->getName());
        s.writeUInt32(nameHash);
        s.writeUInt8(static_cast<uint8_t>(wid));
        // R3.2: writeWireValue handles 12..15 nested types recursively.
        // For 0..11 it delegates to writeFieldValue.
        if (!writeWireValue(s, wid, field->getType(), field->get(const_cast<void*>(obj)))) return false;
    }

    return true;
}

bool ReflectSerializer::deserializeObject(const ayt::reflect::ITypeInfo* type, void* obj, BitStream& s,
                                          uint8_t expectedFieldCount,
                                          FieldAppliedFn onFieldApplied) {
    if (!type || !obj) return false;

    std::vector<const ayt::reflect::IFieldInfo*> netFields;
    if (!collectNetFields(type, netFields)) return false;
    const uint32_t total = type->getFieldCount();
    for (uint8_t i = 0; i < expectedFieldCount; ++i) {
        // Truncation guard: at least 5 bytes (u32 + u8) per record header.
        if (s.getBitPosition() + 40 > s.getBitCount()) return false;

        const uint32_t nameHash = s.readUInt32();
        const uint8_t  widByte  = s.readUInt8();
        const WireTypeId wid = static_cast<WireTypeId>(widByte);

        // Match by FNV-1a name hash. ITypeInfo::findField(name) is a string
        // compare, so we walk fields ourselves.
        const ayt::reflect::IFieldInfo* field = nullptr;
        for (uint32_t j = 0; j < total; ++j) {
            const auto* f = type->getField(j);
            if (f && hashFieldName(f->getName()) == nameHash) { field = f; break; }
        }

        if (!field) return false; // unknown field on this client build
        if (!field->hasAttribute(ayt::reflect::FieldAttribute::NetReplicate)) return false;
        WireTypeId expectedWid;
        if (!resolveWireTypeId(field->getType(), expectedWid) || expectedWid != wid) return false;

        // R3.2: readWireValue handles 12..15 nested types recursively.
        // For 0..11 it delegates to readFieldValue.
        if (!readWireValue(s, wid, field->getType(), field->get(obj))) return false;

        if (onFieldApplied) onFieldApplied(field);
    }
    return true;
}

// =============================================================================
// R3.1 (2026-07-27): serializeDirtyFields + hashFieldValue
//
// The server tracks per-field CRC32C hashes via ReflectedEntry::_fieldHashes
// (size = NetReplicate field count). On tick() it compares the current value's
// hash to the stored one and only emits a field record for the dirty ones.
// This keeps Delta frames tiny — typically 5–15 B per dirty field versus the
// Full Snapshot which always carries every NetReplicate field.
//
// Wire format produced by serializeDirtyFields is byte-for-byte the same as
// serializeObject's frame: same 8B header, same field records. The receiver's
// deserializeObject doesn't care which envelope msgType wrapped it.
// =============================================================================

namespace {

// CRC32C over the raw bytes of a field value. std::string is special-cased
// because it's not trivially-copyable. Empty strings hash to a deterministic
// value (CRC32C of zero-length input).
uint32_t crcOfFieldValue(WireTypeId id, const void* fieldPtr) {
    switch (id) {
        case WireTypeId::Bool:   { bool    v; std::memcpy(&v, fieldPtr, sizeof(v)); return PacketCodec::computeCrc32c(reinterpret_cast<const uint8_t*>(&v), sizeof(v)); }
        case WireTypeId::Int8:   { int8_t  v; std::memcpy(&v, fieldPtr, sizeof(v)); return PacketCodec::computeCrc32c(reinterpret_cast<const uint8_t*>(&v), sizeof(v)); }
        case WireTypeId::Int16:  { int16_t v; std::memcpy(&v, fieldPtr, sizeof(v)); return PacketCodec::computeCrc32c(reinterpret_cast<const uint8_t*>(&v), sizeof(v)); }
        case WireTypeId::Int32:  { int32_t v; std::memcpy(&v, fieldPtr, sizeof(v)); return PacketCodec::computeCrc32c(reinterpret_cast<const uint8_t*>(&v), sizeof(v)); }
        case WireTypeId::Int64:  { int64_t v; std::memcpy(&v, fieldPtr, sizeof(v)); return PacketCodec::computeCrc32c(reinterpret_cast<const uint8_t*>(&v), sizeof(v)); }
        case WireTypeId::UInt8:  { uint8_t  v; std::memcpy(&v, fieldPtr, sizeof(v)); return PacketCodec::computeCrc32c(reinterpret_cast<const uint8_t*>(&v), sizeof(v)); }
        case WireTypeId::UInt16: { uint16_t v; std::memcpy(&v, fieldPtr, sizeof(v)); return PacketCodec::computeCrc32c(reinterpret_cast<const uint8_t*>(&v), sizeof(v)); }
        case WireTypeId::UInt32: { uint32_t v; std::memcpy(&v, fieldPtr, sizeof(v)); return PacketCodec::computeCrc32c(reinterpret_cast<const uint8_t*>(&v), sizeof(v)); }
        case WireTypeId::UInt64: { uint64_t v; std::memcpy(&v, fieldPtr, sizeof(v)); return PacketCodec::computeCrc32c(reinterpret_cast<const uint8_t*>(&v), sizeof(v)); }
        case WireTypeId::Float:  { float    v; std::memcpy(&v, fieldPtr, sizeof(v)); return PacketCodec::computeCrc32c(reinterpret_cast<const uint8_t*>(&v), sizeof(v)); }
        case WireTypeId::Double: { double   v; std::memcpy(&v, fieldPtr, sizeof(v)); return PacketCodec::computeCrc32c(reinterpret_cast<const uint8_t*>(&v), sizeof(v)); }
        case WireTypeId::String: {
            const auto* strPtr = static_cast<const std::string*>(fieldPtr);
            return PacketCodec::computeCrc32c(
                reinterpret_cast<const uint8_t*>(strPtr->data()), strPtr->size());
        }
    }
    return 0;
}

} // anonymous namespace

uint32_t ReflectSerializer::hashFieldValue(WireTypeId id, const void* fieldPtr) {
    if (!fieldPtr) return 0;
    return crcOfFieldValue(id, fieldPtr);
}

bool ReflectSerializer::serializeDirtyFields(const ayt::reflect::ITypeInfo* type, const void* obj,
                                             uint32_t netId, const std::vector<uint32_t>& denseIndices,
                                             BitStream& s) {
    if (!type || !obj || denseIndices.empty()) return false;

    const uint32_t total = type->getFieldCount();
    const uint32_t fieldCount = static_cast<uint32_t>(denseIndices.size());
    if (fieldCount > 255) return false; // frame header fieldCount is u8

    // Walk type fields once, collecting NetReplicate ones into a dense list.
    // The `denseIndices[k]` is the index into THAT dense list, NOT into
    // type->getField(). So we rebuild the dense list here too.
    //
    // For typical entity counts (a few per tick) and modest field counts
    // (≤32), this O(N+M) scan is cheaper than maintaining a side index.
    std::vector<const ayt::reflect::IFieldInfo*> netFields;
    if (!collectNetFields(type, netFields)) return false;

    // Validate every requested dense index resolves to a supported field.
    for (uint32_t k = 0; k < fieldCount; ++k) {
        if (denseIndices[k] >= netFields.size()) return false;
    }

    const uint64_t schemaHash = hashTypeSchema(type);
    if (schemaHash == 0) return false;
    writeReplicationFrameHeader(s, netId, schemaHash, static_cast<uint8_t>(fieldCount));

    for (uint32_t k = 0; k < fieldCount; ++k) {
        const auto* field = netFields[denseIndices[k]];
        WireTypeId wid;
        resolveWireTypeId(field->getType(), wid); // already verified above

        const uint32_t nameHash = hashFieldName(field->getName());
        s.writeUInt32(nameHash);
        s.writeUInt8(static_cast<uint8_t>(wid));
        // R3.2: writeWireValue handles 12..15 nested types recursively.
        writeWireValue(s, wid, field->getType(), field->get(const_cast<void*>(obj)));
    }
    return true;
}

// =============================================================================
// R3.2 (2026-07-28): writeWireValue / readWireValue
//
// Recursive entry points for all 16 WireTypeIds. For 0..11 (primitives)
// they delegate to writeFieldValue/readFieldValue above. For 12..15 they
// emit the nested type's wire prefix and recurse on element/value types.
//
// All nested operations use IContainerTypeInfo (for vector/array/map) or
// IFieldInfo (for nested struct) — the same primitives AYSerializer uses,
// so the AYReflect->wire pipeline stays single-source.
// =============================================================================
namespace {

// Count NetReplicate fields that resolve to a wire type (for nested struct prefix).
uint8_t countNetReplicateFields(const ayt::reflect::ITypeInfo* type) {
    if (!type) return 0;
    uint32_t count = 0;
    const uint32_t total = type->getFieldCount();
    for (uint32_t i = 0; i < total; ++i) {
        const auto* field = type->getField(i);
        if (!field) continue;
        if (!field->hasAttribute(ayt::reflect::FieldAttribute::NetReplicate)) continue;
        WireTypeId wid;
        if (!ReflectSerializer::resolveWireTypeId(field->getType(), wid)) continue;
        ++count;
        if (count == 255) break;
    }
    return static_cast<uint8_t>(count);
}

// Emit a single struct's NetReplicate fields recursively. Caller writes
// [u16 nestedTypeHash][u8 fieldCount] before calling this.
bool serializeNestedStructFields(BitStream& s, const ayt::reflect::ITypeInfo* type, const void* obj) {
    if (!type || !obj) return false;
    std::vector<const ayt::reflect::IFieldInfo*> fields;
    if (!collectNetFields(type, fields)) return false;
    for (const auto* field : fields) {
        WireTypeId wid;
        if (!ReflectSerializer::resolveWireTypeId(field->getType(), wid)) return false;
        const uint32_t nameHash = ReflectSerializer::hashFieldName(field->getName());
        s.writeUInt32(nameHash);
        s.writeUInt8(static_cast<uint8_t>(wid));
        if (!ReflectSerializer::writeWireValue(s, wid, field->getType(), field->get(const_cast<void*>(obj)))) return false;
    }
    return true;
}

// Read [u8 fieldCount] then deserialize that many records into `obj`. Mirrors
// serializeNestedStructFields. Caller is responsible for resolving the
// nested struct type via [u16 nestedTypeHash] before calling this.
bool deserializeNestedStructFields(const ayt::reflect::ITypeInfo* type, void* obj, BitStream& s, uint8_t expectedFieldCount) {
    if (!type || !obj) return false;
    const uint32_t total = type->getFieldCount();
    for (uint8_t i = 0; i < expectedFieldCount; ++i) {
        if (s.getBitPosition() + 40 > s.getBitCount()) return false;
        const uint32_t nameHash = s.readUInt32();
        const uint8_t  widByte  = s.readUInt8();
        const WireTypeId wid = static_cast<WireTypeId>(widByte);

        const ayt::reflect::IFieldInfo* field = nullptr;
        for (uint32_t j = 0; j < total; ++j) {
            const auto* f = type->getField(j);
            if (f && ReflectSerializer::hashFieldName(f->getName()) == nameHash) { field = f; break; }
        }
        if (!field) return false;
        if (!field->hasAttribute(ayt::reflect::FieldAttribute::NetReplicate)) return false;
        WireTypeId expectedWid;
        if (!ReflectSerializer::resolveWireTypeId(field->getType(), expectedWid) || expectedWid != wid) return false;
        if (!ReflectSerializer::readWireValue(s, wid, field->getType(), field->get(obj))) return false;
    }
    return true;
}

} // anonymous namespace

bool ReflectSerializer::writeWireValue(BitStream& s, WireTypeId wid,
                                       const ayt::reflect::ITypeInfo* type, const void* fieldPtr) {
    if (!fieldPtr) return false;
    switch (wid) {
        case WireTypeId::Bool:
        case WireTypeId::Int8:
        case WireTypeId::Int16:
        case WireTypeId::Int32:
        case WireTypeId::Int64:
        case WireTypeId::UInt8:
        case WireTypeId::UInt16:
        case WireTypeId::UInt32:
        case WireTypeId::UInt64:
        case WireTypeId::Float:
        case WireTypeId::Double:
        case WireTypeId::String:
            writeFieldValue(s, wid, fieldPtr);
            return true;

        case WireTypeId::NestedStruct: {
            if (!type) return false;
            // Wire prefix: [u64 schemaHash][u8 fieldCount][records...]
            const uint64_t nestedHash = hashTypeSchema(type);
            if (nestedHash == 0) return false;
            const uint8_t fc = countNetReplicateFields(type);
            s.writeUInt64(nestedHash);
            s.writeUInt8(fc);
            return serializeNestedStructFields(s, type, fieldPtr);
        }

        case WireTypeId::FixedArray: {
            if (!type) return false;
            const auto* ctn = dynamic_cast<const ayt::reflect::IContainerTypeInfo*>(type);
            if (!ctn) return false;
            ayt::reflect::ITypeInfo* elemType = ctn->getElementType();
            if (!elemType) return false;
            WireTypeId elemWid;
            if (!resolveWireTypeId(elemType, elemWid)) return false;
            const size_t N = ctn->getContainerSize(fieldPtr);
            if (N > 255) return false;
            // Wire prefix: [u8 elemWid][u8 N][elements...]
            s.writeUInt8(static_cast<uint8_t>(elemWid));
            s.writeUInt8(static_cast<uint8_t>(N));
            for (size_t i = 0; i < N; ++i) {
                const void* ePtr = ctn->getElementAt(fieldPtr, i);
                if (!ePtr) return false;
                if (!writeWireValue(s, elemWid, elemType, ePtr)) return false;
            }
            return true;
        }

        case WireTypeId::DynamicArray: {
            if (!type) return false;
            const auto* ctn = dynamic_cast<const ayt::reflect::IContainerTypeInfo*>(type);
            if (!ctn) return false;
            ayt::reflect::ITypeInfo* elemType = ctn->getElementType();
            if (!elemType) return false;
            WireTypeId elemWid;
            if (!resolveWireTypeId(elemType, elemWid)) return false;
            const size_t N = ctn->getContainerSize(fieldPtr);
            // Wire prefix: [u8 elemWid][u32 N][elements...]
            s.writeUInt8(static_cast<uint8_t>(elemWid));
            s.writeUInt32(static_cast<uint32_t>(N));
            for (size_t i = 0; i < N; ++i) {
                const void* ePtr = ctn->getElementAt(fieldPtr, i);
                if (!ePtr) return false;
                if (!writeWireValue(s, elemWid, elemType, ePtr)) return false;
            }
            return true;
        }

        case WireTypeId::StringMap: {
            if (!type) return false;
            const auto* ctn = dynamic_cast<const ayt::reflect::IContainerTypeInfo*>(type);
            if (!ctn) return false;
            ayt::reflect::ITypeInfo* valueType = ctn->getElementType();
            if (!valueType) return false;
            WireTypeId valueWid;
            if (!resolveWireTypeId(valueType, valueWid)) return false;
            const size_t N = ctn->getContainerSize(fieldPtr);
            // Wire prefix: [u8 valueWid][u32 entryCount][entries...]
            s.writeUInt8(static_cast<uint8_t>(valueWid));
            s.writeUInt32(static_cast<uint32_t>(N));
            for (size_t i = 0; i < N; ++i) {
                const void* kPtr = ctn->getKeyAt(fieldPtr, i);
                const void* vPtr = ctn->getValueAt(fieldPtr, i);
                if (!kPtr || !vPtr) return false;
                const auto& k = *static_cast<const std::string*>(kPtr);
                const uint16_t klen = static_cast<uint16_t>(k.size());
                s.writeUInt16(klen);
                for (uint16_t j = 0; j < klen; ++j) {
                    s.writeByte(static_cast<uint8_t>(k[j]));
                }
                if (!writeWireValue(s, valueWid, valueType, vPtr)) return false;
            }
            return true;
        }
    }
    return false;
}

bool ReflectSerializer::readWireValue(BitStream& s, WireTypeId wid,
                                      const ayt::reflect::ITypeInfo* type, void* fieldPtr) {
    if (!fieldPtr) return false;
    switch (wid) {
        case WireTypeId::Bool:
        case WireTypeId::Int8:
        case WireTypeId::Int16:
        case WireTypeId::Int32:
        case WireTypeId::Int64:
        case WireTypeId::UInt8:
        case WireTypeId::UInt16:
        case WireTypeId::UInt32:
        case WireTypeId::UInt64:
        case WireTypeId::Float:
        case WireTypeId::Double:
        case WireTypeId::String:
            return readFieldValue(s, wid, fieldPtr);

        case WireTypeId::NestedStruct: {
            if (!type) return false;
            // Validate before mutating any nested field.
            if (s.getBitPosition() + 72 > s.getBitCount()) return false;
            const uint64_t nestedHash = s.readUInt64();
            const uint8_t  fieldCount  = s.readUInt8();
            if (nestedHash == 0 || nestedHash != hashTypeSchema(type)) return false;
            return deserializeNestedStructFields(type, fieldPtr, s, fieldCount);
        }

        case WireTypeId::FixedArray: {
            if (!type) return false;
            const auto* ctn = dynamic_cast<const ayt::reflect::IContainerTypeInfo*>(type);
            if (!ctn) return false;
            ayt::reflect::ITypeInfo* elemType = ctn->getElementType();
            if (!elemType) return false;
            if (s.getBitPosition() + 16 > s.getBitCount()) return false;
            const uint8_t elemWidByte = s.readUInt8();
            const uint8_t N            = s.readUInt8();
            const WireTypeId elemWid = static_cast<WireTypeId>(elemWidByte);
            for (size_t i = 0; i < N; ++i) {
                void* ePtr = ctn->getElementAt(fieldPtr, i);
                if (!ePtr) return false;
                if (!readWireValue(s, elemWid, elemType, ePtr)) return false;
            }
            return true;
        }

        case WireTypeId::DynamicArray: {
            if (!type) return false;
            const auto* ctn = dynamic_cast<const ayt::reflect::IContainerTypeInfo*>(type);
            if (!ctn) return false;
            ayt::reflect::ITypeInfo* elemType = ctn->getElementType();
            if (!elemType) return false;
            if (s.getBitPosition() + 40 > s.getBitCount()) return false;
            const uint8_t  elemWidByte = s.readUInt8();
            const uint32_t N           = s.readUInt32();
            const WireTypeId elemWid = static_cast<WireTypeId>(elemWidByte);
            // Resize the container so getElementAt(0..N-1) is valid.
            ctn->resize(fieldPtr, N);
            for (uint32_t i = 0; i < N; ++i) {
                void* ePtr = ctn->getElementAt(fieldPtr, i);
                if (!ePtr) return false;
                if (!readWireValue(s, elemWid, elemType, ePtr)) return false;
            }
            return true;
        }

        case WireTypeId::StringMap: {
            if (!type) return false;
            const auto* ctn = dynamic_cast<const ayt::reflect::IContainerTypeInfo*>(type);
            if (!ctn) return false;
            ayt::reflect::ITypeInfo* valueType = ctn->getElementType();
            if (!valueType) return false;
            if (s.getBitPosition() + 40 > s.getBitCount()) return false;
            const uint8_t  valueWidByte = s.readUInt8();
            const uint32_t N             = s.readUInt32();
            const WireTypeId valueWid = static_cast<WireTypeId>(valueWidByte);
            // Clear existing entries so server-authoritative state wins.
            ctn->resize(fieldPtr, 0);
            for (uint32_t i = 0; i < N; ++i) {
                if (s.getBitPosition() + 16 > s.getBitCount()) return false;
                const uint16_t klen = s.readUInt16();
                if (s.getBitPosition() + size_t(klen) * 8 > s.getBitCount()) return false;
                std::string k;
                k.resize(klen);
                for (uint16_t j = 0; j < klen; ++j) {
                    k[j] = static_cast<char>(s.readByte());
                }
                // Read value into a temporary then putEntry into the map.
                // Use ITypeInfo::create to allocate a default-constructed V,
                // then writeWireValue fills it, then putEntry inserts (key, V).
                void* valPtr = valueType->create();
                if (!valPtr) return false;
                const bool ok = readWireValue(s, valueWid, valueType, valPtr);
                if (!ok) {
                    valueType->destroy(valPtr);
                    return false;
                }
                // putEntry is on MapTypeInfo specifically; we cast through IContainerTypeInfo
                // by using the public putEntry on the concrete type. The standard IContainer
                // interface doesn't have putEntry, so we use a downcast.
                auto* mapInfo = dynamic_cast<const ayt::reflect::MapTypeInfoBase*>(ctn);
                if (!mapInfo) {
                    valueType->destroy(valPtr);
                    return false;
                }
                mapInfo->putEntry(fieldPtr, k, valPtr);
                valueType->destroy(valPtr);
            }
            return true;
        }
    }
    return false;
}

// =============================================================================
// R3.2 (2026-07-28): hashFieldValueEx
//
// Hash a single field value (any WireTypeId, including 12..15 nested types).
// For primitives this delegates to the R3.1 CRC32C path. For nested types it
// walks all elements / fields and produces a deterministic hash that changes
// whenever ANY inner value changes — "whole-field" granularity per the
// R3.2 design decision (per-element hashing deferred to R3.3+).
// =============================================================================
namespace {

uint32_t hashPrimitiveValue(WireTypeId wid, const void* fieldPtr) {
    switch (wid) {
        case WireTypeId::Bool:   { bool    v; std::memcpy(&v, fieldPtr, sizeof(v)); return PacketCodec::computeCrc32c(reinterpret_cast<const uint8_t*>(&v), sizeof(v)); }
        case WireTypeId::Int8:   { int8_t  v; std::memcpy(&v, fieldPtr, sizeof(v)); return PacketCodec::computeCrc32c(reinterpret_cast<const uint8_t*>(&v), sizeof(v)); }
        case WireTypeId::Int16:  { int16_t v; std::memcpy(&v, fieldPtr, sizeof(v)); return PacketCodec::computeCrc32c(reinterpret_cast<const uint8_t*>(&v), sizeof(v)); }
        case WireTypeId::Int32:  { int32_t v; std::memcpy(&v, fieldPtr, sizeof(v)); return PacketCodec::computeCrc32c(reinterpret_cast<const uint8_t*>(&v), sizeof(v)); }
        case WireTypeId::Int64:  { int64_t v; std::memcpy(&v, fieldPtr, sizeof(v)); return PacketCodec::computeCrc32c(reinterpret_cast<const uint8_t*>(&v), sizeof(v)); }
        case WireTypeId::UInt8:  { uint8_t  v; std::memcpy(&v, fieldPtr, sizeof(v)); return PacketCodec::computeCrc32c(reinterpret_cast<const uint8_t*>(&v), sizeof(v)); }
        case WireTypeId::UInt16: { uint16_t v; std::memcpy(&v, fieldPtr, sizeof(v)); return PacketCodec::computeCrc32c(reinterpret_cast<const uint8_t*>(&v), sizeof(v)); }
        case WireTypeId::UInt32: { uint32_t v; std::memcpy(&v, fieldPtr, sizeof(v)); return PacketCodec::computeCrc32c(reinterpret_cast<const uint8_t*>(&v), sizeof(v)); }
        case WireTypeId::UInt64: { uint64_t v; std::memcpy(&v, fieldPtr, sizeof(v)); return PacketCodec::computeCrc32c(reinterpret_cast<const uint8_t*>(&v), sizeof(v)); }
        case WireTypeId::Float:  { float    v; std::memcpy(&v, fieldPtr, sizeof(v)); return PacketCodec::computeCrc32c(reinterpret_cast<const uint8_t*>(&v), sizeof(v)); }
        case WireTypeId::Double: { double   v; std::memcpy(&v, fieldPtr, sizeof(v)); return PacketCodec::computeCrc32c(reinterpret_cast<const uint8_t*>(&v), sizeof(v)); }
        case WireTypeId::String: {
            const auto* strPtr = static_cast<const std::string*>(fieldPtr);
            return PacketCodec::computeCrc32c(
                reinterpret_cast<const uint8_t*>(strPtr->data()), strPtr->size());
        }
        default: return 0;
    }
}

} // anonymous namespace

uint32_t ReflectSerializer::hashFieldValueEx(WireTypeId wid,
                                              const ayt::reflect::ITypeInfo* type,
                                              const void* fieldPtr) {
    if (!fieldPtr) return 0;
    switch (wid) {
        case WireTypeId::Bool:
        case WireTypeId::Int8:
        case WireTypeId::Int16:
        case WireTypeId::Int32:
        case WireTypeId::Int64:
        case WireTypeId::UInt8:
        case WireTypeId::UInt16:
        case WireTypeId::UInt32:
        case WireTypeId::UInt64:
        case WireTypeId::Float:
        case WireTypeId::Double:
        case WireTypeId::String:
            return hashPrimitiveValue(wid, fieldPtr);

        case WireTypeId::NestedStruct: {
            if (!type) return 0;
            uint32_t h = static_cast<uint32_t>(type->getId() & 0xFFFFFFFFu);
            const uint32_t total = type->getFieldCount();
            for (uint32_t i = 0; i < total; ++i) {
                const auto* f = type->getField(i);
                if (!f) continue;
                if (!f->hasAttribute(ayt::reflect::FieldAttribute::NetReplicate)) continue;
                WireTypeId fwid;
                if (!resolveWireTypeId(f->getType(), fwid)) continue;
                const uint32_t fh = hashFieldValueEx(fwid, f->getType(), f->get(fieldPtr));
                h ^= fh * 16777619u;
            }
            return h;
        }

        case WireTypeId::FixedArray:
        case WireTypeId::DynamicArray: {
            if (!type) return 0;
            const auto* ctn = dynamic_cast<const ayt::reflect::IContainerTypeInfo*>(type);
            if (!ctn) return 0;
            ayt::reflect::ITypeInfo* elemType = ctn->getElementType();
            if (!elemType) return 0;
            WireTypeId elemWid;
            resolveWireTypeId(elemType, elemWid); // may be 0 (Unknown) — handled by hashFieldValueEx
            const size_t N = ctn->getContainerSize(fieldPtr);
            uint32_t h = 0x811C9DC5u ^ static_cast<uint32_t>(N);
            for (size_t i = 0; i < N; ++i) {
                const void* ePtr = ctn->getElementAt(fieldPtr, i);
                if (!ePtr) continue;
                const uint32_t eh = hashFieldValueEx(elemWid, elemType, ePtr);
                h ^= (eh + 0x9E3779B9u + (h << 6) + (h >> 2)); // boost::hash_combine
            }
            return h;
        }

        case WireTypeId::StringMap: {
            if (!type) return 0;
            const auto* ctn = dynamic_cast<const ayt::reflect::IContainerTypeInfo*>(type);
            if (!ctn) return 0;
            ayt::reflect::ITypeInfo* valueType = ctn->getElementType();
            if (!valueType) return 0;
            WireTypeId valueWid;
            resolveWireTypeId(valueType, valueWid);
            const size_t N = ctn->getContainerSize(fieldPtr);
            uint32_t h = 0x811C9DC5u ^ static_cast<uint32_t>(N);
            for (size_t i = 0; i < N; ++i) {
                const void* kPtr = ctn->getKeyAt(fieldPtr, i);
                const void* vPtr = ctn->getValueAt(fieldPtr, i);
                if (!kPtr || !vPtr) continue;
                const auto& k = *static_cast<const std::string*>(kPtr);
                const uint32_t kh = PacketCodec::computeCrc32c(
                    reinterpret_cast<const uint8_t*>(k.data()), k.size());
                const uint32_t vh = hashFieldValueEx(valueWid, valueType, vPtr);
                h ^= (kh * 16777619u) ^ (vh + 0x9E3779B9u);
            }
            return h;
        }
    }
    return 0;
}

} // namespace ayt::net
