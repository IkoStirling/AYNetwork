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

#include <Replication/ReflectSerializer.h>

#include <ayreflect/IReflect.h>
#include <Protocol/PacketCodec.h>
#include <cstring>
#include <string>

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

uint16_t ReflectSerializer::hashFieldName(const char* name) {
    if (!name) return 0;
    return static_cast<uint16_t>(fnv1a32(name) & 0xFFFFu);
}

// =============================================================================
// resolveWireTypeId — maps AYReflect ITypeInfo::getId() (= typeid(T).hash_code())
// to a WireTypeId. R3.0 supports the 12 primitives only.
// =============================================================================
bool ReflectSerializer::resolveWireTypeId(const ayt::reflect::ITypeInfo* fieldType, WireTypeId& outId) {
    if (!fieldType) return false;
    const size_t id = fieldType->getId();

    // Direct typeid compares against the 12 primitives. Hash codes are
    // process-stable (MSVC guarantees; GCC/Clang same TU = same hash), so
    // server/client built from the same compiler produce matching ids.
    // If this assumption ever breaks (e.g. dynamic loading), the fallback
    // is to walk fieldType->getName() and string-match — but R3.0 stays
    // strict for predictability.
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
    else return false;
    return true;
}

// =============================================================================
// Per-type readers/writers — single switch, no virtuals.
// =============================================================================
namespace {

void writeFieldValue(BitStream& s, WireTypeId id, const void* fieldPtr) {
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
    }
    (void)posBefore;
    return true;
}

} // anonymous namespace

// =============================================================================
// ReplicationFrame header writers/readers
// =============================================================================
void ReflectSerializer::writeReplicationFrameHeader(BitStream& s, uint32_t netId, uint16_t typeHash, uint8_t fieldCount) {
    s.writeUInt32(netId);
    s.writeUInt16(typeHash);
    s.writeUInt8(fieldCount);
    s.writeUInt8(0); // reserved
}

bool ReflectSerializer::readReplicationFrameHeader(BitStream& s, FrameHeader& out) {
    if (s.getBitPosition() + 64 > s.getBitCount()) return false;
    out.netId      = s.readUInt32();
    out.typeHash   = s.readUInt16();
    out.fieldCount = s.readUInt8();
    out.reserved   = s.readUInt8();
    return true;
}

void ReflectSerializer::writeEntitySpawn(BitStream& s, uint32_t netId, uint16_t typeHash) {
    s.writeUInt32(netId);
    s.writeUInt16(typeHash);
}

bool ReflectSerializer::readEntitySpawn(BitStream& s, uint32_t& netId, uint16_t& typeHash) {
    if (s.getBitPosition() + 48 > s.getBitCount()) return false;
    netId    = s.readUInt32();
    typeHash = s.readUInt16();
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

    uint32_t fieldCount = 0;
    const uint32_t total = type->getFieldCount();

    // Pre-scan: count supported NetReplicate fields. The wire fieldCount must
    // reflect fields actually emitted, so unsupported types are skipped
    // without incrementing.
    for (uint32_t i = 0; i < total; ++i) {
        const auto* field = type->getField(i);
        if (!field) continue;
        if (!field->hasAttribute(ayt::reflect::FieldAttribute::NetReplicate)) continue;
        WireTypeId wid;
        if (!resolveWireTypeId(field->getType(), wid)) continue;
        fieldCount++;
    }

    // Nothing to send — caller may skip broadcasting this entity.
    if (fieldCount == 0) return false;

    const size_t typeHash = static_cast<size_t>(type->getId() & 0xFFFFu);
    writeReplicationFrameHeader(s, netId, static_cast<uint16_t>(typeHash), static_cast<uint8_t>(fieldCount));

    for (uint32_t i = 0; i < total; ++i) {
        const auto* field = type->getField(i);
        if (!field) continue;
        if (!field->hasAttribute(ayt::reflect::FieldAttribute::NetReplicate)) continue;

        WireTypeId wid;
        if (!resolveWireTypeId(field->getType(), wid)) continue;

        const uint16_t nameHash = hashFieldName(field->getName());
        s.writeUInt16(nameHash);
        s.writeUInt8(static_cast<uint8_t>(wid));
        writeFieldValue(s, wid, field->get(const_cast<void*>(obj)));
    }

    return true;
}

bool ReflectSerializer::deserializeObject(const ayt::reflect::ITypeInfo* type, void* obj, BitStream& s, uint8_t expectedFieldCount) {
    if (!type || !obj) return false;

    const uint32_t total = type->getFieldCount();
    for (uint8_t i = 0; i < expectedFieldCount; ++i) {
        // Truncation guard: at least 3 bytes (u16 + u8) per record header.
        if (s.getBitPosition() + 24 > s.getBitCount()) return false;

        const uint16_t nameHash = s.readUInt16();
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

        if (!readFieldValue(s, wid, field->get(obj))) return false;
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
    netFields.reserve(total);
    for (uint32_t i = 0; i < total; ++i) {
        const auto* f = type->getField(i);
        if (!f) continue;
        if (!f->hasAttribute(ayt::reflect::FieldAttribute::NetReplicate)) continue;
        WireTypeId wid;
        if (!resolveWireTypeId(f->getType(), wid)) continue; // unsupported type → skip
        netFields.push_back(f);
    }

    // Validate every requested dense index resolves to a supported field.
    for (uint32_t k = 0; k < fieldCount; ++k) {
        if (denseIndices[k] >= netFields.size()) return false;
    }

    const size_t typeHash = static_cast<size_t>(type->getId() & 0xFFFFu);
    writeReplicationFrameHeader(s, netId, static_cast<uint16_t>(typeHash),
                                static_cast<uint8_t>(fieldCount));

    for (uint32_t k = 0; k < fieldCount; ++k) {
        const auto* field = netFields[denseIndices[k]];
        WireTypeId wid;
        resolveWireTypeId(field->getType(), wid); // already verified above

        const uint16_t nameHash = hashFieldName(field->getName());
        s.writeUInt16(nameHash);
        s.writeUInt8(static_cast<uint8_t>(wid));
        writeFieldValue(s, wid, field->get(const_cast<void*>(obj)));
    }
    return true;
}

} // namespace ayt::net