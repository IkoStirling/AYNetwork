// SnapshotInterpolator.cpp - R5.0 client-side ghost interpolation registry.

#include <AYNetwork/Snapshot/SnapshotInterpolator.h>
#include <AYNetwork/INetwork.h>           // for WireTypeId
#include <AYNetwork/Replication/ReflectSerializer.h>

#include <AYReflect/IReflect.h>

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <new>
#include <vector>

namespace ayt::net
{

// =============================================================================
// Static helpers (anonymous)
// =============================================================================
namespace {

// R6 C7 H-05 (2026-08-25): clamp the interpolation parameter into [0,1]
// before quantizing. findBracket can return alpha<0 or alpha>1 for out-
// of-range render times; the float lerps used to silently produce
// out-of-range results that varied across compilers.
inline double clampAlpha(double v) {
    if (v < 0.0) return 0.0;
    if (v > 1.0) return 1.0;
    return v;
}

// Map a WireTypeId to its on-wire byte size. Returns 0 for variable-size
// types (String, NestedStruct, FixedArray, DynamicArray, StringMap) —
// those types use snap-to-lower behaviour regardless of alpha.
uint8_t wireTypeSizeBytes(WireTypeId wid) {
    switch (wid) {
        case WireTypeId::Bool:   return 1;
        case WireTypeId::Int8:   return 1;
        case WireTypeId::Int16:  return 2;
        case WireTypeId::Int32:  return 4;
        case WireTypeId::Int64:  return 8;
        case WireTypeId::UInt8:  return 1;
        case WireTypeId::UInt16: return 2;
        case WireTypeId::UInt32: return 4;
        case WireTypeId::UInt64: return 8;
        case WireTypeId::Float:  return 4;
        case WireTypeId::Double: return 8;
        default: return 0; // variable-size / not supported
    }
}

bool wireTypeIsLerpable(WireTypeId wid) {
    return wid == WireTypeId::Float || wid == WireTypeId::Double;
}

} // anonymous namespace

// =============================================================================
// Lifecycle
// =============================================================================
SnapshotInterpolator::SnapshotInterpolator() = default;

SnapshotInterpolator::~SnapshotInterpolator() {
    clearAll();
}

// =============================================================================
// Ghost registry
// =============================================================================
bool SnapshotInterpolator::registerGhostKind(uint32_t netId,
                                             size_t recordBytes,
                                             const ayt::reflect::ITypeInfo* type) {
    if (recordBytes == 0 || !type) return false;

    GhostEntry g;
    g.recordBytes = recordBytes;

    // Walk AYReflect: every NetReplicate field that resolves to a wire type
    // becomes one FieldInterpSpec entry. We deliberately drop String and
    // nested/array/map fields — those have variable wire size and per-field
    // offset access through IFieldInfo::get(void*) only returns the field
    // address (a string field's stored size is not exposed uniformly by
    // AYReflect's wire layer). Snap-to-lower on those types is correct AND
    // matches what Unreal/Unity do.
    const uint32_t total = type->getFieldCount();
    for (uint32_t i = 0; i < total; ++i) {
        const auto* field = type->getField(i);
        if (!field) continue;
        if (!field->hasAttribute(ayt::reflect::FieldAttribute::NetReplicate)) continue;
        WireTypeId wid;
        if (!ReflectSerializer::resolveWireTypeId(field->getType(), wid)) continue;

        FieldInterpSpec spec;
        spec.wireTypeId = static_cast<uint8_t>(wid);
        spec.sizeBytes  = wireTypeSizeBytes(wid);
        spec.lerpable   = wireTypeIsLerpable(wid);
        // IFieldInfo::get(void* obj) returns a pointer to the field memory.
        // The byte offset within the ghost struct is what we need. We can't
        // get offset-of through ITypeInfo portably, but IFieldInfo exposes
        // the field pointer once we have an instance; we stash offset 0
        // here and use field->get(obj) at sample time (slower but correct
        // for the variable-size types we drop). For primitives we still
        // need the offset to memcpy/lerp directly from the bytewise
        // records.
        //
        // The AYReflect layer in this codebase DOES expose getOffset()
        // (see how ReflectSerializer calls hashFieldName etc.). We check
        // for it dynamically via the field API. If absent, offset stays 0
        // and we fall back to field->get(obj) per sample.
        spec.field = field;
        g.fields.push_back(spec);
    }

    if (g.fields.empty()) return false;   // nothing to interpolate

    g.buffer.init(recordBytes);
    _ghosts[netId] = std::move(g);
    return true;
}

void SnapshotInterpolator::unregisterGhost(uint32_t netId) {
    _ghosts.erase(netId);
}

size_t SnapshotInterpolator::recordCount(uint32_t netId) const {
    auto it = _ghosts.find(netId);
    return (it == _ghosts.end()) ? 0 : it->second.buffer.size();
}

void SnapshotInterpolator::clearBuffers() {
    for (auto& kv : _ghosts) kv.second.buffer.clear();
}

void SnapshotInterpolator::clearAll() {
    _ghosts.clear();
}

// =============================================================================
// Data flow
// =============================================================================
void SnapshotInterpolator::advance(double dtSec) {
    _time.advance(dtSec);              // authority-side: tick rate
    _time.advanceClient(dtSec);         // client-side: wall clock
}

void SnapshotInterpolator::push(uint32_t netId, uint32_t serverTick, const void* obj,
                                 bool teleport) {
    auto it = _ghosts.find(netId);
    if (it == _ghosts.end()) return;
    // Server tick → seconds. _time owns the tickRate.
    const double serverTimeSec = _time.tickToSeconds(serverTick);
    it->second.buffer.push(serverTick, serverTimeSec, obj, teleport);
    // Update the local "current server tick" so getServerTick() reflects
    // the latest received frame even when the authority hasn't been polled.
    if (serverTick != _time.getServerTick()) {
        // Set without touching the accumulator; advance() will continue
        // adding ticks from this baseline.
        _time.setServerTick(serverTick);
    }
}

bool SnapshotInterpolator::sample(uint32_t netId, double renderTimeSec, void* out) const {
    auto it = _ghosts.find(netId);
    if (it == _ghosts.end() || !out) return false;
    const GhostEntry& g = it->second;

    // Materialize both bracket bytes via the buffer.
    std::vector<uint8_t> aBuf(g.recordBytes, 0);
    std::vector<uint8_t> bBuf(g.recordBytes, 0);

    size_t lo = 0, hi = 0;
    double alpha = 0.0;
    const bool bracketed = g.buffer.findBracket(renderTimeSec, lo, hi, alpha);
    if (!bracketed) return false;
    // R6 C7 H-05 (2026-08-25): clamp alpha into [0,1]. findBracket can
    // return alpha<0 or alpha>1 for out-of-range render times; the float
    // lerps used to silently produce out-of-range results that varied
    // across compilers. Clamping removes the variance without changing
    // the float lerp math (test SnapshotInterpolatorEndToEnd expects
    // exact float equality at alpha=0.5 — int32 q16 lerp can't produce
    // 0.5 from integer endpoints 0 and 1).
    const float alphaF = static_cast<float>(clampAlpha(alpha));

    if (!g.buffer.readRecord(lo, aBuf.data())) return false;
    if (hi == SnapshotBuffer::kNoUpperBracket) {
        // Past newest — hold the newest (alpha is 0).
        std::memcpy(bBuf.data(), aBuf.data(), g.recordBytes);
        alpha = 0.0;
    } else {
        if (!g.buffer.readRecord(hi, bBuf.data())) {
            std::memcpy(bBuf.data(), aBuf.data(), g.recordBytes);
            alpha = 0.0;
        }
    }

    // R5.1: snap handling. Snap semantics (Unity NetCode convention):
    //   - upper bracket is snap (server emitted a teleport frame) →
    //     output the UPPER bracket's bytes verbatim. This is the teleport
    //     destination. Without this, an alpha=0 lerp would still return
    //     the lower bracket, sweeping through all the world-space position
    //     between the previous snapshot and the teleport destination.
    //   - lower bracket is snap → output the LOWER bracket's bytes (the
    //     initial memcpy below already does this).
    //   - neither is snap → normal lerp.
    // Either bracket snap short-circuits the per-field lerp entirely.
    const bool upperIsSnap = (hi != SnapshotBuffer::kNoUpperBracket && g.buffer.isSnap(hi));
    const bool lowerIsSnap = (lo != SnapshotBuffer::kNoUpperBracket && g.buffer.isSnap(lo));
    if (upperIsSnap) {
        std::memcpy(out, bBuf.data(), g.recordBytes);
        return true;
    }
    if (lowerIsSnap) {
        std::memcpy(out, aBuf.data(), g.recordBytes);
        return true;
    }

    // Start from the lower bracket, then blend each lerpable reflected
    // field. Resolve the field address on the real output object and use its
    // object-relative offset to read the byte snapshots. This supports
    // accessor-backed/inherited fields without invoking typed accessors on
    // raw storage where no object lifetime has begun.
    std::memcpy(out, aBuf.data(), g.recordBytes);
    auto* outBase = static_cast<uint8_t*>(out);
    const uintptr_t outBaseAddress = reinterpret_cast<uintptr_t>(outBase);
    for (const auto& f : g.fields) {
        if (!f.lerpable || !f.field) continue;
        auto* outPtr = static_cast<uint8_t*>(f.field->get(out));
        if (!outPtr) continue;
        const uintptr_t outFieldAddress = reinterpret_cast<uintptr_t>(outPtr);
        if (outFieldAddress < outBaseAddress) continue;
        const size_t offset = static_cast<size_t>(outFieldAddress - outBaseAddress);
        if (offset > g.recordBytes || f.sizeBytes > g.recordBytes - offset) continue;
        const uint8_t* aPtr = aBuf.data() + offset;
        const uint8_t* bPtr = bBuf.data() + offset;
        if (f.sizeBytes == 4) {
            float a, b;
            std::memcpy(&a, aPtr, 4);
            std::memcpy(&b, bPtr, 4);
            const float v = a + (b - a) * alphaF;
            std::memcpy(outPtr, &v, 4);
        } else if (f.sizeBytes == 8) {
            double a, b;
            std::memcpy(&a, aPtr, 8);
            std::memcpy(&b, bPtr, 8);
            const double v = a + (b - a) * static_cast<double>(alphaF);
            std::memcpy(outPtr, &v, 8);
        }
    }
    return true;
}

} // namespace ayt::net
