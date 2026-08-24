#pragma once
// AYNetwork/Prediction/MispredictionResolver.h - R5.2 pure reconcile function.
//
// Design notes
// =============
// MispredictionResolver is a pure function over predicted/server byte
// buffers + a FieldLayout (from ReflectSerializer). It does NOT touch
// any networking, threading, or registry. Tests can drive it with
// synthetic inputs.
//
// Per-field rules
// ---------------
//   - Field tagged ServerAuthoritative: SKIP (server is source of truth;
//     clients never predict it; resolver asserts and continues).
//   - Field not tagged NetReplicate: SKIP.
//   - Field tag NetReplicate + delta within threshold:
//       smooth (lerp toward server) when predicted.inputSeq <=
//       server.lastAckedInputTick, else snap.
//   - Field tag NetReplicate + delta above threshold:
//       snap (overwrite predicted with server value) when
//       predicted.inputSeq <= server.lastAckedInputTick; otherwise
//       smooth.
//
// snap = "client was wrong; trust server fully".
// smooth = "client was right but behind; lerp to catch up".
//
// Threshold policy
// ----------------
//   - Float / Double: relative epsilon (1e-4f / 1e-6).
//   - Int* / UInt* / Bool / String / Nested: exact byte compare.

#include <AYNetwork/Prediction/PredictionManager.h>

#include <cstdint>
#include <cstddef>
#include <vector>

namespace ayt::net
{

// One field descriptor for the resolver. Reuses the same FieldLayout
// the ReflectSerializer already maintains (cached at schema time).
struct ResolverField {
    const char* name = nullptr;
    uint32_t    byteOffset = 0;
    uint32_t    byteSize = 0;
    // 0 = unknown, 1 = float, 2 = double, 3 = int/uint/bool/string/nested
    uint8_t     numericKind = 0;
    bool        netReplicate = false;
    bool        serverAuthoritative = false;
};

struct ResolverLayout {
    std::vector<ResolverField> fields;
};

struct ReconcileResult {
    bool snapped = false;            // any field snapped this call
    uint32_t fieldsScanned = 0;      // NetReplicate fields walked
    uint32_t fieldsSmoothed = 0;
    uint32_t fieldsSnapped = 0;
    uint32_t fieldsSkipped = 0;      // not NetReplicate OR ServerAuthoritative
};

class MispredictionResolver {
public:
    // Compare predicted.bytes vs server.bytes using layout, write the
    // reconciled result back into predicted.bytes. Returns the reconcile
    // summary (no allocations).
    static ReconcileResult reconcile(std::vector<uint8_t>& predictedBytes,
                                     const std::vector<uint8_t>& serverBytes,
                                     const ResolverLayout& layout,
                                     uint32_t predictedInputSeq,
                                     uint32_t serverLastAckedInputTick,
                                     float dtSec,
                                     float smoothingDuration);

    // Test seam: apply smoothing in isolation. src→dst via
    // exponential alpha = clamp(dtSec/smoothingDuration, 0, 1). Field-
    // agnostic bytewise lerp via the layout's numericKind.
    static void applySmoothing(std::vector<uint8_t>& dst,
                               const std::vector<uint8_t>& src,
                               const ResolverLayout& layout,
                               float dtSec,
                               float smoothingDuration);

    // Pure byte-level threshold comparator (test seam).
    static bool aboveThreshold(const uint8_t* a, const uint8_t* b,
                               size_t n, uint8_t numericKind);
};

} // namespace ayt::net
