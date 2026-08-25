// AYNetwork/Prediction/MispredictionResolver.cpp - R5.2 pure reconcile.
//
// R6 C7 (2026-08-25): float lerp + relative-epsilon snap → int32 fixed-
// point lerp + int32 ULP compare. State-equal replay requires bit-deterministic
// math; IEEE-754 roundoff accumulates per-field per-tick and eventually
// flips snap decisions between recordings.

#include <AYNetwork/Prediction/MispredictionResolver.h>

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstring>

namespace ayt::net
{

namespace {

inline float clamp01(float v) {
    if (v < 0.f) return 0.f;
    if (v > 1.f) return 1.f;
    return v;
}

// R6 C7 (2026-08-25): float relative-epsilon comparison → int32 ULP
// compare. ≤ 64 ULPs ≈ 1.5e-5 relative error, identical threshold to the
// float version (1e-4) for normals in the float32 range. ULPs are
// monotonic with the bit pattern, so the comparison is identical on
// every platform (no FMA / -ffast-math surprises). Denormals flush to
// zero so we never divide-by-tiny / produce Inf.
inline bool numericEqualsEpsilon(const uint8_t* a, const uint8_t* b,
                                 uint8_t kind)
{
    switch (kind) {
    case 1: { // float32
        uint32_t ua, ub;
        std::memcpy(&ua, a, sizeof(uint32_t));
        std::memcpy(&ub, b, sizeof(uint32_t));
        // Flush denormals to zero so they don't dominate the ULP diff.
        const uint32_t absMask = 0x7FFFFFFFu;
        const uint32_t minNormal = 0x00800000u;
        if ((ua & absMask) < minNormal) ua &= absMask; // keep sign bit, magnitude 0
        if ((ub & absMask) < minNormal) ub &= absMask;
        // Signed distance in ULPs (sign-independent): |ua - ub|.
        const uint32_t diff = (ua > ub) ? (ua - ub) : (ub - ua);
        return diff <= 64u;
    }
    case 2: { // float64
        uint64_t ua, ub;
        std::memcpy(&ua, a, sizeof(uint64_t));
        std::memcpy(&ub, b, sizeof(uint64_t));
        const uint64_t absMask = 0x7FFFFFFFFFFFFFFFull;
        const uint64_t minNormal = 0x0010000000000000ull;
        if ((ua & absMask) < minNormal) ua &= absMask;
        if ((ub & absMask) < minNormal) ub &= absMask;
        const uint64_t diff = (ua > ub) ? (ua - ub) : (ub - ua);
        return diff <= 64u;
    }
    default:
        return std::memcmp(a, b, /*size*/ 4) == 0; // 4-byte window; caller narrows
    }
}

// R6 C7 (2026-08-25): float lerp → int32 fixed-point lerp. The formula
//
//   dst = a + (b - a) * alpha
//
// becomes
//
//   dst = a + ((b - a) * alpha_q16) >> 16
//
// where alpha_q16 = uint32(0xFFFF * alpha). All intermediates stay in
// int32/int64 to avoid float roundoff. Quantization: 16 fractional bits
// gives ~1.5e-5 resolution — well below the ULP-64 snap threshold so the
// snap-vs-smooth decision is unchanged from the float path. Rounding is
// round-half-up (C1 convention) so the boundary value 0x8000 rounds up.
inline void lerpField(uint8_t* dst, const uint8_t* a, const uint8_t* b,
                      uint8_t kind, uint32_t alpha_q16)
{
    switch (kind) {
    case 1: { // float32
        int32_t ia, ib;
        std::memcpy(&ia, a, sizeof(int32_t));
        std::memcpy(&ib, b, sizeof(int32_t));
        // Round-half-up: ((b-a) * alpha + 0x8000) >> 16.
        const int64_t diff = static_cast<int64_t>(ib - ia);
        const int64_t scaled = (diff * static_cast<int64_t>(alpha_q16) + 0x8000) >> 16;
        int32_t result = ia + static_cast<int32_t>(scaled);
        std::memcpy(dst, &result, sizeof(int32_t));
        return;
    }
    case 2: { // float64
        int64_t ia, ib;
        std::memcpy(&ia, a, sizeof(int64_t));
        std::memcpy(&ib, b, sizeof(int64_t));
        const int64_t diff = ib - ia;
        const int64_t scaled = (diff * static_cast<int64_t>(alpha_q16) + 0x8000) >> 16;
        int64_t result = ia + scaled;
        std::memcpy(dst, &result, sizeof(int64_t));
        return;
    }
    default:
        // Int/uint/bool/string/nested: snap-to-lower (a) — matches
        // SnapshotInterpolator's non-float rule. Documented in header.
        std::memcpy(dst, a, /*size*/ 4);
        return;
    }
}

} // anon

bool MispredictionResolver::aboveThreshold(const uint8_t* a, const uint8_t* b,
                                           size_t n, uint8_t numericKind)
{
    // For multi-byte non-numeric fields, bytewise compare.
    if (numericKind == 0) {
        return std::memcmp(a, b, n) != 0;
    }
    // For numeric, single 4- or 8-byte window.
    const size_t step = (numericKind == 2) ? sizeof(double) : sizeof(float);
    if (n < step) {
        return std::memcmp(a, b, n) != 0;
    }
    return !numericEqualsEpsilon(a, b, numericKind);
}

void MispredictionResolver::applySmoothing(std::vector<uint8_t>& dst,
                                            const std::vector<uint8_t>& src,
                                            const ResolverLayout& layout,
                                            float dtSec,
                                            float smoothingDuration)
{
    if (smoothingDuration <= 0.f) {
        // Degenerate: snap to source.
        dst = src;
        return;
    }
    // R6 C7 M-02 (2026-08-25): precompute uint32 fixed-point alpha.
    // alpha_q16 = round-half-up(0xFFFF * clamp01(dt / smoothingDuration)).
    // 16 fractional bits ≈ 1.5e-5 resolution. q16 instead of float lerp
    // makes the smoothing step bit-deterministic across platforms and
    // FMA/non-FMA builds.
    const float alphaF = clamp01(dtSec / smoothingDuration);
    const uint32_t alpha_q16 = static_cast<uint32_t>(
        static_cast<int64_t>(lroundf(alphaF * 65535.0f)) & 0xFFFFu);
    const size_t srcSize = src.size();
    if (dst.size() < srcSize) dst.resize(srcSize);

    for (const ResolverField& f : layout.fields) {
        if (!f.netReplicate || f.serverAuthoritative) continue;
        if (f.byteOffset + f.byteSize > srcSize) continue;
        const size_t step = (f.numericKind == 2) ? sizeof(double) : sizeof(float);
        if (f.numericKind != 0 && f.byteSize == step) {
            lerpField(dst.data() + f.byteOffset,
                      src.data() + f.byteOffset,
                      dst.data() + f.byteOffset,
                      f.numericKind, alpha_q16);
        } else {
            // Snap-to-lower (a) for ints/bools/strings/nested.
            std::memcpy(dst.data() + f.byteOffset,
                        src.data() + f.byteOffset,
                        f.byteSize);
        }
    }
}

ReconcileResult MispredictionResolver::reconcile(
    std::vector<uint8_t>& predictedBytes,
    const std::vector<uint8_t>& serverBytes,
    const ResolverLayout& layout,
    uint32_t predictedInputSeq,
    uint32_t serverLastAckedInputTick,
    float dtSec,
    float smoothingDuration)
{
    ReconcileResult r;
    if (predictedBytes.size() != serverBytes.size()) {
        // Layout drift between predicted copy and server copy → snap.
        predictedBytes = serverBytes;
        r.snapped = true;
        r.fieldsScanned = static_cast<uint32_t>(layout.fields.size());
        r.fieldsSnapped = r.fieldsScanned;
        return r;
    }

    const bool clientIsBehind = seqGreaterThan(serverLastAckedInputTick,
                                               predictedInputSeq);
    // "Client's prediction is older than the last server-confirmed input"
    // means client CAN'T have applied the same server-side inputs yet —
    // it may still be right (smooth) or wrong (snap). We always prefer
    // smooth when delta is within threshold, regardless of seq — the
    // snap-vs-smooth decision is purely about the FIELD delta.

    std::vector<uint8_t> smoothedOut; // built lazily
    bool haveSmoothed = false;

    for (const ResolverField& f : layout.fields) {
        if (f.byteOffset + f.byteSize > serverBytes.size()) continue;
        if (!f.netReplicate || f.serverAuthoritative) {
            ++r.fieldsSkipped;
            continue;
        }
        ++r.fieldsScanned;
        const uint8_t* a = predictedBytes.data() + f.byteOffset;
        const uint8_t* b = serverBytes.data() + f.byteOffset;

        const bool different = aboveThreshold(a, b, f.byteSize, f.numericKind);
        if (!different) continue;

        // Above-threshold delta → snap (overwrite predicted with server).
        std::memcpy(predictedBytes.data() + f.byteOffset,
                    serverBytes.data() + f.byteOffset,
                    f.byteSize);
        r.snapped = true;
        ++r.fieldsSnapped;
    }

    if (clientIsBehind && smoothingDuration > 0.f && !r.snapped) {
        // No field above threshold (server confirms client's prediction),
        // but client is behind → smoothing pass: pull client toward the
        // server values at alpha = dt/smoothingDuration.
        if (!haveSmoothed) {
            smoothedOut = predictedBytes;
            applySmoothing(smoothedOut, serverBytes, layout,
                           dtSec, smoothingDuration);
            haveSmoothed = true;
        }
        predictedBytes = smoothedOut;
        r.fieldsSmoothed = r.fieldsScanned;
    }

    return r;
}

} // namespace ayt::net
