// AYNetwork/Prediction/MispredictionResolver.cpp - R5.2 pure reconcile.
//
// R6 C7 (2026-08-25): threshold decisions use ordered IEEE-754 ULP distance.
// Smoothing still operates on numeric float/double values; interpolating the
// integer representation of their bit patterns is not numerically meaningful.

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

// R6 C7 (2026-08-25): float relative-epsilon comparison → ordered ULP
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
        if ((ua & absMask) < minNormal) ua = 0;
        if ((ub & absMask) < minNormal) ub = 0;
        if ((ua & 0x7F800000u) == 0x7F800000u ||
            (ub & 0x7F800000u) == 0x7F800000u) return ua == ub;
        const auto ordered = [](uint32_t bits) {
            return (bits & 0x80000000u) ? ~bits : (bits | 0x80000000u);
        };
        const uint32_t oa = ordered(ua);
        const uint32_t ob = ordered(ub);
        const uint32_t diff = (oa > ob) ? (oa - ob) : (ob - oa);
        return diff <= 64u;
    }
    case 2: { // float64
        uint64_t ua, ub;
        std::memcpy(&ua, a, sizeof(uint64_t));
        std::memcpy(&ub, b, sizeof(uint64_t));
        const uint64_t absMask = 0x7FFFFFFFFFFFFFFFull;
        const uint64_t minNormal = 0x0010000000000000ull;
        if ((ua & absMask) < minNormal) ua = 0;
        if ((ub & absMask) < minNormal) ub = 0;
        if ((ua & 0x7FF0000000000000ull) == 0x7FF0000000000000ull ||
            (ub & 0x7FF0000000000000ull) == 0x7FF0000000000000ull) return ua == ub;
        const auto ordered = [](uint64_t bits) {
            return (bits & 0x8000000000000000ull)
                ? ~bits : (bits | 0x8000000000000000ull);
        };
        const uint64_t oa = ordered(ua);
        const uint64_t ob = ordered(ub);
        const uint64_t diff = (oa > ob) ? (oa - ob) : (ob - oa);
        return diff <= 64u;
    }
    default:
        return std::memcmp(a, b, /*size*/ 4) == 0; // 4-byte window; caller narrows
    }
}

// Alpha is quantized to q16 so callers make the same smoothing-step choice;
// field values themselves are interpolated numerically.
inline void lerpField(uint8_t* dst, const uint8_t* a, const uint8_t* b,
                      uint8_t kind, uint32_t alpha_q16)
{
    switch (kind) {
    case 1: { // float32
        float va, vb;
        std::memcpy(&va, a, sizeof(va));
        std::memcpy(&vb, b, sizeof(vb));
        const float alpha = static_cast<float>(alpha_q16) / 65535.0f;
        volatile float delta = vb - va;
        volatile float scaled = delta * alpha;
        const float result = va + scaled;
        std::memcpy(dst, &result, sizeof(result));
        return;
    }
    case 2: { // float64
        double va, vb;
        std::memcpy(&va, a, sizeof(va));
        std::memcpy(&vb, b, sizeof(vb));
        const double alpha = static_cast<double>(alpha_q16) / 65535.0;
        volatile double delta = vb - va;
        volatile double scaled = delta * alpha;
        const double result = va + scaled;
        std::memcpy(dst, &result, sizeof(result));
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
    // R6 C7 M-02 (2026-08-25): precompute a q16 alpha.
    // alpha_q16 = round-half-up(0xFFFF * clamp01(dt / smoothingDuration)).
    // 16 fractional bits ≈ 1.5e-5 resolution. q16 instead of float lerp
    // stabilizes the selected smoothing ratio across runs.
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
                      dst.data() + f.byteOffset,
                      src.data() + f.byteOffset,
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

    const bool predictionCoveredByAck =
        !seqGreaterThan(predictedInputSeq, serverLastAckedInputTick);
    const float alphaF = smoothingDuration > 0.f
        ? clamp01(dtSec / smoothingDuration) : 1.f;
    const uint32_t alphaQ16 = static_cast<uint32_t>(
        static_cast<int64_t>(lroundf(alphaF * 65535.0f)) & 0xFFFFu);

    for (const ResolverField& f : layout.fields) {
        if (f.byteOffset + f.byteSize > serverBytes.size()) continue;
        if (!f.netReplicate || f.serverAuthoritative) {
            ++r.fieldsSkipped;
            continue;
        }
        ++r.fieldsScanned;
        const uint8_t* a = predictedBytes.data() + f.byteOffset;
        const uint8_t* b = serverBytes.data() + f.byteOffset;

        if (std::memcmp(a, b, f.byteSize) == 0) continue;
        const bool above = aboveThreshold(a, b, f.byteSize, f.numericKind);
        const bool shouldSnap = above ? predictionCoveredByAck
                                      : !predictionCoveredByAck;
        if (shouldSnap || smoothingDuration <= 0.f || f.numericKind == 0) {
            std::memcpy(predictedBytes.data() + f.byteOffset, b, f.byteSize);
            r.snapped = true;
            ++r.fieldsSnapped;
        } else {
            const size_t step = (f.numericKind == 2) ? sizeof(double) : sizeof(float);
            if (f.byteSize == step) {
                lerpField(predictedBytes.data() + f.byteOffset,
                          a, b, f.numericKind, alphaQ16);
                ++r.fieldsSmoothed;
            } else {
                std::memcpy(predictedBytes.data() + f.byteOffset, b, f.byteSize);
                r.snapped = true;
                ++r.fieldsSnapped;
            }
        }
    }

    return r;
}

} // namespace ayt::net
