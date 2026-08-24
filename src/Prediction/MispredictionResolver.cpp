// AYNetwork/Prediction/MispredictionResolver.cpp - R5.2 pure reconcile.

#include <AYNetwork/Prediction/MispredictionResolver.h>

#include <algorithm>
#include <cmath>
#include <cstring>

namespace ayt::net
{

namespace {

inline float clamp01(float v) {
    if (v < 0.f) return 0.f;
    if (v > 1.f) return 1.f;
    return v;
}

inline bool numericEqualsEpsilon(const uint8_t* a, const uint8_t* b,
                                 uint8_t kind)
{
    switch (kind) {
    case 1: { // float32
        float fa, fb;
        std::memcpy(&fa, a, sizeof(float));
        std::memcpy(&fb, b, sizeof(float));
        const float denom = std::max(std::fabs(fa), std::fabs(fb));
        if (denom < 1e-6f) return true; // both near zero
        return std::fabs(fa - fb) / denom <= 1e-4f;
    }
    case 2: { // float64
        double da, db;
        std::memcpy(&da, a, sizeof(double));
        std::memcpy(&db, b, sizeof(double));
        const double denom = std::max(std::fabs(da), std::fabs(db));
        if (denom < 1e-9) return true;
        return std::fabs(da - db) / denom <= 1e-6;
    }
    default:
        return std::memcmp(a, b, /*size*/ 4) == 0; // 4-byte window; caller narrows
    }
}

inline void lerpField(uint8_t* dst, const uint8_t* a, const uint8_t* b,
                      uint8_t kind, float alpha)
{
    switch (kind) {
    case 1: { // float32
        float fa, fb;
        std::memcpy(&fa, a, sizeof(float));
        std::memcpy(&fb, b, sizeof(float));
        float f = fa + (fb - fa) * alpha;
        std::memcpy(dst, &f, sizeof(float));
        return;
    }
    case 2: { // float64
        double da, db;
        std::memcpy(&da, a, sizeof(double));
        std::memcpy(&db, b, sizeof(double));
        double d = da + (db - da) * static_cast<double>(alpha);
        std::memcpy(dst, &d, sizeof(double));
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
    const float alpha = clamp01(dtSec / smoothingDuration);
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
                      f.numericKind, alpha);
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
