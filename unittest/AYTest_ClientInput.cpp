// AYTest_ClientInput.cpp - R5.2 (2026-08-24) client prediction tests.
//
// 10 cases covering InputRing lifecycle, ClientInputCodec wire format,
// AckTail piggy-back round-trip, ack math wraparound, ServerAuthoritative
// field skip, MispredictionResolver snap-vs-smooth, consumeClientInputs
// ordering, and end-to-end GNS loopback AutonomousProxy mirror.

#include <AYNetwork.h>
#include <AYTest.h>

#include <AYNetwork/Prediction/InputRing.h>
#include <AYNetwork/Prediction/ClientInputCodec.h>
#include <AYNetwork/Prediction/PredictionManager.h>
#include <AYNetwork/Prediction/MispredictionResolver.h>
#include <AYNetwork/Replication/ReflectSerializer.h>

#include <AYReflect/IReflect.h>
#include <AYReflect/detail/ReflectImpl.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <thread>
#include <vector>

using namespace ayt::net;

namespace
{
constexpr std::chrono::milliseconds kTimeout{2000};

bool pumpUntil(const std::function<bool()>& pred,
               std::chrono::milliseconds timeout = kTimeout) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    return pred();
}

ClientInputRecord makeRec(uint32_t seq, const std::vector<uint8_t>& payload) {
    ClientInputRecord r;
    r.inputSeq = seq;
    r.serverTickAtSend = seq * 10u;
    r.payload = payload;
    return r;
}

} // anonymous namespace

TEST_SUITE(ClientInput)

// =============================================================================
// Case 1: InputRing push + tryGet lookup. No overflow.
// =============================================================================
TEST_CASE(InputRing_PushLookup)
{
    ayt::test::setCurrentCase("InputRing_PushLookup");
    InputRing ring(32);

    CHECK(ring.push(makeRec(1, {0xAA, 0xBB})));
    CHECK(ring.push(makeRec(2, {0xCC})));
    CHECK(ring.push(makeRec(3, {0xDD, 0xEE, 0xFF})));

    CHECK_INT_EQ(static_cast<size_t>(ring.size()), static_cast<size_t>(3));
    CHECK_INT_EQ(static_cast<uint32_t>(ring.newestSeq()), static_cast<uint32_t>(3));
    CHECK_INT_EQ(static_cast<uint32_t>(ring.oldestLiveSeq()), static_cast<uint32_t>(1));

    ClientInputRecord r;
    CHECK(ring.tryGet(1, r));
    CHECK_INT_EQ(static_cast<uint32_t>(r.inputSeq), static_cast<uint32_t>(1));
    CHECK_INT_EQ(static_cast<size_t>(r.payload.size()), static_cast<size_t>(2));
    CHECK_INT_EQ(static_cast<int>(r.payload[0]), static_cast<int>(0xAA));
    CHECK_INT_EQ(static_cast<int>(r.payload[1]), static_cast<int>(0xBB));

    CHECK(ring.tryGet(3, r));
    CHECK_INT_EQ(static_cast<size_t>(r.payload.size()), static_cast<size_t>(3));

    CHECK(!ring.tryGet(0, r));     // out of range low
    CHECK(!ring.tryGet(99, r));    // out of range high
}

// =============================================================================
// Case 2: InputRing drop-oldest overflow.
// =============================================================================
TEST_CASE(InputRing_DropOldestOverflow)
{
    ayt::test::setCurrentCase("InputRing_DropOldestOverflow");
    InputRing ring(32);

    for (uint32_t s = 1; s <= 33; ++s) {
        CHECK(ring.push(makeRec(s, {static_cast<uint8_t>(s & 0xFFu)})));
    }
    CHECK_INT_EQ(static_cast<size_t>(ring.size()), static_cast<size_t>(32));
    CHECK_INT_EQ(static_cast<uint32_t>(ring.oldestLiveSeq()), static_cast<uint32_t>(2)); // seq 1 dropped
    CHECK_INT_EQ(static_cast<uint32_t>(ring.newestSeq()), static_cast<uint32_t>(33));

    ClientInputRecord r;
    CHECK(!ring.tryGet(1, r));
    CHECK(ring.tryGet(2, r));
    CHECK(ring.tryGet(33, r));
}

// =============================================================================
// Case 3: InputRing ackUpTo forward-only clamp.
// =============================================================================
TEST_CASE(InputRing_AckUpToClamp)
{
    ayt::test::setCurrentCase("InputRing_AckUpToClamp");
    InputRing ring(8);
    for (uint32_t s = 1; s <= 5; ++s) ring.push(makeRec(s, {}));

    CHECK_INT_EQ(static_cast<uint32_t>(ring.ackedSeq()), static_cast<uint32_t>(0));
    ring.ackUpTo(3);
    CHECK_INT_EQ(static_cast<uint32_t>(ring.ackedSeq()), static_cast<uint32_t>(4));
    // Future-ack attempt must not regress.
    ring.ackUpTo(1);
    CHECK_INT_EQ(static_cast<uint32_t>(ring.ackedSeq()), static_cast<uint32_t>(4));
    ring.ackUpTo(5);
    CHECK_INT_EQ(static_cast<uint32_t>(ring.ackedSeq()), static_cast<uint32_t>(6));
}

// =============================================================================
// Case 4: ClientInputCodec round-trip.
// =============================================================================
TEST_CASE(ClientInputCodec_RoundTrip)
{
    ayt::test::setCurrentCase("ClientInputCodec_RoundTrip");
    const uint32_t inSeq = 42;
    const uint32_t inTick = 0xDEADBEEFu;
    const std::vector<uint8_t> payload = {0x10, 0x20, 0x30, 0x40};

    std::vector<uint8_t> buf(8 + payload.size());
    const size_t written = ClientInputCodec::write(buf.data(),
        inSeq, inTick, payload.data(), payload.size());
    CHECK_INT_EQ(static_cast<size_t>(written), static_cast<size_t>(8 + payload.size()));

    uint32_t seqOut = 0, tickOut = 0;
    const uint8_t* payloadOut = nullptr;
    size_t payloadSizeOut = 0;
    CHECK(ClientInputCodec::read(buf.data(), buf.size(),
                                 seqOut, tickOut, payloadOut, payloadSizeOut));
    CHECK_INT_EQ(static_cast<uint32_t>(seqOut), static_cast<uint32_t>(inSeq));
    CHECK_INT_EQ(static_cast<uint32_t>(tickOut), static_cast<uint32_t>(inTick));
    CHECK_INT_EQ(static_cast<size_t>(payloadSizeOut), payload.size());
    CHECK(std::memcmp(payloadOut, payload.data(), payload.size()) == 0);

    // Truncated body → false.
    CHECK(!ClientInputCodec::read(buf.data(), 4u, seqOut, tickOut,
                                  payloadOut, payloadSizeOut));
}

// =============================================================================
// Case 5: AckTail piggy-back round-trip.
// =============================================================================
TEST_CASE(AckTail_RoundTrip)
{
    ayt::test::setCurrentCase("AckTail_RoundTrip");
    // present=true writes 8 bytes.
    {
        BitStream s;
        ReflectSerializer::AckTail tail;
        tail.lastAckedInputTick = 42;
        tail.serverCommandAge = 7;
        tail.present = true;
        ReflectSerializer::writeAckTail(s, tail);
        CHECK_INT_EQ(static_cast<size_t>(s.getSize()), static_cast<size_t>(8));

        // Construct a separate read-mode stream over the written bytes.
        const size_t n = s.getSize();
        std::vector<uint8_t> buf(n);
        std::memcpy(buf.data(), s.getData(), n);
        BitStream rd(buf.data(), buf.size());

        ReflectSerializer::AckTail r;
        r.present = true;
        CHECK(ReflectSerializer::readAckTail(rd, r));
        CHECK(r.present);
        CHECK_INT_EQ(static_cast<uint32_t>(r.lastAckedInputTick), static_cast<uint32_t>(42));
        CHECK_INT_EQ(static_cast<uint32_t>(r.serverCommandAge),   static_cast<uint32_t>(7));
    }
    // present=false writes nothing.
    {
        BitStream s;
        ReflectSerializer::AckTail tail; // present=false default
        ReflectSerializer::writeAckTail(s, tail);
        CHECK_INT_EQ(static_cast<size_t>(s.getSize()), static_cast<size_t>(0));

        // Reader on the legacy R5.0/R5.1 stream also marks tail.present=false
        // (the reader's own intent flag — "expect no tail bytes, skip"). The
        // reader short-circuits to true without touching bit position.
        const size_t n = s.getSize();
        std::vector<uint8_t> buf(n);
        BitStream rd(buf.data(), buf.size());

        ReflectSerializer::AckTail r;
        // r.present stays false (default).
        CHECK(ReflectSerializer::readAckTail(rd, r));
        CHECK(!r.present);
    }
}

// =============================================================================
// Case 6: Ack math wraparound across u32 boundary.
// =============================================================================
TEST_CASE(Ack_WraparoundMath)
{
    ayt::test::setCurrentCase("Ack_WraparoundMath");
    // Older seq is 0xFFFFFFF0; newer is 0x00000005 (5 ticks ahead modulo 2^32).
    const uint32_t older = 0xFFFFFFF0u;
    const uint32_t newer = 0x00000005u;
    CHECK(seqGreaterThan(newer, older));   // wraparound-aware newer > older
    CHECK(!seqGreaterThan(older, newer));  // older is NOT newer than 0x5

    // Same seq is not "greater".
    CHECK(!seqGreaterThan(5u, 5u));

    // A ring with newestSeq=0xFFFFFFFE and ackedSeq=0xFFFFFFFC should treat
    // a push at seq=0x00000001 as newer (it is).
    InputRing ring(8);
    CHECK(ring.push(makeRec(0xFFFFFFFEu, {})));
    CHECK(ring.push(makeRec(0x00000001u, {})));
    CHECK_INT_EQ(static_cast<uint32_t>(ring.newestSeq()),
                 static_cast<uint32_t>(0x00000001u));
}

TEST_CASE(AckTail_IsolatedByOwningConnection)
{
    ayt::test::setCurrentCase("AckTail_IsolatedByOwningConnection");
    ReplicationManager manager(nullptr);
    manager.setModeForTesting(ConnectionMode::Server);

    int32_t first = 1;
    int32_t second = 2;
    const auto* type = ayt::reflect::TypeRegistryImpl::instance()
                           .findType<int32_t>();
    CHECK(type != nullptr);
    manager.registerObject(&first, type, 101u);
    manager.registerObject(&second, type, 202u);
    manager.setObjectProxyKind(101u, ProxyKind::AutonomousProxy, 11u);
    manager.setObjectProxyKind(202u, ProxyKind::AutonomousProxy, 22u);

    const uint8_t payload = 0x5Au;
    std::vector<uint8_t> inputBody(9u);
    CHECK(ClientInputCodec::write(inputBody.data(), 1u, 10u,
                                  &payload, 1u) == inputBody.size());
    CHECK(manager.onClientInput(11u, inputBody.data(), inputBody.size()));
    CHECK(manager.onClientInput(22u, inputBody.data(), inputBody.size()));
    manager.consumeClientInputs(10u);

    CHECK(manager.buildAckTailForConnection(11u, 0u).present);
    CHECK(manager.buildAckTailForConnection(22u, 0u).present);
    CHECK(!manager.buildAckTailForConnection(33u, 0u).present);

    manager.unregisterObject(101u);
    CHECK(!manager.buildAckTailForConnection(11u, 0u).present);
    CHECK(manager.buildAckTailForConnection(22u, 0u).present);
}

TEST_CASE(ReplicationManager_AuthorityEpochResetDropsStaleOwnership)
{
    ayt::test::setCurrentCase(
        "ReplicationManager_AuthorityEpochResetDropsStaleOwnership");
    ReplicationManager manager(nullptr);
    manager.setModeForTesting(ConnectionMode::Server);
    int32_t value = 17;
    const auto* type = ayt::reflect::TypeRegistryImpl::instance()
                           .findType<int32_t>();
    CHECK(type != nullptr);
    manager.registerObject(&value, type, 301u);
    manager.setObjectProxyKind(
        301u, ProxyKind::AutonomousProxy, 44u);
    const uint8_t payload = 1;
    std::vector<uint8_t> inputBody(9u);
    CHECK(ClientInputCodec::write(
        inputBody.data(), 5u, 10u, &payload, 1u) == inputBody.size());
    CHECK(manager.onClientInput(44u, inputBody.data(), inputBody.size()));
    manager.consumeClientInputs(10u);
    CHECK(manager.buildAckTailForConnection(44u, 0u).present);

    manager.resetForAuthorityEpoch(true);

    CHECK(manager.findObject(301u) == &value);
    CHECK(manager.getObjectProxyKind(301u) == ProxyKind::SimulatedProxy);
    CHECK(!manager.buildAckTailForConnection(44u, 0u).present);
    CHECK_INT_EQ(manager.getLastAckedInputTick(44u), 0);
}

// =============================================================================
// Case 7: MispredictionResolver skips ServerAuthoritative fields.
// =============================================================================
TEST_CASE(Misprediction_ServerAuthoritativeSkip)
{
    ayt::test::setCurrentCase("Misprediction_ServerAuthoritativeSkip");

    // Build a small layout: one float (numericKind=1) NetReplicate,
    // and one float NetReplicate + ServerAuthoritative.
    ResolverLayout layout;
    {
        ResolverField f;
        f.name = "pos";
        f.byteOffset = 0;
        f.byteSize = 4;
        f.numericKind = 1; // float32
        f.netReplicate = true;
        f.serverAuthoritative = false;
        layout.fields.push_back(f);
    }
    {
        ResolverField f;
        f.name = "hp";
        f.byteOffset = 4;
        f.byteSize = 4;
        f.numericKind = 1;
        f.netReplicate = true;
        f.serverAuthoritative = true; // MUST be skipped
        layout.fields.push_back(f);
    }

    std::vector<uint8_t> predicted(8, 0);
    std::vector<uint8_t> server(8, 0);
    // pos differs.
    float pos_p = 1.0f, pos_s = 2.0f;
    std::memcpy(predicted.data() + 0, &pos_p, 4);
    std::memcpy(server.data()    + 0, &pos_s, 4);
    // hp differs but is server-authoritative.
    float hp_p = 100.f, hp_s = 99.f;
    std::memcpy(predicted.data() + 4, &hp_p, 4);
    std::memcpy(server.data()    + 4, &hp_s, 4);

    auto r = MispredictionResolver::reconcile(predicted, server, layout,
        /*predictedInputSeq=*/ 5, /*serverLastAckedInputTick=*/ 5,
        /*dtSec=*/ 0.f, /*smoothingDuration=*/ 0.1f);

    // pos snapped (above threshold); hp skipped.
    CHECK(r.snapped);
    CHECK_INT_EQ(static_cast<uint32_t>(r.fieldsScanned), static_cast<uint32_t>(1));
    CHECK_INT_EQ(static_cast<uint32_t>(r.fieldsSnapped), static_cast<uint32_t>(1));
    CHECK_INT_EQ(static_cast<uint32_t>(r.fieldsSkipped), static_cast<uint32_t>(1));
    // predicted's pos byte-for-byte equals server's pos.
    float posNow = 0.f, hpNow = 0.f;
    std::memcpy(&posNow, predicted.data() + 0, 4);
    std::memcpy(&hpNow, predicted.data() + 4, 4);
    CHECK(posNow == pos_s);
    // hp preserved (resolver skipped) — still 100.
    CHECK(hpNow == hp_p);
}

// =============================================================================
// Case 8: Misprediction snap vs smooth threshold.
// =============================================================================
TEST_CASE(Misprediction_SnapVsSmoothThreshold)
{
    ayt::test::setCurrentCase("Misprediction_SnapVsSmoothThreshold");

    ResolverLayout layout;
    ResolverField f;
    f.name = "v";
    f.byteOffset = 0;
    f.byteSize = 4;
    f.numericKind = 1; // float32
    f.netReplicate = true;
    layout.fields.push_back(f);

    // Sub-threshold delta → smooth pass leaves value NOT snapped; the
    // output's value is between predicted and server when smoothingDuration
    // is small but finite (alpha=dt/dur).
    {
        std::vector<uint8_t> predicted(4, 0);
        std::vector<uint8_t> server(4, 0);
        float a = 1.0f, b = 1.0f + 1e-6f; // tiny delta < 1e-4 rel
        std::memcpy(predicted.data(), &a, 4);
        std::memcpy(server.data(),    &b, 4);

        auto r = MispredictionResolver::reconcile(predicted, server, layout,
            /*predictedInputSeq=*/ 5, /*serverLastAckedInputTick=*/ 5,
            /*dtSec=*/ 0.f, /*smoothingDuration=*/ 0.1f);
        CHECK(!r.snapped);   // nothing above threshold
        CHECK_INT_EQ(static_cast<uint32_t>(r.fieldsSnapped), static_cast<uint32_t>(0));
    }
    // Above-threshold delta → snap; result is byte-equal to server.
    {
        std::vector<uint8_t> predicted(4, 0);
        std::vector<uint8_t> server(4, 0);
        float a = 1.0f, b = 5.0f;
        std::memcpy(predicted.data(), &a, 4);
        std::memcpy(server.data(),    &b, 4);
        auto r = MispredictionResolver::reconcile(predicted, server, layout,
            5, 5, 0.f, 0.1f);
        CHECK(r.snapped);
        float out = 0.f;
        std::memcpy(&out, predicted.data(), 4);
        CHECK(out == b);
    }
}

// =============================================================================
// Case 9: consumeClientInputs invokes callback in seq order exactly once.
// =============================================================================
TEST_CASE(ConsumeClientInputs_Ordering)
{
    ayt::test::setCurrentCase("ConsumeClientInputs_Ordering");

    PredictionManager pm(8);
    std::vector<uint8_t> body1(8, 0);
    ClientInputCodec::write(body1.data(), 1, 10, nullptr, 0);
    pm.onClientInput(/*conn=*/ 100, body1.data(), body1.size());

    std::vector<uint8_t> body2(8, 0);
    ClientInputCodec::write(body2.data(), 2, 20, nullptr, 0);
    pm.onClientInput(100, body2.data(), body2.size());

    std::vector<uint8_t> body3(8, 0);
    ClientInputCodec::write(body3.data(), 1, 30, nullptr, 0);
    pm.onClientInput(/*conn=*/ 200, body3.data(), body3.size());

    std::vector<uint32_t> calls;
    pm.consumeClientInputs(/*simTick=*/ 7, [&](uint32_t conn, uint32_t seq,
                                               const uint8_t*, size_t) {
        calls.push_back(conn * 1000u + seq);
    });
    // Expected: conn 100 seq1, conn 100 seq2, conn 200 seq1.
    CHECK_INT_EQ(static_cast<size_t>(calls.size()), static_cast<size_t>(3));
    CHECK_INT_EQ(static_cast<uint32_t>(calls[0]), static_cast<uint32_t>(100001));
    CHECK_INT_EQ(static_cast<uint32_t>(calls[1]), static_cast<uint32_t>(100002));
    CHECK_INT_EQ(static_cast<uint32_t>(calls[2]), static_cast<uint32_t>(200001));

    // After consumption, rings should be empty.
    CHECK_INT_EQ(static_cast<size_t>(pm.pendingInputCount(100)), static_cast<size_t>(0));
    CHECK_INT_EQ(static_cast<size_t>(pm.pendingInputCount(200)), static_cast<size_t>(0));
}

// =============================================================================
// Case 10: End-to-end PredictionManager smoke (no GNS).
// Verifies: PredictionManager persists inputs, marks acked, exposes ack state.
// The full GNS loopback path lives in AYTest_AckPipeline.cpp (case 5) and
// is reused by R5.2 — this case locks the manager-level invariants.
// =============================================================================
TEST_CASE(PredictionManager_EndToEnd)
{
    ayt::test::setCurrentCase("PredictionManager_EndToEnd");
    PredictionManager pm(8);
    CHECK_INT_EQ(static_cast<size_t>(pm.trackedConnections()), static_cast<size_t>(0));

    // Push 3 inputs on conn 7.
    for (uint32_t s = 1; s <= 3; ++s) {
        std::vector<uint8_t> body(8, 0);
        ClientInputCodec::write(body.data(), s, s * 100u, nullptr, 0);
        CHECK(pm.onClientInput(7, body.data(), body.size()));
    }
    CHECK_INT_EQ(static_cast<size_t>(pm.pendingInputCount(7)), static_cast<size_t>(3));

    // Ack seq 2 → next-not-yet-acked = 3.
    pm.markAcked(7, /*lastAckedInputTick=*/ 2);
    CHECK_INT_EQ(static_cast<uint32_t>(pm.lastAckedInputTick(7)), static_cast<uint32_t>(2));

    // Ghost registry round-trip.
    pm.registerPredictedGhost(/*netId=*/ 17, /*layoutHash=*/ 0xCAFE);
    CHECK(pm.isPredictedGhost(17));
    CHECK_INT_EQ(static_cast<uint64_t>(pm.getLayoutHash(17)),
                 static_cast<uint64_t>(0xCAFE));
    const uint8_t bytes[] = {0x11, 0x22, 0x33, 0x44};
    pm.setPredictedBytes(17, bytes, 4);
    std::vector<uint8_t> out;
    CHECK(pm.tryGetPredictedBytes(17, out));
    CHECK_INT_EQ(static_cast<size_t>(out.size()), static_cast<size_t>(4));
    CHECK_INT_EQ(static_cast<int>(out[0]), static_cast<int>(0x11));

    // Server ack for netId updates lastAckedInputTick.
    pm.onServerAck(17, /*lastAckedInputTick=*/ 9, /*serverCommandAge=*/ 5);
    CHECK_INT_EQ(static_cast<uint32_t>(pm.lastAckedInputTick(7)), static_cast<uint32_t>(2));
    // Ghost-side: last-acked lives on the ghost struct, not the per-conn map.
    // Verify via internal-state isAcked through onServerAck round trip: we
    // re-fetch by calling the resolver with a fake server ack.
    // Here we just confirm the call did not throw and the ghost is still tracked.
    CHECK(pm.isPredictedGhost(17));
}

TEST_CASE(PredictionManager_ConfiguredRingCapacity)
{
    ayt::test::setCurrentCase("PredictionManager_ConfiguredRingCapacity");
    PredictionManager pm(2);
    for (uint32_t seq = 1; seq <= 3; ++seq) {
        std::vector<uint8_t> body(8, 0);
        ClientInputCodec::write(body.data(), seq, 0, nullptr, 0);
        CHECK(pm.onClientInput(77, body.data(), body.size()));
    }
    CHECK(pm.pendingInputCount(77) == 2u);
}

TEST_CASE(PredictionManager_AuthorityEpochResetPreservesGhostState)
{
    ayt::test::setCurrentCase(
        "PredictionManager_AuthorityEpochResetPreservesGhostState");
    PredictionManager pm(8);
    std::vector<uint8_t> input(8, 0);
    ClientInputCodec::write(input.data(), 9, 100, nullptr, 0);
    CHECK(pm.onClientInput(77, input.data(), input.size()));
    pm.markAcked(77, 8);
    pm.registerPredictedGhost(42, 0xCAFE);
    const uint8_t predicted[] = {4, 2, 1};
    pm.setPredictedBytes(42, predicted, sizeof(predicted));

    pm.resetForAuthorityEpoch();

    CHECK_INT_EQ(pm.trackedConnections(), 0);
    CHECK_INT_EQ(pm.pendingInputCount(77), 0);
    CHECK_INT_EQ(pm.lastAckedInputTick(77), 0);
    CHECK(pm.isPredictedGhost(42));
    CHECK_INT_EQ(pm.getLayoutHash(42), 0xCAFE);
    std::vector<uint8_t> retained;
    CHECK(pm.tryGetPredictedBytes(42, retained));
    CHECK(retained == std::vector<uint8_t>({4, 2, 1}));
}

TEST_CASE(Misprediction_NumericLerpUsesValues)
{
    ayt::test::setCurrentCase("Misprediction_NumericLerpUsesValues");
    ResolverLayout layout;
    layout.fields.push_back({"v", 0, 4, 1, true, false});
    std::vector<uint8_t> predicted(4), server(4);
    float a = 0.0f, b = 1.0f;
    std::memcpy(predicted.data(), &a, 4);
    std::memcpy(server.data(), &b, 4);
    auto r = MispredictionResolver::reconcile(
        predicted, server, layout,
        /*predictedInputSeq=*/ 6, /*serverLastAckedInputTick=*/ 5,
        /*dtSec=*/ 0.05f, /*smoothingDuration=*/ 0.1f);
    float out = 0.0f;
    std::memcpy(&out, predicted.data(), 4);
    CHECK(!r.snapped);
    CHECK(out > 0.49f && out < 0.51f);
    CHECK(MispredictionResolver::aboveThreshold(
        reinterpret_cast<const uint8_t*>(&b),
        reinterpret_cast<const uint8_t*>(&a), 4, 1));
}

// R6 C7 B-12 (2026-08-25): numericEqualsEpsilon now compares |bit-pattern diff|
// in ULPs (≤64) instead of the float relative-epsilon formula. Verify the
// sub-64-ULP case still considers values equal, and the >64-ULP case snaps.
TEST_CASE(Misprediction_UlpThreshold_Boundary) {
    ayt::test::setCurrentCase("Misprediction_UlpThreshold_Boundary");

    ResolverLayout layout;
    ResolverField f;
    f.name = "v";
    f.byteOffset = 0;
    f.byteSize = 4;
    f.numericKind = 1; // float32
    f.netReplicate = true;
    layout.fields.push_back(f);

    // 32 ULPs apart — must be considered equal (under threshold).
    {
        std::vector<uint8_t> predicted(4, 0);
        std::vector<uint8_t> server(4, 0);
        float a = 1.0f;
        uint32_t ua = 0, ub = 0;
        std::memcpy(&ua, &a, 4);
        ub = ua + 32u;        // 32 ULPs above
        std::memcpy(&a, &ua, 4);
        float b = 0.f;
        std::memcpy(&b, &ub, 4);
        std::memcpy(predicted.data(), &a, 4);
        std::memcpy(server.data(),    &b, 4);

        auto r = MispredictionResolver::reconcile(predicted, server, layout,
            5, 5, 0.f, 0.1f);
        CHECK(!r.snapped);
        CHECK_INT_EQ(static_cast<uint32_t>(r.fieldsSnapped), static_cast<uint32_t>(0));
    }
    // 256 ULPs apart — must snap.
    {
        std::vector<uint8_t> predicted(4, 0);
        std::vector<uint8_t> server(4, 0);
        float a = 1.0f;
        uint32_t ua = 0, ub = 0;
        std::memcpy(&ua, &a, 4);
        ub = ua + 256u;
        std::memcpy(&a, &ua, 4);
        float b = 0.f;
        std::memcpy(&b, &ub, 4);
        std::memcpy(predicted.data(), &a, 4);
        std::memcpy(server.data(),    &b, 4);

        auto r = MispredictionResolver::reconcile(predicted, server, layout,
            5, 5, 0.f, 0.1f);
        CHECK(r.snapped);
    }
}

TEST_SUITE_END
