// AYTest_SnapshotInterpolation.cpp - R5.0 Snapshot Interpolation tests.
//
// Coverage (design §15.7):
//   1. NetworkTimeAdvance               — serverTick advances at rate
//   2. NetworkTimeInterpolationDelay    — interpolationTime = clientTime - delay
//   3. NetworkTimeTickToSeconds         — tick ↔ seconds conversion
//   4. SnapshotBufferPushExact          — push + sample at exact record time
//   5. SnapshotBufferLerpMidpoint       — bracket indices + alpha for midpoint
//   6. SnapshotBufferWarmupBeforeFirst  — sample before first → (0,0,0)
//   7. SnapshotBufferPastNewestHolds    — sample past newest → newest copy
//   8. SnapshotBufferRingOverflow       — capacity evicts oldest
//   9. SnapshotBufferOutOfOrderPush     — older pushes insert, no eviction
//  10. SnapshotInterpolatorEndToEnd     — full pipeline with reflected ghost
//  11. SnapshotInterpolatorUnregister   — unregisterGhost drops the buffer
//  12. SnapshotInterpolatorMixed        — mixed lerp/snap falls back to snap-to-lower
//  13. ReplicationManagerTickStamps     — tick() emits u32 serverTick prefix
//  14. WireCompatibilityZeroTick        — serverTick=0 keeps R3.x layout (no prefix)
//  15. WireWithServerTickPrefixRoundTrip — explicit tick round-trips through the body

#include <AYNetwork.h>
#include <AYTest.h>
#include <AYNetwork/INetwork.h>
#include <AYNetwork/Protocol/PacketCodec.h>
#include <AYNetwork/Replication/ReflectSerializer.h>
#include <AYNetwork/Replication/ReplicationManager.h>
#include <AYNetwork/Snapshot/NetworkTime.h>
#include <AYNetwork/Snapshot/SnapshotBuffer.h>
#include <AYNetwork/Snapshot/SnapshotInterpolator.h>

#include <AYReflect/IReflect.h>
#include <AYReflect/ReflectMacros.h>
#include <AYReflect/detail/ReflectImpl.h>
#include <AYReflect.h>

#include <array>
#include <cstdint>
#include <cstring>
#include <functional>
#include <string>
#include <vector>

using namespace ayt::net;
using ayt::reflect::FieldAttribute;

namespace {

// =============================================================================
// Fixtures — flat-POD ghosts with explicit NetReplicate fields. Same registrar
// pattern as AYTest_Replication.cpp so the reflection registry has these types
// at static-init time, before any TEST_CASE runs.
// =============================================================================
struct GhostPosition {
    float   x = 0.f;
    float   y = 0.f;
    float   z = 0.f;
};

struct GhostStateMachine {
    int32_t state   = 0;     // enum-like; integer → snap-to-lower
    float   progress = 0.f;  // lerpable
};

// Static-init registrar: mirrors ReplicationFixtureRegistrar pattern.
struct SnapshotFixtureRegistrar {
    SnapshotFixtureRegistrar() {
        using ayt::reflect::FieldInfoImpl;
        using ayt::reflect::TypeInfoImpl;
        using ayt::reflect::TypeRegistryImpl;
        using ayt::reflect::detail::defaultCreate;
        using ayt::reflect::detail::defaultDestroy;
        using ayt::reflect::detail::defaultCopy;
        auto& reg = TypeRegistryImpl::instance();

        if (!reg.findType("GhostPosition")) {
            auto* info = new TypeInfoImpl<GhostPosition>(
                "GhostPosition",
                defaultCreate<GhostPosition>,
                defaultDestroy<GhostPosition>,
                defaultCopy<GhostPosition>);
            using T = GhostPosition;
            const auto NR = FieldAttribute::Serialize | FieldAttribute::NetReplicate;
            info->addField(new FieldInfoImpl("x",  reg.findType<float>(),   offsetof(T, x),  NR));
            info->addField(new FieldInfoImpl("y",  reg.findType<float>(),   offsetof(T, y),  NR));
            info->addField(new FieldInfoImpl("z",  reg.findType<float>(),   offsetof(T, z),  NR));
            reg.registerTypeInfo("GhostPosition", info);
        }

        if (!reg.findType("GhostStateMachine")) {
            auto* info = new TypeInfoImpl<GhostStateMachine>(
                "GhostStateMachine",
                defaultCreate<GhostStateMachine>,
                defaultDestroy<GhostStateMachine>,
                defaultCopy<GhostStateMachine>);
            using T = GhostStateMachine;
            const auto NR = FieldAttribute::Serialize | FieldAttribute::NetReplicate;
            info->addField(new FieldInfoImpl("state",    reg.findType<int32_t>(), offsetof(T, state),    NR));
            info->addField(new FieldInfoImpl("progress", reg.findType<float>(),   offsetof(T, progress), NR));
            reg.registerTypeInfo("GhostStateMachine", info);
        }
    }
};

static SnapshotFixtureRegistrar g_snapshotFixtureRegistrar;

} // anonymous namespace

// =============================================================================
// NetworkTime tests
// =============================================================================
TEST_SUITE(NetworkTimeSuite)

TEST_CASE(NetworkTimeAdvance) {
    ayt::test::setCurrentCase("NetworkTimeAdvance");
    NetworkTime nt;
    nt.setTickRate(30.0);
    CHECK_INT_EQ(static_cast<int>(nt.getServerTick()), 0);
    nt.advance(1.0);          // 1 second at 30 Hz → 30 ticks
    CHECK_INT_EQ(static_cast<int>(nt.getServerTick()), 30);
    nt.advance(0.5);          // half a second → 15 ticks
    CHECK_INT_EQ(static_cast<int>(nt.getServerTick()), 45);
}

TEST_CASE(NetworkTimeInterpolationDelay) {
    ayt::test::setCurrentCase("NetworkTimeInterpolationDelay");
    NetworkTime nt;
    nt.setTickRate(30.0);
    nt.setInterpolationDelaySec(0.1);   // 100 ms
    nt.advanceClient(1.0);               // 1 second of client time
    CHECK(nt.getClientTimeSec() == 1.0);
    CHECK(nt.getInterpolationTimeSec() == 0.9);
}

TEST_CASE(NetworkTimeTickToSeconds) {
    ayt::test::setCurrentCase("NetworkTimeTickToSeconds");
    NetworkTime nt;
    nt.setTickRate(30.0);
    CHECK(nt.tickToSeconds(30) == 1.0);
    CHECK(nt.tickToSeconds(60) == 2.0);
    CHECK(nt.tickToSeconds(0)  == 0.0);
}
TEST_SUITE_END

// =============================================================================
// SnapshotBuffer tests
// =============================================================================
TEST_SUITE(SnapshotBufferSuite)

TEST_CASE(SnapshotBufferPushExact) {
    ayt::test::setCurrentCase("SnapshotBufferPushExact");
    SnapshotBuffer buf;
    buf.init(sizeof(GhostPosition));
    GhostPosition p;
    p.x = 1.0f; p.y = 2.0f; p.z = 3.0f;
    buf.push(/*tick=*/10, /*time=*/10.0/30.0, &p);

    GhostPosition out{};
    const double t10 = 10.0 / 30.0;
    CHECK(buf.sample(t10, &out));
    CHECK(out.x == 1.0f);
    CHECK_INT_EQ(buf.size(), 1u);
}

TEST_CASE(SnapshotBufferLerpMidpoint) {
    ayt::test::setCurrentCase("SnapshotBufferLerpMidpoint");
    SnapshotBuffer buf;
    buf.init(sizeof(GhostPosition));

    GhostPosition a; a.x = 0.0f;  buf.push(/*tick=*/0, 0.0, &a);
    GhostPosition b; b.x = 10.0f; buf.push(/*tick=*/10, 10.0/30.0, &b);

    size_t lo, hi; double alpha = 0.0;
    CHECK(buf.findBracket(5.0/30.0, lo, hi, alpha));
    CHECK(lo == 0);
    CHECK(hi == 1);
    // alpha = (5/30 - 0) / (10/30 - 0) = 0.5
    CHECK(alpha == 0.5);

    GhostPosition aBytes{}, bBytes{};
    CHECK(buf.readRecord(lo, &aBytes));
    CHECK(buf.readRecord(hi, &bBytes));
    CHECK(aBytes.x == 0.0f);
    CHECK(bBytes.x == 10.0f);
}

TEST_CASE(SnapshotBufferWarmupBeforeFirst) {
    ayt::test::setCurrentCase("SnapshotBufferWarmupBeforeFirst");
    SnapshotBuffer buf;
    buf.init(sizeof(GhostPosition));
    GhostPosition p; p.x = 1.0f;
    buf.push(/*tick=*/5, 5.0/30.0, &p);

    GhostPosition out{};
    // Sample at time 0 — before the first record. findBracket returns
    // (0, 0, 0.0) and readRecord(0, out) succeeds (we hold the oldest).
    size_t lo, hi; double alpha = 0.0;
    CHECK(buf.findBracket(0.0, lo, hi, alpha));
    CHECK_INT_EQ(lo, 0u);
    CHECK(alpha == 0.0);
    CHECK(buf.readRecord(lo, &out));
    CHECK(out.x == 1.0f);
}

TEST_CASE(SnapshotBufferPastNewestHolds) {
    ayt::test::setCurrentCase("SnapshotBufferPastNewestHolds");
    SnapshotBuffer buf;
    buf.init(sizeof(GhostPosition));
    GhostPosition p; p.x = 7.5f;
    buf.push(/*tick=*/10, 10.0/30.0, &p);

    // Past the newest → (size-1, kNoUpperBracket, 0.0).
    size_t lo, hi; double alpha = 99.0;
    CHECK(buf.findBracket(99.0, lo, hi, alpha));
    CHECK(lo == 0);
    CHECK(hi == SnapshotBuffer::kNoUpperBracket);
    CHECK(alpha == 0.0);

    GhostPosition out{};
    CHECK(buf.readRecord(lo, &out));
    CHECK(out.x == 7.5f);
}

TEST_CASE(SnapshotBufferRingOverflow) {
    ayt::test::setCurrentCase("SnapshotBufferRingOverflow");
    SnapshotBuffer buf;
    buf.init(sizeof(GhostPosition), /*capacity=*/4);

    // Push 6 records into a capacity-4 buffer; expect to keep the last 4.
    for (uint32_t t = 0; t < 6; ++t) {
        GhostPosition p; p.x = static_cast<float>(t);
        buf.push(t, t / 30.0, &p);
    }
    CHECK_INT_EQ(buf.size(), 4u);
    CHECK_INT_EQ(static_cast<int>(buf.oldestTick()), 2);   // 0,1 evicted
    CHECK_INT_EQ(static_cast<int>(buf.newestTick()), 5);
}

TEST_CASE(SnapshotBufferOutOfOrderPush) {
    ayt::test::setCurrentCase("SnapshotBufferOutOfOrderPush");
    SnapshotBuffer buf;
    buf.init(sizeof(GhostPosition));

    GhostPosition a; a.x = 1.0f; buf.push(/*tick=*/10, 10.0/30.0, &a);
    GhostPosition c; c.x = 3.0f; buf.push(/*tick=*/30, 30.0/30.0, &c);

    // Older push → inserts between a and c, no eviction of c.
    GhostPosition b; b.x = 2.0f; buf.push(/*tick=*/20, 20.0/30.0, &b);
    CHECK_INT_EQ(buf.size(), 3u);
    CHECK_INT_EQ(static_cast<int>(buf.tickAt(0)), 10);
    CHECK_INT_EQ(static_cast<int>(buf.tickAt(1)), 20);
    CHECK_INT_EQ(static_cast<int>(buf.tickAt(2)), 30);
    CHECK(buf.timeAt(0) == 10.0/30.0);
}

// R5.1: push(..., snap=true) marks the record; isSnap() reports it.
TEST_CASE(SnapshotBufferSnapRecord) {
    ayt::test::setCurrentCase("SnapshotBufferSnapRecord");
    SnapshotBuffer buf;
    buf.init(sizeof(GhostPosition));

    GhostPosition a; buf.push(/*tick=*/0, 0.0, &a, /*snap=*/false);
    GhostPosition b; buf.push(/*tick=*/10, 10.0/30.0, &b, /*snap=*/true);
    GhostPosition c; buf.push(/*tick=*/20, 20.0/30.0, &c, /*snap=*/false);

    CHECK(!buf.isSnap(0));
    CHECK(buf.isSnap(1));
    CHECK(!buf.isSnap(2));
    CHECK(!buf.isSnap(SnapshotBuffer::kNoUpperBracket));
    CHECK(!buf.isSnap(999));
}
TEST_SUITE_END

// =============================================================================
// SnapshotInterpolator E2E
// =============================================================================
TEST_SUITE(SnapshotInterpolatorSuite)

TEST_CASE(SnapshotInterpolatorEndToEnd) {
    ayt::test::setCurrentCase("SnapshotInterpolatorEndToEnd");
    const auto* type = ayt::reflect::TypeRegistryImpl::instance().findType<GhostPosition>();
    CHECK(type != nullptr);

    SnapshotInterpolator interp;
    interp.setTickRate(30.0);
    interp.setInterpolationDelaySec(0.0); // warmup-friendly for tests
    CHECK(interp.registerGhostKind(/*netId=*/1, sizeof(GhostPosition), type));

    // Tick 0..3: positions stepping by +1 each tick.
    for (uint32_t t = 0; t <= 3; ++t) {
        GhostPosition p;
        p.x = static_cast<float>(t);
        interp.push(1, t, &p);
    }

    GhostPosition out{};
    // Sample exactly at tick 1.0 → equals the tick-1 record (alpha=1.0
    // between records 0 and 1 picks the upper bracket).
    CHECK(interp.sample(1, /*renderTime=*/1.0/30.0, &out));
    CHECK(out.x == 1.0f);

    // Midpoint between tick 0 and tick 1 → 0.5 (multi-field lerp fast
    // path lerps every float in the struct).
    CHECK(interp.sample(1, 0.5/30.0, &out));
    CHECK(out.x == 0.5f);
}

TEST_CASE(SnapshotInterpolatorUnregister) {
    ayt::test::setCurrentCase("SnapshotInterpolatorUnregister");
    const auto* type = ayt::reflect::TypeRegistryImpl::instance().findType<GhostPosition>();
    CHECK(type != nullptr);

    SnapshotInterpolator interp;
    interp.registerGhostKind(42, sizeof(GhostPosition), type);
    CHECK_INT_EQ(interp.ghostCount(), 1u);

    GhostPosition p; p.x = 9.0f;
    interp.push(42, 0, &p);

    interp.unregisterGhost(42);
    CHECK_INT_EQ(interp.ghostCount(), 0u);

    GhostPosition out{};
    CHECK(!interp.sample(42, 0.0, &out));
}

TEST_CASE(SnapshotInterpolatorMixedSnapToLower) {
    ayt::test::setCurrentCase("SnapshotInterpolatorMixedSnapToLower");
    // GhostStateMachine: int32_t state + float progress. When the field
    // set is MIXED (lerpable + non-lerpable), the implementation falls
    // back to bytewise snap-to-lower (Unreal/Unity convention) — integer
    // transitions never blend mid-tick. We verify state stays at the
    // lower bracket across the bracket pair.
    const auto* type = ayt::reflect::TypeRegistryImpl::instance().findType<GhostStateMachine>();
    CHECK(type != nullptr);

    SnapshotInterpolator interp;
    interp.setTickRate(30.0);
    interp.setInterpolationDelaySec(0.0);
    interp.registerGhostKind(/*netId=*/7, sizeof(GhostStateMachine), type);

    GhostStateMachine a; a.state = 1; a.progress = 0.0f;
    GhostStateMachine b; b.state = 2; b.progress = 1.0f;  // state transitions!
    interp.push(7, 0, &a);
    interp.push(7, 10, &b);

    GhostStateMachine out{};
    // Sample at midpoint. The mixed-field fallback should pick the
    // lower bracket for both fields → state=1, progress=0.0.
    CHECK(interp.sample(7, 5.0/30.0, &out));
    CHECK_INT_EQ(out.state, 1);
    CHECK(out.progress == 0.0f);
}

// R5.1: a teleport snapshot in the buffer forces sample() to snap to the
// upper bracket's bytes — no lerp sweep across the world.
TEST_CASE(SnapshotInterpolatorTeleportSnap) {
    ayt::test::setCurrentCase("SnapshotInterpolatorTeleportSnap");
    const auto* type = ayt::reflect::TypeRegistryImpl::instance().findType<GhostPosition>();
    CHECK(type != nullptr);

    SnapshotInterpolator interp;
    interp.setTickRate(30.0);
    interp.setInterpolationDelaySec(0.0);
    interp.registerGhostKind(/*netId=*/11, sizeof(GhostPosition), type);

    // Steady motion at x=0,1,2; then a teleport to x=9999 at tick 3.
    for (uint32_t t = 0; t <= 2; ++t) {
        GhostPosition p; p.x = static_cast<float>(t);
        interp.push(11, t, &p, /*teleport=*/false);
    }
    GhostPosition jump; jump.x = 9999.0f;
    interp.push(11, /*tick=*/3, &jump, /*teleport=*/true);

    GhostPosition out{};
    // Sample between tick 2 and tick 3 — without the teleport flag, this
    // would lerp to roughly (2.0, 2.5, 3.0) for x. With the flag, the
    // upper record is snap and we hold tick-3's bytes verbatim: x=9999.
    CHECK(interp.sample(11, 2.5/30.0, &out));
    CHECK(out.x == 9999.0f);
}
TEST_SUITE_END

// =============================================================================
// ReplicationManager wire-stamping tests
// =============================================================================
TEST_SUITE(ReplicationManagerTickStampSuite)

TEST_CASE(ReplicationManagerTickStampsServerTick) {
    ayt::test::setCurrentCase("ReplicationManagerTickStampsServerTick");
    // Set up an authority-side manager with a broadcast sink so we can
    // inspect the sealed wire bytes without an INetworkSubSystem.
    const auto* type = ayt::reflect::TypeRegistryImpl::instance().findType<GhostPosition>();
    CHECK(type != nullptr);

    ReplicationManager mgr(nullptr);
    mgr.setModeForTesting(ConnectionMode::Server);
    mgr.setServerTickRate(30.0);

    GhostPosition obj;
    obj.x = 5.0f;
    mgr.registerObject(&obj, type, /*netId=*/7);

    std::vector<std::pair<uint8_t, std::vector<uint8_t>>> captured;
    mgr.setBroadcastSinkForTesting(
        [&](uint8_t channel, const void* data, size_t size) {
            std::vector<uint8_t> bytes(static_cast<const uint8_t*>(data),
                                       static_cast<const uint8_t*>(data) + size);
            captured.push_back({channel, std::move(bytes)});
        });

    // tick() always advances the server tick counter by exactly 1
    // (independent of deltaTime) so we can predict the stamp.
    mgr.tick(1.0f / 30.0f);
    const uint32_t expectedTick = mgr.getServerTick();
    CHECK(expectedTick >= 1u);

    // We expect at least two sealed frames: EntitySpawn + ReplicationFrame.
    // The Replication frame's body starts with the serverTick prefix;
    // the EntitySpawn frame's body starts with netId (no prefix). Walk
    // every captured frame's body[0..3] and confirm the Replication one
    // matches expectedTick.
    bool sawReplicationWithServerTick = false;
    for (auto& [channel, sealed] : captured) {
        if (sealed.size() < 12 + 4) continue;
        const uint8_t* body = sealed.data() + 12;   // skip PacketHeader
        uint32_t got = 0;
        std::memcpy(&got, body, 4);
        if (got == expectedTick) { sawReplicationWithServerTick = true; break; }
    }
    CHECK(sawReplicationWithServerTick);
}

// R5.1: markTeleported + tick emits a Full Snapshot with kFlagTeleport set
// in the FrameHeader's flags byte, then drains the marker.
TEST_CASE(ReplicationManagerTeleportEmitsFlag) {
    ayt::test::setCurrentCase("ReplicationManagerTeleportEmitsFlag");
    const auto* type = ayt::reflect::TypeRegistryImpl::instance().findType<GhostPosition>();
    CHECK(type != nullptr);

    ReplicationManager mgr(nullptr);
    mgr.setModeForTesting(ConnectionMode::Server);
    mgr.setServerTickRate(30.0);

    GhostPosition obj; obj.x = 1.0f;
    mgr.registerObject(&obj, type, /*netId=*/7);
    mgr.markTeleported(7);
    CHECK(mgr.isTeleportPending(7));

    std::vector<std::pair<uint8_t, std::vector<uint8_t>>> captured;
    mgr.setBroadcastSinkForTesting(
        [&](uint8_t channel, const void* data, size_t size) {
            std::vector<uint8_t> bytes(static_cast<const uint8_t*>(data),
                                       static_cast<const uint8_t*>(data) + size);
            captured.push_back({channel, std::move(bytes)});
        });

    mgr.tick(1.0f / 30.0f);

    // After emission, the marker should be drained.
    CHECK(!mgr.isTeleportPending(7));

    // Find the Replication frame in the captured stream. Body layout
    //   [u32 serverTick][u32 netId][u64 schemaHash][u8 fieldCount][u8 flags][records...]
    // The flag byte is at body offset 4 + 4 + 8 + 1 = 17. The PacketHeader
    // is 12 B at the front, so the absolute byte index is 12 + 17 = 29.
    bool sawTeleportFlag = false;
    for (auto& [channel, sealed] : captured) {
        if (sealed.size() < 12 + 18) continue;
        const uint8_t* body = sealed.data() + 12;
        // First 4 bytes = serverTick. Confirm this looks like a Replication
        // body (serverTick != 0 and fieldCount > 0) before peeking flags.
        uint32_t tick = 0; std::memcpy(&tick, body, 4);
        if (tick == 0) continue;  // spawn frame, not replication
        // netId at body[4..7]
        uint32_t netId = 0; std::memcpy(&netId, body + 4, 4);
        if (netId != 7) continue;
        const uint8_t flags = body[17];
        if (flags & kFlagTeleport) { sawTeleportFlag = true; break; }
    }
    CHECK(sawTeleportFlag);
}

TEST_CASE(WireCompatibilityZeroTickKeepsLegacyLayout) {
    ayt::test::setCurrentCase("WireCompatibilityZeroTickKeepsLegacyLayout");
    // R3.x-style serializer calls (default serverTick=0) must NOT emit
    // the prefix — keeps all pre-R5.0 round-trip tests passing unchanged.
    const auto* type = ayt::reflect::TypeRegistryImpl::instance().findType<GhostPosition>();
    CHECK(type != nullptr);

    GhostPosition src;
    src.x = 1.0f; src.y = 2.0f; src.z = 3.0f;
    BitStream wire;
    CHECK(ReflectSerializer::serializeObject(type, &src, /*netId=*/1, wire));
    wire.resetForRead();

    // Reading the legacy header position (no prefix) must succeed.
    ReflectSerializer::FrameHeader hdr;
    CHECK(ReflectSerializer::readReplicationFrameHeader(wire, hdr));
    CHECK_INT_EQ(static_cast<uint32_t>(hdr.netId), 1u);

    // And the round-trip still works.
    GhostPosition dst{};
    CHECK(ReflectSerializer::deserializeObject(type, &dst, wire, hdr.fieldCount));
    CHECK(dst.x == 1.0f);
    CHECK(dst.y == 2.0f);
    CHECK(dst.z == 3.0f);
}

TEST_CASE(WireWithServerTickPrefixRoundTrip) {
    ayt::test::setCurrentCase("WireWithServerTickPrefixRoundTrip");
    const auto* type = ayt::reflect::TypeRegistryImpl::instance().findType<GhostPosition>();
    CHECK(type != nullptr);

    GhostPosition src; src.x = 3.5f; src.y = 4.5f; src.z = 5.5f;
    BitStream wire;
    CHECK(ReflectSerializer::serializeObject(type, &src, /*netId=*/99,
                                             wire, /*serverTick=*/4242));
    wire.resetForRead();

    uint32_t gotTick = 0;
    CHECK(ReflectSerializer::readServerTick(wire, gotTick));
    CHECK_INT_EQ(gotTick, 4242u);

    ReflectSerializer::FrameHeader hdr;
    CHECK(ReflectSerializer::readReplicationFrameHeader(wire, hdr));
    CHECK_INT_EQ(static_cast<uint32_t>(hdr.netId), 99u);
    CHECK(!hdr.isTeleport());   // default flags — no teleport

    GhostPosition dst{};
    CHECK(ReflectSerializer::deserializeObject(type, &dst, wire, hdr.fieldCount));
    CHECK(dst.x == 3.5f);
    CHECK(dst.y == 4.5f);
    CHECK(dst.z == 5.5f);
}

// R5.1: kFlagTeleport travels from serializeObject into FrameHeader.
TEST_CASE(WireTeleportFlagRoundTrip) {
    ayt::test::setCurrentCase("WireTeleportFlagRoundTrip");
    const auto* type = ayt::reflect::TypeRegistryImpl::instance().findType<GhostPosition>();
    CHECK(type != nullptr);

    GhostPosition src; src.x = 99.0f;
    BitStream wire;
    // Pass serverTick=0 (no prefix) so the FrameHeader starts at byte 0
    // and we can read it directly with readReplicationFrameHeader. The
    // teleport flag travels in the FrameHeader itself (R5.1).
    CHECK(ReflectSerializer::serializeObject(type, &src, /*netId=*/3,
                                             wire, /*serverTick=*/0,
                                             /*flags=*/kFlagTeleport));
    wire.resetForRead();

    ReflectSerializer::FrameHeader hdr;
    CHECK(ReflectSerializer::readReplicationFrameHeader(wire, hdr));
    CHECK(hdr.isTeleport());
    CHECK_INT_EQ(static_cast<int>(hdr.flags()), kFlagTeleport);
}
TEST_SUITE_END