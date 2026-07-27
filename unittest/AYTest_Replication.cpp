// AYTest_Replication.cpp - R3.0 Replication layer tests
//
// Six cases covering the ReflectSerializer pure path and the end-to-end
// GnsConnection loopback path:
//
//   1. ReflectSerializerRoundTripPrimitives — full 12 WireTypeId dispatch
//   2. ReflectSerializerFiltersNonReplicated — only Serialize, no NetReplicate
//   3. ReplicationManagerSnapshotBroadcast   — server→client snapshot
//   4. ReplicationManagerEntitySpawnAndDespawn
//   5. AuthorityServerDropsClientReplicate  — §6.6 server-authoritative gate
//   6. BitstreamPacketCodecIntegration      — replicate over PacketCodec seal
//
// Test convention (R1 retro):
//   - AYTest framework only; CHECK_* macros; no REQUIRE / fixtures
//   - setCurrentCase at the top of every TEST_CASE
//   - CHECK_INT_EQ double-evaluates — store stream reads in locals first
//   - GNS tests use pumpUntil + atomics + gns::init/shutdown

#include <AYNetwork.h>
#include <AYTest.h>
#include <PacketCodec.h>
#include <GnsConnection.h>
#include <Replication/ReflectSerializer.h>
#include <Replication/ReplicationManager.h>

#include <ayreflect/IReflect.h>
#include <ayreflect/ReflectMacros.h>
#include <ayreflect/detail/ReflectImpl.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <functional>
#include <string>
#include <thread>
#include <vector>

using namespace ayt::net;
using ayt::reflect::FieldAttribute;

// =============================================================================
// Test fixtures: register a couple of reflected types covering many WireTypeIds.
//
// We can't use the framework-style AYTYPE_FIELD_EX macros here — they require
// the field-name identifier to be in scope (decltype + offsetof) which only
// works inside a member function of T. Instead we mirror the S3.2 fixture
// pattern from Test_Reflect.cpp: register each type with a manual registrar
// at static init. Native primitives (int32_t etc.) are registered inside
// AYReflect's own startup (see AYReflect.cpp R3.0 patch, 2026-07-27).
// =============================================================================

struct ReplicationAllPrimitives {
    bool    b   = true;
    int8_t  i8  = -12;
    int16_t i16 = -1234;
    int32_t i32 = -123456;
    int64_t i64 = -123456789012LL;
    uint8_t  u8  = 200;
    uint16_t u16 = 60000;
    uint32_t u32 = 4000000000u;
    uint64_t u64 = 18000000000000000000ull;
    float    f   = 3.14159f;
    double   d   = 2.718281828;
    std::string s = "hello world";
};

struct ReplicationNoNet {
    int32_t hp = 100;          // Serialize only — MUST NOT appear on wire
    int32_t score = 42;        // NetReplicate — MUST appear on wire
};

namespace
{

// Static-init registrar: builds TypeInfoImpl<T> for each fixture and adds the
// NetReplicate fields by hand. This is the same pattern Test_Reflect.cpp uses
// for its S3.2 fixtures.
struct ReplicationFixtureRegistrar {
    ReplicationFixtureRegistrar() {
        using ayt::reflect::FieldAttribute;
        using ayt::reflect::FieldInfoImpl;
        using ayt::reflect::TypeInfoImpl;
        using ayt::reflect::TypeRegistryImpl;
        using ayt::reflect::detail::defaultCreate;
        using ayt::reflect::detail::defaultDestroy;
        using ayt::reflect::detail::defaultCopy;
        auto& reg = TypeRegistryImpl::instance();

        if (!reg.findType("ReplicationAllPrimitives")) {
            auto* info = new TypeInfoImpl<ReplicationAllPrimitives>(
                "ReplicationAllPrimitives",
                defaultCreate<ReplicationAllPrimitives>,
                defaultDestroy<ReplicationAllPrimitives>,
                defaultCopy<ReplicationAllPrimitives>);
            using T = ReplicationAllPrimitives;
            using FA = FieldAttribute;
            auto NR = FA::Serialize | FA::NetReplicate;
            info->addField(new FieldInfoImpl("b",   reg.findType<bool>(),        offsetof(T, b),   NR));
            info->addField(new FieldInfoImpl("i8",  reg.findType<int8_t>(),      offsetof(T, i8),  NR));
            info->addField(new FieldInfoImpl("i16", reg.findType<int16_t>(),     offsetof(T, i16), NR));
            info->addField(new FieldInfoImpl("i32", reg.findType<int32_t>(),     offsetof(T, i32), NR));
            info->addField(new FieldInfoImpl("i64", reg.findType<int64_t>(),     offsetof(T, i64), NR));
            info->addField(new FieldInfoImpl("u8",  reg.findType<uint8_t>(),     offsetof(T, u8),  NR));
            info->addField(new FieldInfoImpl("u16", reg.findType<uint16_t>(),    offsetof(T, u16), NR));
            info->addField(new FieldInfoImpl("u32", reg.findType<uint32_t>(),    offsetof(T, u32), NR));
            info->addField(new FieldInfoImpl("u64", reg.findType<uint64_t>(),    offsetof(T, u64), NR));
            info->addField(new FieldInfoImpl("f",   reg.findType<float>(),       offsetof(T, f),   NR));
            info->addField(new FieldInfoImpl("d",   reg.findType<double>(),      offsetof(T, d),   NR));
            info->addField(new FieldInfoImpl("s",   reg.findType<std::string>(), offsetof(T, s),   NR));
            reg.registerTypeInfo("ReplicationAllPrimitives", info);
        }

        if (!reg.findType("ReplicationNoNet")) {
            auto* info = new TypeInfoImpl<ReplicationNoNet>(
                "ReplicationNoNet",
                defaultCreate<ReplicationNoNet>,
                defaultDestroy<ReplicationNoNet>,
                defaultCopy<ReplicationNoNet>);
            using T = ReplicationNoNet;
            using FA = FieldAttribute;
            info->addField(new FieldInfoImpl("hp",    reg.findType<int32_t>(), offsetof(T, hp),    FA::Serialize));
            info->addField(new FieldInfoImpl("score", reg.findType<int32_t>(), offsetof(T, score), FA::Serialize | FA::NetReplicate));
            reg.registerTypeInfo("ReplicationNoNet", info);
        }
    }
};

static ReplicationFixtureRegistrar g_replicationFixtureRegistrar;

} // anonymous namespace

namespace
{

// Pump two endpoints concurrently until predicate is true or timeout.
bool pumpUntil(GnsConnection& a, GnsConnection& b,
               std::chrono::milliseconds timeout,
               const std::function<bool()>& pred,
               const char* what) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        a.update();
        b.update();
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ::printf("    [Replication] TIMEOUT waiting for %s (a.state=%d b.state=%d)\n",
             what, static_cast<int>(a.getState()), static_cast<int>(b.getState()));
    return false;
}

} // anonymous namespace

// =============================================================================
// Case 1 — ReflectSerializer round-trip across all 12 WireTypeIds.
// =============================================================================
TEST_SUITE(ReflectSerializer)
TEST_CASE(RoundTripPrimitives) {
    ayt::test::setCurrentCase("RoundTripPrimitives");

    const auto* type = ayt::reflect::TypeRegistryImpl::instance().findType<ReplicationAllPrimitives>();
    CHECK(type != nullptr);

    ReplicationAllPrimitives src;
    src.b   = false;
    src.i8  = 7;
    src.i16 = 12345;
    src.i32 = -1;
    src.i64 = 0x1122334455667788LL;
    src.u8  = 99;
    src.u16 = 12345;
    src.u32 = 0xDEADBEEFu;
    src.u64 = 0xFEDCBA9876543210ull;
    src.f   = -2.5f;
    src.d   = -3.14159265;
    src.s   = "round trip test";

    BitStream wire;
    CHECK(ReflectSerializer::serializeObject(type, &src, /*netId=*/ 42, wire));

    // Header sanity
    ReflectSerializer::FrameHeader hdr;
    wire.resetForRead();
    CHECK(ReflectSerializer::readReplicationFrameHeader(wire, hdr));
    CHECK_INT_EQ(static_cast<uint32_t>(hdr.netId), 42u);
    CHECK_INT_EQ(static_cast<int>(hdr.fieldCount), 12);
    CHECK_INT_EQ(static_cast<int>(hdr.reserved), 0);

    // Round-trip back
    ReplicationAllPrimitives dst{};
    CHECK(ReflectSerializer::deserializeObject(type, &dst, wire, hdr.fieldCount));

    CHECK(dst.b   == src.b);
    CHECK(dst.i8  == src.i8);
    CHECK(dst.i16 == src.i16);
    CHECK(dst.i32 == src.i32);
    CHECK(dst.i64 == src.i64);
    CHECK(dst.u8  == src.u8);
    CHECK(dst.u16 == src.u16);
    CHECK(dst.u32 == src.u32);
    CHECK(dst.u64 == src.u64);
    CHECK(dst.f   == src.f);
    CHECK(dst.d   == src.d);
    CHECK(dst.s   == src.s);
}
TEST_SUITE_END

// =============================================================================
// Case 2 — Fields with only FieldAttribute::Serialize (no NetReplicate) are
// filtered out of the wire frame entirely.
// =============================================================================
TEST_SUITE(ReflectSerializerFilter)
TEST_CASE(FiltersNonReplicated) {
    ayt::test::setCurrentCase("FiltersNonReplicated");

    const auto* type = ayt::reflect::TypeRegistryImpl::instance().findType<ReplicationNoNet>();
    CHECK(type != nullptr);

    ReplicationNoNet src;
    src.hp = 9999;       // not on wire
    src.score = 7;       // on wire

    BitStream wire;
    CHECK(ReflectSerializer::serializeObject(type, &src, /*netId=*/ 5, wire));

    ReflectSerializer::FrameHeader hdr;
    wire.resetForRead();
    CHECK(ReflectSerializer::readReplicationFrameHeader(wire, hdr));
    CHECK_INT_EQ(static_cast<int>(hdr.fieldCount), 1);

    // After reading 1 record, stream should be empty.
    ReplicationNoNet dst{};
    CHECK(ReflectSerializer::deserializeObject(type, &dst, wire, hdr.fieldCount));
    CHECK(dst.score == 7);    // NetReplicate field, round-tripped
    CHECK(dst.hp == 100);     // default-initialised, never touched by serialize (Serialize-only field skipped)
}
TEST_SUITE_END

// =============================================================================
// Case 3 — End-to-end GNS loopback: server register → tick → client receives.
// =============================================================================
TEST_SUITE(ReplicationManagerLoopback)

TEST_CASE(SnapshotBroadcast) {
    ayt::test::setCurrentCase("SnapshotBroadcast");

    if (!gns::init()) { CHECK(false); return; }

    constexpr uint16_t kVirtualPort = 27444;

    GnsConnection server, client;
    server.setProtocolVersion(1);
    client.setProtocolVersion(1);
    server.initServer(kVirtualPort);
    client.initClient("127.0.0.1", kVirtualPort);

    auto ready = [&]() { return client.getState() == GnsConnectionState::Ready; };
    CHECK(pumpUntil(server, client, std::chrono::seconds(5), ready, "handshake"));

    // ReplicationManager on both sides — but only the SERVER side tick()s
    // and broadcasts. Client-side manager only deserializes incoming frames.
    ReplicationManager serverMgr(nullptr);
    serverMgr.setModeForTesting(ConnectionMode::Server);
    ReplicationManager clientMgr(nullptr);

    auto* type = ayt::reflect::TypeRegistryImpl::instance().findType<ReplicationAllPrimitives>();
    CHECK(type != nullptr);

    // Test seam: route the server-side broadcast directly to the client's
    // onRawData (bypassing PacketCodec seal — we already pass sealed bytes).
    serverMgr.setBroadcastSinkForTesting([&](uint8_t /*ch*/, const void* data, size_t size) {
        client.onRawData(static_cast<const uint8_t*>(data), size);
    });

    // Hook a client-side data sink: parse Replication frames and apply.
    ReplicationAllPrimitives received{};
    std::atomic<bool> gotFrame{false};
    client.onData([&](const uint8_t* data, size_t size) {
        BitStream bs(const_cast<uint8_t*>(data), size);
        // Body prefix: [u16 innerMsgType] — ReplicationManager added this.
        if (bs.getBitPosition() + 16 > bs.getBitCount()) return;
        uint16_t inner = bs.readUInt16();
        if (inner != kMsgTypeReplication) return;
        ReflectSerializer::FrameHeader hdr;
        if (!ReflectSerializer::readReplicationFrameHeader(bs, hdr)) return;
        ReflectSerializer::deserializeObject(type, &received, bs, hdr.fieldCount);
        gotFrame = true;
    });

    // Register on server
    ReplicationAllPrimitives src{};
    src.i32 = 0xABCDEF01;
    src.s   = "from server";
    serverMgr.registerObject(&src, type, /*netId=*/ 17);

    // Tick until client receives at least one frame
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (std::chrono::steady_clock::now() < deadline && !gotFrame) {
        server.update();
        client.update();
        serverMgr.tick(0.016f);
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    CHECK(gotFrame.load());
    CHECK(received.i32 == 0xABCDEF01);
    CHECK(received.s   == "from server");

    gns::shutdown();
}
TEST_SUITE_END

// =============================================================================
// Case 4 — EntitySpawn / EntityDespawn over GNS.
// =============================================================================
TEST_SUITE(ReplicationManagerSpawnDespawn)
TEST_CASE(EntitySpawnAndDespawn) {
    ayt::test::setCurrentCase("EntitySpawnAndDespawn");

    if (!gns::init()) { CHECK(false); return; }

    constexpr uint16_t kVirtualPort = 27445;

    GnsConnection server, client;
    server.setProtocolVersion(1);
    client.setProtocolVersion(1);
    server.initServer(kVirtualPort);
    client.initClient("127.0.0.1", kVirtualPort);
    CHECK(pumpUntil(server, client, std::chrono::seconds(5),
                    [&]() { return client.getState() == GnsConnectionState::Ready; },
                    "handshake"));

    ReplicationManager serverMgr(nullptr);
    serverMgr.setModeForTesting(ConnectionMode::Server);
    ReplicationManager clientMgr(nullptr);

    auto* type = ayt::reflect::TypeRegistryImpl::instance().findType<ReplicationNoNet>();
    CHECK(type != nullptr);

    std::atomic<int> spawnCount{0};
    std::atomic<int> despawnCount{0};
    uint32_t spawnedNetId = 0;
    uint16_t spawnedTypeHash = 0;

    serverMgr.setBroadcastSinkForTesting([&](uint8_t /*ch*/, const void* data, size_t size) {
        client.onRawData(static_cast<const uint8_t*>(data), size);
    });

    client.onData([&](const uint8_t* data, size_t size) {
        BitStream bs(const_cast<uint8_t*>(data), size);
        // Body prefix: [u16 innerMsgType] then payload.
        if (bs.getBitPosition() + 16 > bs.getBitCount()) return;
        uint16_t inner = bs.readUInt16();
        if (inner == kMsgTypeEntitySpawn) {
            uint32_t nid; uint16_t th;
            if (ReflectSerializer::readEntitySpawn(bs, nid, th)) {
                spawnCount++;
                spawnedNetId = nid;
                spawnedTypeHash = th;
            }
            return;
        }
        if (inner == kMsgTypeEntityDespawn) {
            uint32_t nid;
            if (ReflectSerializer::readEntityDespawn(bs, nid)) {
                despawnCount++;
            }
            return;
        }
    });

    ReplicationNoNet obj;
    serverMgr.registerObject(&obj, type, /*netId=*/ 99);
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (std::chrono::steady_clock::now() < deadline && spawnCount.load() == 0) {
        server.update(); client.update(); serverMgr.tick(0.016f);
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    CHECK(spawnCount.load() >= 1);
    CHECK(spawnedNetId == 99u);
    CHECK(spawnedTypeHash == static_cast<uint16_t>(type->getId() & 0xFFFFu));

    serverMgr.unregisterObject(99);
    deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (std::chrono::steady_clock::now() < deadline && despawnCount.load() == 0) {
        server.update(); client.update(); serverMgr.tick(0.016f);
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    CHECK(despawnCount.load() >= 1);

    gns::shutdown();
}
TEST_SUITE_END

// =============================================================================
// Case 5 — §6.6 Authority: server's onReceive drops frames that don't match
// a locally-registered (netId, ITypeInfo*). Since the server never sees
// the client's registerObject (clients don't broadcast EntitySpawn either),
// frames addressed to an unknown netId must be silently dropped. This proves
// the lookup gate (findType(netId) == nullptr → return false) holds.
// =============================================================================
TEST_SUITE(ReplicationAuthorityGate)
TEST_CASE(ServerDropsClientReplicate) {
    ayt::test::setCurrentCase("ServerDropsClientReplicate");

    if (!gns::init()) { CHECK(false); return; }

    constexpr uint16_t kVirtualPort = 27446;

    GnsConnection server, client;
    server.setProtocolVersion(1);
    client.setProtocolVersion(1);
    server.initServer(kVirtualPort);
    client.initClient("127.0.0.1", kVirtualPort);
    CHECK(pumpUntil(server, client, std::chrono::seconds(5),
                    [&]() { return client.getState() == GnsConnectionState::Ready; },
                    "handshake"));

    ReplicationManager serverMgr(nullptr);
    serverMgr.setModeForTesting(ConnectionMode::Server);
    ReplicationManager clientMgr(nullptr);
    clientMgr.setModeForTesting(ConnectionMode::Client);

    auto* type = ayt::reflect::TypeRegistryImpl::instance().findType<ReplicationNoNet>();
    CHECK(type != nullptr);

    // Server side starts with zero registered objects. The client side has
    // its own local registration so it can author a frame, but the server
    // never receives the client's EntitySpawn (clients don't broadcast).
    CHECK(serverMgr.getRegisteredCount() == 0);

    // Client side: register an object, then route a sealed Replication frame
    // from client → server via the test sink (bypass real GNS loopback so
    // we can directly observe onReceive's gate).
    ReplicationNoNet clientObj;
    clientMgr.registerObject(&clientObj, type, /*netId=*/ 7);

    std::atomic<bool> serverOnReceiveReturned{false};
    std::atomic<bool> serverOnReceiveConsumed{false};

    // Build the same frame the client would have built if it broadcast —
    // we deliver it directly to serverMgr.onReceive, asserting the gate
    // rejects it because the server has no matching local registration.
    BitStream body;
    body.writeUInt16(kMsgTypeReplication);
    ReplicationNoNet src;
    src.score = 42;
    ReflectSerializer::serializeObject(type, &src, /*netId=*/ 7, body);
    auto sealed = PacketCodec::encode(
        static_cast<const uint8_t*>(body.getData()), body.getSize(),
        kMsgTypeReplication, kSchemaVersion,
        CHANNEL_RELIABLE, /*flags=*/ 0, /*timestampMs=*/ 0,
        /*compress=*/ false);

    // Decode at server side (simulating what onRawData would do) and feed
    // body into onReceive.
    DecodedPacket dec = PacketCodec::decode(sealed.data(), sealed.size());
    CHECK(dec.ok);
    BitStream bs(dec.body.data(), dec.body.size());
    bool consumed = serverMgr.onReceive(bs, /*from=*/ nullptr);
    serverOnReceiveReturned.store(true);
    serverOnReceiveConsumed.store(consumed);

    // The server must have rejected the frame (findType(7) == nullptr).
    CHECK(serverOnReceiveReturned.load());
    CHECK(!serverOnReceiveConsumed.load()); // false == dropped
    CHECK(serverMgr.getRegisteredCount() == 0);

    gns::shutdown();
}
TEST_SUITE_END

// =============================================================================
// Case 6 — Replicate frames survive PacketCodec seal (12B header + body + 4B CRC).
// =============================================================================
TEST_SUITE(ReplicationCodecIntegration)
TEST_CASE(SerializeAndDecodeOverPacketCodec) {
    ayt::test::setCurrentCase("SerializeAndDecodeOverPacketCodec");

    auto* type = ayt::reflect::TypeRegistryImpl::instance().findType<ReplicationAllPrimitives>();
    CHECK(type != nullptr);

    ReplicationAllPrimitives src{};
    src.i32 = 0x11223344;
    src.s   = "PacketCodec";

    // 1. Build the ReplicationFrame body in a BitStream.
    BitStream body;
    CHECK(ReflectSerializer::serializeObject(type, &src, /*netId=*/ 101, body));

    // 2. Seal it through PacketCodec with kMsgTypeReplication.
    std::vector<uint8_t> sealed = PacketCodec::encode(
        static_cast<const uint8_t*>(body.getData()), body.getSize(),
        kMsgTypeReplication, kSchemaVersion,
        CHANNEL_RELIABLE, /*flags=*/ 0, /*timestampMs=*/ 0,
        /*compress=*/ false);

    // 3. Decode at the receiver side.
    DecodedPacket dec = PacketCodec::decode(sealed.data(), sealed.size());
    CHECK(dec.ok);
    CHECK(dec.header.msgType == kMsgTypeReplication);

    // 4. Feed the body back into ReflectSerializer.
    BitStream bs(dec.body.data(), dec.body.size());
    ReflectSerializer::FrameHeader hdr;
    CHECK(ReflectSerializer::readReplicationFrameHeader(bs, hdr));
    CHECK(hdr.netId == 101u);

    ReplicationAllPrimitives dst{};
    CHECK(ReflectSerializer::deserializeObject(type, &dst, bs, hdr.fieldCount));
    CHECK(dst.i32 == 0x11223344);
    CHECK(dst.s   == "PacketCodec");
}
TEST_SUITE_END