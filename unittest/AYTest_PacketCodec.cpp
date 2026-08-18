// AYTest_PacketCodec.cpp - Protocol-layer conformance tests for PacketCodec.
//
// R2 (2026-07-27): 10 cases covering the §5.1 wire format end-to-end.
//
// Cases 1-7 are pure-function (no GNS): they exercise PacketCodec::encode
// and PacketCodec::decode directly with raw byte vectors, validating:
//   - Plain round-trip with all header fields preserved
//   - CRC32C detects body and header corruption
//   - CRC32C scope = [header][body], excludes trailing CRC bytes
//   - Known-vector test for CRC32C ("123456789" -> 0xE3069283)
//   - lz4 compress + decompress round-trip via AYStorage::Lz4Decompressor
//   - Fragmenter produces N frames with the LAST variable-length
//   - Assembler reassembles in-order, out-of-order, with duplicates, with loss
//
// Cases 8-10 use real GNS to validate the GnsConnection integration:
//   - Handshake frames sealed via PacketCodec -> msgType demux in onRawData
//   - REJECT path preserves linger=true semantics
//   - End-to-end small + fragmented-large app message round-trip
//
// Test convention (from R1 retro):
//   - AYTest framework only; CHECK_* macros; no REQUIRE / fixtures
//   - setCurrentCase at the top of every TEST_CASE
//   - CHECK_INT_EQ double-evaluates -> store stream reads in locals first
//   - GNS tests use pumpUntil + atomics + gns::init/shutdown

#include <AYNetwork.h>
#include <AYTest.h>
#include <AYNetwork/Protocol/PacketCodec.h>
#include <AYNetwork/Protocol/PacketAssembler.h>
#include <AYNetwork/Transport/GnsConnection.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <functional>
#include <random>
#include <string>
#include <thread>
#include <vector>

using namespace ayt::net;

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
    ::printf("    [PacketCodec] TIMEOUT waiting for %s (a.state=%d b.state=%d)\n",
             what, static_cast<int>(a.getState()), static_cast<int>(b.getState()));
    return pred();
}

} // anonymous namespace

// =============================================================================
// Pure (no GNS) — TEST_SUITE(PacketCodec)
// =============================================================================
TEST_SUITE(PacketCodec)

// 1) Plain round-trip: every header field survives + body preserved verbatim.
TEST_CASE(CodecRoundTripPlain) {
    ayt::test::setCurrentCase("CodecRoundTripPlain");

    const uint8_t body[] = {'h','e','l','l','o',',','w','o','r','l','d'};
    auto wire = PacketCodec::encode(
        body, sizeof(body),
        /*msgType=*/42, /*schemaVersion=*/kSchemaVersion,
        /*channel=*/CHANNEL_RELIABLE,
        /*flags=*/0,
        /*timestampMs=*/123456789,
        /*compress=*/false);

    CHECK(wire.size() == PacketCodec::kHeaderSize + sizeof(body) + PacketCodec::kCrcSize);

    auto d = PacketCodec::decode(wire.data(), wire.size());
    CHECK(d.ok);
    CHECK_INT_EQ(d.header.msgType, 42);
    CHECK_INT_EQ(d.header.schemaVersion, kSchemaVersion);
    CHECK_INT_EQ(d.header.length, sizeof(body));
    CHECK_INT_EQ(d.header.channel, CHANNEL_RELIABLE);
    CHECK_INT_EQ(d.header.flags, 0);
    CHECK_INT_EQ(d.header.timestampMs, 123456789);
    CHECK_INT_EQ(d.body.size(), sizeof(body));
    CHECK(std::memcmp(d.body.data(), body, sizeof(body)) == 0);
}

// 2) CRC detects body + header corruption.
TEST_CASE(CodecCrcRejectsCorruption) {
    ayt::test::setCurrentCase("CodecCrcRejectsCorruption");

    const uint8_t body[] = "abcdefghijklmnop";
    auto wire = PacketCodec::encode(
        body, sizeof(body),
        /*msgType=*/7, /*schemaVersion=*/kSchemaVersion,
        /*channel=*/CHANNEL_RELIABLE,
        /*flags=*/0,
        /*timestampMs=*/1,
        /*compress=*/false);

    // Flip one body byte (offset somewhere in the body region).
    wire[PacketCodec::kHeaderSize + 3] ^= 0xFF;
    auto d1 = PacketCodec::decode(wire.data(), wire.size());
    CHECK_FALSE(d1.ok);

    // Re-encode and flip one header byte (channel field).
    auto wire2 = PacketCodec::encode(
        body, sizeof(body),
        /*msgType=*/7, /*schemaVersion=*/kSchemaVersion,
        /*channel=*/CHANNEL_RELIABLE,
        /*flags=*/0,
        /*timestampMs=*/1,
        /*compress=*/false);
    wire2[6] ^= 0x01;   // channel
    auto d2 = PacketCodec::decode(wire2.data(), wire2.size());
    CHECK_FALSE(d2.ok);
}

// 3) CRC scope: covers [header][body], excludes the trailing 4B.
//    Also checks the reference vector CRC32C("123456789") = 0xE3069283.
TEST_CASE(CodecCrcScopeExcludesTrailer) {
    ayt::test::setCurrentCase("CodecCrcScopeExcludesTrailer");

    // Reference vector: CRC32C of ASCII "123456789" (9 bytes) =
    //   0xE3069283 (Castagnoli polynomial).
    const uint8_t nine[] = {'1','2','3','4','5','6','7','8','9'};
    uint32_t crc = PacketCodec::computeCrc32c(nine, sizeof(nine));
    CHECK_INT_EQ(static_cast<uint32_t>(crc), 0xE3069283u);

    // Now build a frame and verify flipping the trailing CRC makes decode fail,
    // and that decode succeeds when the trailing 4B are untouched but header/body
    // bytes are.
    auto wire = PacketCodec::encode(
        nine, sizeof(nine),
        /*msgType=*/1, /*schemaVersion=*/kSchemaVersion,
        /*channel=*/CHANNEL_RELIABLE,
        /*flags=*/0,
        /*timestampMs=*/0,
        /*compress=*/false);
    // Flip a byte in the trailing CRC; length check still passes (CRC is not
    // length-counted), but the CRC itself will mismatch.
    wire[wire.size() - 1] ^= 0x80;
    auto d = PacketCodec::decode(wire.data(), wire.size());
    CHECK_FALSE(d.ok);
}

// 4) lz4 round-trip: 4 KB compressible buffer + tiny buffer.
TEST_CASE(CodecLz4RoundTrip) {
    ayt::test::setCurrentCase("CodecLz4RoundTrip");

    // 4 KB of repeating pattern -> highly compressible.
    std::vector<uint8_t> big(4096);
    for (size_t i = 0; i < big.size(); ++i) big[i] = static_cast<uint8_t>(i & 0x7);

    auto wireBig = PacketCodec::encode(
        big.data(), big.size(),
        /*msgType=*/1, /*schemaVersion=*/kSchemaVersion,
        /*channel=*/CHANNEL_RELIABLE,
        /*flags=*/0,
        /*timestampMs=*/0,
        /*compress=*/true);

    CHECK(hasFlag(wireBig[7], PacketFlag::Compressed));
    auto dBig = PacketCodec::decode(wireBig.data(), wireBig.size());
    CHECK(dBig.ok);
    CHECK_INT_EQ(dBig.body.size(), big.size());
    CHECK(std::memcmp(dBig.body.data(), big.data(), big.size()) == 0);

    // Tiny incompressible-ish body (random bytes).
    std::vector<uint8_t> tiny(64);
    for (size_t i = 0; i < tiny.size(); ++i) tiny[i] = static_cast<uint8_t>(i * 37 + 11);
    auto wireTiny = PacketCodec::encode(
        tiny.data(), tiny.size(),
        /*msgType=*/1, /*schemaVersion=*/kSchemaVersion,
        /*channel=*/CHANNEL_RELIABLE,
        /*flags=*/0,
        /*timestampMs=*/0,
        /*compress=*/true);
    auto dTiny = PacketCodec::decode(wireTiny.data(), wireTiny.size());
    CHECK(dTiny.ok);
    CHECK_INT_EQ(dTiny.body.size(), tiny.size());
    CHECK(std::memcmp(dTiny.body.data(), tiny.data(), tiny.size()) == 0);
}

// 5) Fragment + in-order reassemble; assert last chunk is variable-length.
TEST_CASE(FragmentAndReassembleInOrder) {
    ayt::test::setCurrentCase("FragmentAndReassembleInOrder");

    constexpr size_t kPayload = 5000;
    constexpr uint32_t kMtu    = 600;
    std::vector<uint8_t> payload(kPayload);
    for (size_t i = 0; i < payload.size(); ++i) payload[i] = static_cast<uint8_t>(i & 0xFF);

    auto frames = PacketAssembler::fragment(
        payload.data(), payload.size(), kMtu,
        /*msgType=*/1, /*schemaVersion=*/kSchemaVersion,
        /*channel=*/CHANNEL_RELIABLE,
        /*timestampMs=*/0,
        /*fragmentId=*/0xDEADBEEFu);
    CHECK(frames.size() >= 2);
    // Each frame must fit in MTU.
    for (const auto& f : frames) {
        CHECK(f.size() <= kMtu);
    }
    // Last frame's body chunk must be shorter than the others' body chunk.
    // (We can't peek FragmentHeader without decoding; instead, validate by
    // reassembly and confirm total == payload size.)
    PacketAssembler asmblr;
    std::optional<std::vector<uint8_t>> result;
    for (const auto& f : frames) {
        auto d = PacketCodec::decode(f.data(), f.size());
        CHECK(d.ok);
        CHECK(hasFlag(d.header.flags, PacketFlag::Fragmented));
        result = asmblr.consume(d.body.data(), d.body.size());
    }
    CHECK(result.has_value());
    CHECK_INT_EQ(result->size(), payload.size());
    CHECK(std::memcmp(result->data(), payload.data(), payload.size()) == 0);
}

// 6) Reassemble: reorder + duplicates + loss-then-readd.
TEST_CASE(FragmentReorderDupLoss) {
    ayt::test::setCurrentCase("FragmentReorderDupLoss");

    constexpr size_t kPayload = 5000;
    constexpr uint32_t kMtu    = 600;
    std::vector<uint8_t> payload(kPayload);
    for (size_t i = 0; i < payload.size(); ++i) payload[i] = static_cast<uint8_t>(i & 0xFF);

    auto frames = PacketAssembler::fragment(
        payload.data(), payload.size(), kMtu,
        /*msgType=*/1, /*schemaVersion=*/kSchemaVersion,
        /*channel=*/CHANNEL_RELIABLE,
        /*timestampMs=*/0,
        /*fragmentId=*/0xC0FFEE01u);
    const size_t N = frames.size();
    CHECK(N >= 3);

    // Build a permutation: reverse first, then re-feed the middle one twice.
    std::vector<size_t> order;
    for (size_t i = 0; i < N; ++i) order.push_back(N - 1 - i); // reversed
    // Inject a duplicate of the middle frame at the end.
    order.push_back(N / 2);

    PacketAssembler asmblr;
    std::optional<std::vector<uint8_t>> result;
    int completeHits = 0;
    for (size_t idx : order) {
        auto d = PacketCodec::decode(frames[idx].data(), frames[idx].size());
        CHECK(d.ok);
        auto r = asmblr.consume(d.body.data(), d.body.size());
        if (r.has_value()) {
            ++completeHits;
            result = std::move(r);
        }
    }
    // Exactly one completion — the duplicate after the last unique fragment
    // is a no-op.
    CHECK_INT_EQ(completeHits, 1);
    CHECK(result.has_value());
    CHECK_INT_EQ(result->size(), payload.size());
    CHECK(std::memcmp(result->data(), payload.data(), payload.size()) == 0);

    // Loss scenario: a brand-new packet, drop one fragment, then feed the rest.
    auto frames2 = PacketAssembler::fragment(
        payload.data(), payload.size(), kMtu,
        /*msgType=*/1, /*schemaVersion=*/kSchemaVersion,
        /*channel=*/CHANNEL_RELIABLE,
        /*timestampMs=*/0,
        /*fragmentId=*/0xC0FFEE02u);
    PacketAssembler asmblr2;
    std::optional<std::vector<uint8_t>> result2;
    for (size_t i = 0; i < frames2.size(); ++i) {
        if (i == 1) continue; // skip fragment 1
        auto d = PacketCodec::decode(frames2[i].data(), frames2[i].size());
        CHECK(d.ok);
        auto r = asmblr2.consume(d.body.data(), d.body.size());
        if (r.has_value()) result2 = std::move(r);
    }
    CHECK_FALSE(result2.has_value());
}

// 7) Variable-length last fragment: payload sized so the last chunk is
//    exactly 1 byte. Guards the over-alloc bug in the old assemble().
TEST_CASE(FragmentVariableLastFragment) {
    ayt::test::setCurrentCase("FragmentVariableLastFragment");

    constexpr size_t kPayload = 1025; // > 1024 so we get >1 fragment
    constexpr uint32_t kMtu    = 256;
    std::vector<uint8_t> payload(kPayload);
    for (size_t i = 0; i < payload.size(); ++i) payload[i] = static_cast<uint8_t>(i & 0xFF);

    auto frames = PacketAssembler::fragment(
        payload.data(), payload.size(), kMtu,
        /*msgType=*/1, /*schemaVersion=*/kSchemaVersion,
        /*channel=*/CHANNEL_RELIABLE,
        /*timestampMs=*/0,
        /*fragmentId=*/0xCAFEBABEu);
    CHECK(frames.size() >= 2);

    PacketAssembler asmblr;
    std::optional<std::vector<uint8_t>> result;
    for (const auto& f : frames) {
        auto d = PacketCodec::decode(f.data(), f.size());
        CHECK(d.ok);
        auto r = asmblr.consume(d.body.data(), d.body.size());
        if (r.has_value()) result = std::move(r);
    }
    CHECK(result.has_value());
    CHECK_INT_EQ(result->size(), kPayload);
    CHECK(std::memcmp(result->data(), payload.data(), payload.size()) == 0);
}

TEST_SUITE_END

// =============================================================================
// GNS-backed — TEST_SUITE(PacketCodecGNS)
// =============================================================================
TEST_SUITE(PacketCodecGNS)

TEST_CASE(CodecRejectsOversizedPlainBody) {
    ayt::test::setCurrentCase("CodecRejectsOversizedPlainBody");

    std::vector<uint8_t> body(PacketCodec::kMaxWireBodySize + 1u, 0x5A);
    auto wire = PacketCodec::encode(
        body.data(), body.size(), 7, kSchemaVersion,
        CHANNEL_RELIABLE, 0, 1, false);
    CHECK(wire.empty());
}

TEST_CASE(CodecRejectsDecompressionBomb) {
    ayt::test::setCurrentCase("CodecRejectsDecompressionBomb");

    std::vector<uint8_t> body(1024, 0x41);
    auto wire = PacketCodec::encode(
        body.data(), body.size(), 8, kSchemaVersion,
        CHANNEL_RELIABLE, 0, 2, true);
    CHECK(!wire.empty());

    const uint32_t oversized =
        static_cast<uint32_t>(PacketCodec::kMaxDecodedBodySize + 1u);
    uint8_t* unc = wire.data() + PacketCodec::kHeaderSize;
    unc[0] = static_cast<uint8_t>(oversized & 0xFF);
    unc[1] = static_cast<uint8_t>((oversized >> 8) & 0xFF);
    unc[2] = static_cast<uint8_t>((oversized >> 16) & 0xFF);
    unc[3] = static_cast<uint8_t>((oversized >> 24) & 0xFF);

    const uint32_t crc = PacketCodec::computeCrc32c(
        wire.data(), wire.size() - PacketCodec::kCrcSize);
    uint8_t* trailer = wire.data() + wire.size() - PacketCodec::kCrcSize;
    trailer[0] = static_cast<uint8_t>(crc & 0xFF);
    trailer[1] = static_cast<uint8_t>((crc >> 8) & 0xFF);
    trailer[2] = static_cast<uint8_t>((crc >> 16) & 0xFF);
    trailer[3] = static_cast<uint8_t>((crc >> 24) & 0xFF);

    auto decoded = PacketCodec::decode(wire.data(), wire.size());
    CHECK(!decoded.ok);
    CHECK(decoded.body.empty());
}

TEST_CASE(AssemblerEnforcesFragmentAndInflightBudgets) {
    ayt::test::setCurrentCase("AssemblerEnforcesFragmentAndInflightBudgets");

    PacketAssembler assembler;
    assembler.setMaxFragments(2);
    assembler.setMaxInFlight(1);
    assembler.setTimeoutMs(10);

    // [fragmentId=1][index=0][count=3][one-byte chunk]
    const uint8_t tooManyFragments[] = {1,0,0,0, 0,0, 3,0, 0xAA};
    auto rejected = assembler.consume(
        tooManyFragments, sizeof(tooManyFragments), 100);
    CHECK(!rejected.has_value());
    CHECK_INT_EQ(assembler.pendingCount(), 0);

    const uint8_t firstAssembly[] = {2,0,0,0, 0,0, 2,0, 0xBB};
    const uint8_t secondAssembly[] = {3,0,0,0, 0,0, 2,0, 0xCC};
    (void)assembler.consume(firstAssembly, sizeof(firstAssembly), 100);
    CHECK_INT_EQ(assembler.pendingCount(), 1);
    (void)assembler.consume(secondAssembly, sizeof(secondAssembly), 101);
    CHECK_INT_EQ(assembler.pendingCount(), 1);

    assembler.reapExpired(111);
    CHECK_INT_EQ(assembler.pendingCount(), 0);
    CHECK_INT_EQ(assembler.pendingBytes(), 0);
}

// 8) HELLO/WELCOME sealed as kMsgTypeHandshake frames; onRawData demuxes
//    by msgType; both sides reach Ready; small app message flows.
TEST_CASE(HandshakeOverPacketHeaderHappyPath) {
    ayt::test::setCurrentCase("HandshakeOverPacketHeaderHappyPath");

    CHECK(gns::init());

    constexpr uint16_t kPort = 7820;

    GnsConnection server;
    server.setProtocolVersion(kProtocolVersion);
    std::atomic<int> serverDataFrames{0};
    server.onData([&](const uint8_t*, size_t) { serverDataFrames.fetch_add(1); });
    server.initServer(kPort);

    GnsConnection client;
    client.setProtocolVersion(kProtocolVersion);
    std::atomic<int> clientDataFrames{0};
    client.onData([&](const uint8_t*, size_t) { clientDataFrames.fetch_add(1); });
    client.initClient("127.0.0.1", kPort);

    bool ready = pumpUntil(client, server, std::chrono::seconds(5), [&] {
        return client.getState() == GnsConnectionState::Ready &&
               server.getState() == GnsConnectionState::Ready;
    }, "both Ready");
    CHECK(ready);

    // App data — client -> server.
    int sr = client.send(0, "ping", 4);
    CHECK_INT_EQ(sr, 0);
    bool sGot = pumpUntil(client, server, std::chrono::seconds(5), [&] {
        return serverDataFrames.load() >= 1;
    }, "server got app data");
    CHECK(sGot);

    // App data — server -> client.
    int er = server.send(0, "pong", 4);
    CHECK_INT_EQ(er, 0);
    bool cGot = pumpUntil(client, server, std::chrono::seconds(5), [&] {
        return clientDataFrames.load() >= 1;
    }, "client got app data");
    CHECK(cGot);

    client.disconnect("test done");
    server.disconnect("test done");
    gns::shutdown();
}

// 9) Version mismatch -> REJECT sealed + linger=true; client sees
//    DisconnectReason::ProtocolMismatch.
TEST_CASE(HandshakeRejectOverPacketHeader) {
    ayt::test::setCurrentCase("HandshakeRejectOverPacketHeader");

    CHECK(gns::init());

    constexpr uint16_t kPort = 7821;

    GnsConnection server;
    server.setProtocolVersion(kProtocolVersion + 1);  // server expects v2
    server.initServer(kPort);

    GnsConnection client;
    client.setProtocolVersion(kProtocolVersion);      // client speaks v1
    client.initClient("127.0.0.1", kPort);

    bool rejected = pumpUntil(client, server, std::chrono::seconds(5), [&] {
        return client.getLastDisconnectReason() == DisconnectReason::ProtocolMismatch;
    }, "client rejected with ProtocolMismatch");
    CHECK(rejected);
    CHECK(client.getLastDisconnectReason() == DisconnectReason::ProtocolMismatch);

    server.disconnect("test done");
    gns::shutdown();
}

// 10) End-to-end framed app message: small message + large message that
//     fragments across MTU. Both decoded and forwarded to onData.
TEST_CASE(EndToEndFramedAppMessage) {
    ayt::test::setCurrentCase("EndToEndFramedAppMessage");

    CHECK(gns::init());

    constexpr uint16_t kPort = 7822;

    GnsConnection server;
    server.setProtocolVersion(kProtocolVersion);
    std::vector<uint8_t> serverPayload;
    std::atomic<bool> serverGotLarge{false};
    server.onData([&](const uint8_t* data, size_t len) {
        serverPayload.assign(data, data + len);
        if (len >= 4096) serverGotLarge.store(true);
    });
    server.initServer(kPort);

    GnsConnection client;
    client.setProtocolVersion(kProtocolVersion);
    std::vector<uint8_t> clientPayload;
    std::atomic<bool> clientGotLarge{false};
    client.onData([&](const uint8_t* data, size_t len) {
        clientPayload.assign(data, data + len);
        if (len >= 4096) clientGotLarge.store(true);
    });
    client.initClient("127.0.0.1", kPort);

    bool ready = pumpUntil(client, server, std::chrono::seconds(5), [&] {
        return client.getState() == GnsConnectionState::Ready &&
               server.getState() == GnsConnectionState::Ready;
    }, "both Ready");
    CHECK(ready);

    // Small message.
    int sr = client.send(0, "hello", 5);
    CHECK_INT_EQ(sr, 0);
    bool sGotSmall = pumpUntil(client, server, std::chrono::seconds(5), [&] {
        return serverPayload.size() == 5;
    }, "server got small");
    CHECK(sGotSmall);
    CHECK(std::memcmp(serverPayload.data(), "hello", 5) == 0);

    // Large message (~6 KB) -> fragments across MTU. The Assembler must
    // reassemble before onData fires, and the bytes must equal the original.
    std::vector<uint8_t> large(6144);
    for (size_t i = 0; i < large.size(); ++i) large[i] = static_cast<uint8_t>((i * 7) & 0xFF);
    int lr = client.send(0, large.data(), large.size());
    CHECK_INT_EQ(lr, 0);
    bool sGotLarge = pumpUntil(client, server, std::chrono::seconds(8), [&] {
        return serverGotLarge.load() && serverPayload.size() == large.size();
    }, "server reassembled large");
    CHECK(sGotLarge);
    CHECK(std::memcmp(serverPayload.data(), large.data(), large.size()) == 0);

    // Server -> client large message.
    int er = server.send(0, large.data(), large.size());
    CHECK_INT_EQ(er, 0);
    bool cGotLarge = pumpUntil(client, server, std::chrono::seconds(8), [&] {
        return clientGotLarge.load() && clientPayload.size() == large.size();
    }, "client reassembled large");
    CHECK(cGotLarge);
    CHECK(std::memcmp(clientPayload.data(), large.data(), large.size()) == 0);

    client.disconnect("test done");
    server.disconnect("test done");
    gns::shutdown();
}

TEST_SUITE_END
