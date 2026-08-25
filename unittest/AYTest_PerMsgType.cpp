// AYNetwork/unittest/AYTest_PerMsgType.cpp - R5.5 (2026-08-25)
// Per-msgType slot vs extras dispatch tests. 5 cases:
//   - Slot table maps 7 in-game msgTypes correctly
//   - Handshake bytes land in extras keyed by (msgType<<8|subType)
//   - AppAck and ClientInput correct slot/extra placement
//   - Percent-of-total math within 0.01 %
//   - Unknown msgType falls through to extras without crashing

#include <AYTest.h>
#include <AYNetwork/Profiler/ProfilerRegistry.h>
#include <AYNetwork/Profiler/ProfilerSnapshot.h>
#include <AYNetwork/Profiler/ProfilerMsgType.h>
#include <AYNetwork/INetwork.h>

using namespace ayt::net;

namespace {
constexpr const char* kCase1 = "PerMsgType_SlotTableDispatch";
constexpr const char* kCase2 = "PerMsgType_HandshakeExtrasKey";
constexpr const char* kCase3 = "PerMsgType_AppAckAndClientInputExtras";
constexpr const char* kCase4 = "PerMsgType_PercentageMath";
constexpr const char* kCase5 = "PerMsgType_UnknownMsgTypeFallsThrough";
} // anonymous namespace

TEST_SUITE(PerMsgType)

TEST_CASE(PerMsgType_SlotTableDispatch) {
    ayt::test::setCurrentCase(kCase1);
    ProfilerRegistry reg;
    constexpr uint32_t kConn = 1;

    reg.recordSend(kConn, kMsgTypeReplication,    10, 0);
    reg.recordSend(kConn, kMsgTypeEntitySpawn,   20, 0);
    reg.recordSend(kConn, kMsgTypeEntityDespawn, 30, 0);
    reg.recordSend(kConn, kMsgTypeDelta,         40, 0);
    reg.recordSend(kConn, kMsgTypeRpcRequest,    50, 0);
    reg.recordSend(kConn, kMsgTypeRpcResponse,   60, 0);
    reg.recordSend(kConn, kMsgTypeRpcReject,     70, 0);

    ProfilerSnapshot s;
    CHECK(reg.snapshotFor(kConn, s));

    CHECK(s.byMsgType[0].sendBytes == 10u);   // Replication
    CHECK(s.byMsgType[1].sendBytes == 20u);   // EntitySpawn
    CHECK(s.byMsgType[2].sendBytes == 30u);   // EntityDespawn
    CHECK(s.byMsgType[3].sendBytes == 40u);   // Delta
    CHECK(s.byMsgType[4].sendBytes == 50u);   // RpcRequest
    CHECK(s.byMsgType[5].sendBytes == 60u);   // RpcResponse
    CHECK(s.byMsgType[6].sendBytes == 70u);   // RpcReject

    // Verify the helper itself agrees with the table.
    CHECK(profiler::slotForInGameMsgType(kMsgTypeReplication)    == 0u);
    CHECK(profiler::slotForInGameMsgType(kMsgTypeEntitySpawn)   == 1u);
    CHECK(profiler::slotForInGameMsgType(kMsgTypeEntityDespawn) == 2u);
    CHECK(profiler::slotForInGameMsgType(kMsgTypeDelta)         == 3u);
    CHECK(profiler::slotForInGameMsgType(kMsgTypeRpcRequest)    == 4u);
    CHECK(profiler::slotForInGameMsgType(kMsgTypeRpcResponse)   == 5u);
    CHECK(profiler::slotForInGameMsgType(kMsgTypeRpcReject)     == 6u);

    CHECK(profiler::isInGameMsgType(kMsgTypeReplication));
    CHECK(profiler::isInGameMsgType(kMsgTypeRpcReject));
    CHECK(!profiler::isInGameMsgType(kMsgTypeAppAck));
    CHECK(!profiler::isInGameMsgType(kMsgTypeClientInput));
    CHECK(!profiler::isInGameMsgType(kMsgTypeHandshake));
}

TEST_CASE(PerMsgType_HandshakeExtrasKey) {
    ayt::test::setCurrentCase(kCase2);
    ProfilerRegistry reg;
    constexpr uint32_t kConn = 2;

    // The wire path computes the key via profiler::handshakeExtrasKey and
    // passes it as the msgType argument. Verify the encoding is unique per
    // (msgType, subType) pair.
    const uint16_t helloKey   = profiler::handshakeExtrasKey(HandshakeMsgType::Hello);
    const uint16_t welcomeKey = profiler::handshakeExtrasKey(HandshakeMsgType::Welcome);
    const uint16_t rejectKey  = profiler::handshakeExtrasKey(HandshakeMsgType::Reject);

    CHECK(helloKey   != welcomeKey);
    CHECK(helloKey   != rejectKey);
    CHECK(welcomeKey != rejectKey);

    reg.recordSend(kConn, helloKey,   100, 0);
    reg.recordSend(kConn, welcomeKey, 120, 0);
    reg.recordSend(kConn, rejectKey,   80, 0);
    reg.recordRecv(kConn, helloKey,    90, 0);

    ProfilerSnapshot s;
    CHECK(reg.snapshotFor(kConn, s));
    CHECK(s.byMsgTypeExtras.size() == 3u);
    CHECK(s.byMsgTypeExtras[helloKey].sendBytes   == 100u);
    CHECK(s.byMsgTypeExtras[helloKey].recvBytes   == 90u);
    CHECK(s.byMsgTypeExtras[welcomeKey].sendBytes == 120u);
    CHECK(s.byMsgTypeExtras[rejectKey].sendBytes  == 80u);

    // Extras don't pollute the in-game slot table.
    for (uint8_t i = 0; i < kProfilerMsgTypeSlotCount; ++i) {
        CHECK(s.byMsgType[i].sendBytes == 0u);
        CHECK(s.byMsgType[i].recvBytes == 0u);
    }
}

TEST_CASE(PerMsgType_AppAckAndClientInputExtras) {
    ayt::test::setCurrentCase(kCase3);
    ProfilerRegistry reg;
    constexpr uint32_t kConn = 3;

    reg.recordSend(kConn, kMsgTypeAppAck,      33, 0);
    reg.recordSend(kConn, kMsgTypeClientInput, 44, 0);
    reg.recordRecv(kConn, kMsgTypeAppAck,      55, 0);
    reg.recordRecv(kConn, kMsgTypeClientInput, 66, 0);

    ProfilerSnapshot s;
    CHECK(reg.snapshotFor(kConn, s));

    // Both msgTypes are extras (not in slot table).
    CHECK(s.byMsgTypeExtras.count(kMsgTypeAppAck)      == 1u);
    CHECK(s.byMsgTypeExtras.count(kMsgTypeClientInput) == 1u);
    CHECK(s.byMsgTypeExtras[kMsgTypeAppAck].sendBytes      == 33u);
    CHECK(s.byMsgTypeExtras[kMsgTypeAppAck].recvBytes      == 55u);
    CHECK(s.byMsgTypeExtras[kMsgTypeClientInput].sendBytes == 44u);
    CHECK(s.byMsgTypeExtras[kMsgTypeClientInput].recvBytes == 66u);

    for (uint8_t i = 0; i < kProfilerMsgTypeSlotCount; ++i) {
        CHECK(s.byMsgType[i].sendBytes == 0u);
        CHECK(s.byMsgType[i].recvBytes == 0u);
    }
}

TEST_CASE(PerMsgType_PercentageMath) {
    ayt::test::setCurrentCase(kCase4);
    ProfilerRegistry reg;
    constexpr uint32_t kConn = 4;

    // Total = 1000 send bytes.  Per slot: 600, 200, 100, 100, 0, 0, 0.
    reg.recordSend(kConn, kMsgTypeReplication,    600, 0);   // 60%
    reg.recordSend(kConn, kMsgTypeEntitySpawn,   200, 0);   // 20%
    reg.recordSend(kConn, kMsgTypeDelta,         100, 0);   // 10%
    reg.recordSend(kConn, kMsgTypeRpcRequest,    100, 0);   // 10%

    ProfilerSnapshot s;
    CHECK(reg.snapshotFor(kConn, s));
    CHECK(s.windowTotalSendBytes == 1000u);

    auto pctOf = [&](uint64_t slotBytes) -> double {
        const double total = static_cast<double>(s.windowTotalSendBytes);
        return total > 0.0 ? 100.0 * static_cast<double>(slotBytes) / total : 0.0;
    };
    CHECK(pctOf(s.byMsgType[0].sendBytes) > 59.99);
    CHECK(pctOf(s.byMsgType[0].sendBytes) < 60.01);
    CHECK(pctOf(s.byMsgType[1].sendBytes) > 19.99);
    CHECK(pctOf(s.byMsgType[1].sendBytes) < 20.01);
    CHECK(pctOf(s.byMsgType[3].sendBytes) > 9.99);
    CHECK(pctOf(s.byMsgType[3].sendBytes) < 10.01);
    CHECK(pctOf(s.byMsgType[4].sendBytes) > 9.99);
    CHECK(pctOf(s.byMsgType[4].sendBytes) < 10.01);

    // Sum of percentages = 100% (within floating-point tolerance).
    double sumPct = 0.0;
    for (uint8_t i = 0; i < kProfilerMsgTypeSlotCount; ++i) {
        sumPct += pctOf(s.byMsgType[i].sendBytes);
    }
    CHECK(sumPct > 99.99);
    CHECK(sumPct < 100.01);
}

TEST_CASE(PerMsgType_UnknownMsgTypeFallsThrough) {
    ayt::test::setCurrentCase(kCase5);
    ProfilerRegistry reg;
    constexpr uint32_t kConn = 5;

    // Anything that isn't one of the 7 in-game msgTypes lands in extras.
    // Unknown msgType 0xBEEF doesn't crash; it just gets keyed in extras.
    reg.recordSend(kConn, 0xBEEF, 50, 0);
    reg.recordRecv(kConn, 0xCAFE, 25, 0);

    ProfilerSnapshot s;
    CHECK(reg.snapshotFor(kConn, s));
    CHECK(s.byMsgTypeExtras.count(0xBEEF) == 1u);
    CHECK(s.byMsgTypeExtras.count(0xCAFE) == 1u);
    CHECK(s.byMsgTypeExtras[0xBEEF].sendBytes == 50u);
    CHECK(s.byMsgTypeExtras[0xCAFE].recvBytes == 25u);

    // Slot table untouched.
    for (uint8_t i = 0; i < kProfilerMsgTypeSlotCount; ++i) {
        CHECK(s.byMsgType[i].sendBytes == 0u);
        CHECK(s.byMsgType[i].recvBytes == 0u);
    }
}

TEST_SUITE_END