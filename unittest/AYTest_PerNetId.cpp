// AYNetwork/unittest/AYTest_PerNetId.cpp - R5.5 (2026-08-25)
// Per-netId cumulative / current-tick / dirty-field tests. 4 cases:
//   - registerNetId pre-creates rows; recordSend updates cumulative
//   - currentTickSendBytes resets on tickWindow; cumulative is monotonic
//   - unregisterNetId removes the row
//   - Multiple ghosts sorted by cumulativeSendBytes desc

#include <AYTest.h>
#include <AYNetwork/Profiler/ProfilerRegistry.h>
#include <AYNetwork/Profiler/ProfilerSnapshot.h>
#include <AYNetwork/Profiler/ProfilerMsgType.h>
#include <AYNetwork/INetwork.h>

using namespace ayt::net;

namespace {
constexpr const char* kCase1 = "PerNetId_RegisterAndRecordSend";
constexpr const char* kCase2 = "PerNetId_CurrentTickResetsMonotonic";
constexpr const char* kCase3 = "PerNetId_UnregisterRemovesRow";
constexpr const char* kCase4 = "PerNetId_SortedByCumulativeDesc";
} // anonymous namespace

TEST_SUITE(PerNetId)

TEST_CASE(PerNetId_RegisterAndRecordSend) {
    ayt::test::setCurrentCase(kCase1);
    ProfilerRegistry reg;
    constexpr uint32_t kConn = 100;

    // Pre-register 3 ghosts.
    reg.registerNetId(kConn, 11);
    reg.registerNetId(kConn, 22);
    reg.registerNetId(kConn, 33);

    // Record 2 sends for ghost 11 (100 bytes each), 1 for ghost 22 (200),
    // 3 for ghost 33 (50 each).
    reg.recordSend(kConn, kMsgTypeReplication, 100, 11);
    reg.recordSend(kConn, kMsgTypeDelta,      100, 11);
    reg.recordSend(kConn, kMsgTypeReplication, 200, 22);
    reg.recordSend(kConn, kMsgTypeReplication,  50, 33);
    reg.recordSend(kConn, kMsgTypeDelta,        50, 33);
    reg.recordSend(kConn, kMsgTypeEntitySpawn,  50, 33);

    ProfilerSnapshot s;
    CHECK(reg.snapshotFor(kConn, s));
    CHECK(s.perNetId.size() == 3u);

    // The snapshot sorts by cumulativeSendBytes desc — but for stable
    // assertion we look up by netId.
    auto findRow = [&s](uint32_t id) -> const NetIdWireCost* {
        for (const auto& r : s.perNetId) {
            if (r.netId == id) return &r;
        }
        return nullptr;
    };
    const auto* r11 = findRow(11); CHECK(r11 != nullptr);
    const auto* r22 = findRow(22); CHECK(r22 != nullptr);
    const auto* r33 = findRow(33); CHECK(r33 != nullptr);

    CHECK(r11->cumulativeSendBytes == 200u);
    CHECK(r11->currentTickSendBytes == 200u);
    CHECK(r22->cumulativeSendBytes == 200u);
    CHECK(r22->currentTickSendBytes == 200u);
    CHECK(r33->cumulativeSendBytes == 150u);
    CHECK(r33->currentTickSendBytes == 150u);
}

TEST_CASE(PerNetId_CurrentTickResetsMonotonic) {
    ayt::test::setCurrentCase(kCase2);
    ProfilerRegistry reg;
    constexpr uint32_t kConn = 101;

    reg.registerNetId(kConn, 7);
    reg.recordSend(kConn, kMsgTypeReplication, 100, 7);
    reg.recordSend(kConn, kMsgTypeDelta,       50, 7);

    ProfilerSnapshot a;
    CHECK(reg.snapshotFor(kConn, a));
    CHECK(a.perNetId[0].cumulativeSendBytes == 150u);
    CHECK(a.perNetId[0].currentTickSendBytes == 150u);

    // Tick boundary — current-tick resets, cumulative untouched.
    reg.tickWindow(/*serverTick=*/ 1);
    ProfilerSnapshot b;
    CHECK(reg.snapshotFor(kConn, b));
    CHECK(b.perNetId[0].cumulativeSendBytes == 150u);
    CHECK(b.perNetId[0].currentTickSendBytes == 0u);

    // New tick — current-tick starts accumulating again.
    reg.recordSend(kConn, kMsgTypeDelta, 75, 7);
    ProfilerSnapshot c;
    CHECK(reg.snapshotFor(kConn, c));
    CHECK(c.perNetId[0].cumulativeSendBytes == 225u);   // 150 + 75
    CHECK(c.perNetId[0].currentTickSendBytes == 75u);
}

TEST_CASE(PerNetId_UnregisterRemovesRow) {
    ayt::test::setCurrentCase(kCase3);
    ProfilerRegistry reg;
    constexpr uint32_t kConn = 102;

    reg.registerNetId(kConn, 5);
    reg.registerNetId(kConn, 6);
    reg.recordSend(kConn, kMsgTypeReplication, 100, 5);
    reg.recordSend(kConn, kMsgTypeReplication, 200, 6);

    ProfilerSnapshot before;
    CHECK(reg.snapshotFor(kConn, before));
    CHECK(before.perNetId.size() == 2u);

    reg.unregisterNetId(kConn, 5);
    ProfilerSnapshot after;
    CHECK(reg.snapshotFor(kConn, after));
    CHECK(after.perNetId.size() == 1u);
    CHECK(after.perNetId[0].netId == 6u);
    CHECK(after.perNetId[0].cumulativeSendBytes == 200u);

    // Unregister unknown netId is a silent no-op.
    reg.unregisterNetId(kConn, 9999);
    ProfilerSnapshot stillOne;
    CHECK(reg.snapshotFor(kConn, stillOne));
    CHECK(stillOne.perNetId.size() == 1u);
}

TEST_CASE(PerNetId_SortedByCumulativeDesc) {
    ayt::test::setCurrentCase(kCase4);
    ProfilerRegistry reg;
    constexpr uint32_t kConn = 103;

    // Cumulative values: g1=300, g2=100, g3=200 → expected order: g1, g3, g2.
    reg.recordSend(kConn, kMsgTypeReplication, 300, /*ghostNetId=*/ 1);
    reg.recordSend(kConn, kMsgTypeReplication, 100, /*ghostNetId=*/ 2);
    reg.recordSend(kConn, kMsgTypeReplication, 200, /*ghostNetId=*/ 3);

    ProfilerSnapshot s;
    CHECK(reg.snapshotFor(kConn, s));
    CHECK(s.perNetId.size() == 3u);
    CHECK(s.perNetId[0].netId == 1u);
    CHECK(s.perNetId[1].netId == 3u);
    CHECK(s.perNetId[2].netId == 2u);
    CHECK(s.perNetId[0].cumulativeSendBytes >= s.perNetId[1].cumulativeSendBytes);
    CHECK(s.perNetId[1].cumulativeSendBytes >= s.perNetId[2].cumulativeSendBytes);
}

TEST_SUITE_END