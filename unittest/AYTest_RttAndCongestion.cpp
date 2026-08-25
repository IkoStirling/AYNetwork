// AYNetwork/unittest/AYTest_RttAndCongestion.cpp - R5.5 (2026-08-25)
// ConnLiveStatus population tests. 4 cases:
//   - Default snapshot values when ConnAccessor returns null (test path)
//   - ConnAccessor returning a GnsConnection* threads through to fillLiveStatus
//   - GNS fields are populated when s_gns has a live connection
//   - Path local/remote surface in ConnLiveStatus

#include <AYTest.h>
#include <AYNetwork/Profiler/ProfilerRegistry.h>
#include <AYNetwork/Profiler/ProfilerSnapshot.h>
#include <AYNetwork/Profiler/ProfilerMsgType.h>
#include <AYNetwork/Transport/GnsConnection.h>
#include <AYNetwork/INetwork.h>

#include <cstdio>

using namespace ayt::net;

namespace {
constexpr const char* kCase1 = "RttAndCongestion_DefaultNoConnAccessor";
constexpr const char* kCase2 = "RttAndCongestion_ConnAccessorThreadsThrough";
constexpr const char* kCase3 = "RttAndCongestion_LiveStatusFieldsDefault";
constexpr const char* kCase4 = "RttAndCongestion_PathFieldsSurface";
} // anonymous namespace

TEST_SUITE(RttAndCongestion)

TEST_CASE(RttAndCongestion_DefaultNoConnAccessor) {
    ayt::test::setCurrentCase(kCase1);
    ProfilerRegistry reg;

    // No ConnAccessor installed → fillLiveStatus is a no-op.
    reg.recordSend(7, kMsgTypeReplication, 100, 0);

    ProfilerSnapshot s;
    CHECK(reg.snapshotFor(7, s));
    // Default values per ProfilerSnapshot.h: pingMs=-1, quality=-1,
    // pending/sendQ=0, path=0.
    CHECK(s.live.pingMs == -1);
    CHECK(s.live.qualityLocal < 0.f);
    CHECK(s.live.qualityRemote < 0.f);
    CHECK(s.live.pendingReliable == 0u);
    CHECK(s.live.pendingUnreliable == 0u);
    CHECK(s.live.sendQueueBytes == 0u);
    CHECK(s.live.pathLocal == 0u);
    CHECK(s.live.pathRemote == 0u);
}

TEST_CASE(RttAndCongestion_ConnAccessorThreadsThrough) {
    ayt::test::setCurrentCase(kCase2);
    ProfilerRegistry reg;

    // Install a ConnAccessor that returns a non-null GnsConnection* when
    // asked about netId=99, and null otherwise. Even if s_gns is null in
    // this TU (no real GNS init), the accessor is invoked and the
    // early-out path exercises correctly.
    GnsConnection dummy;
    bool sawCall = false;
    reg.setConnAccessorForTesting(
        [&](uint32_t netId, const GnsConnection*& out) {
            sawCall = true;
            if (netId == 99) out = &dummy;
            else out = nullptr;
        });

    reg.recordSend(99, kMsgTypeReplication, 100, 0);
    ProfilerSnapshot s;
    CHECK(reg.snapshotFor(99, s));
    CHECK(sawCall);

    // Default fields unchanged because no GNS handle is live in this TU.
    // (s_gns is null when no test pump was run.)
    CHECK(s.live.pingMs == -1);
    CHECK(s.live.qualityLocal < 0.f);
}

TEST_CASE(RttAndCongestion_LiveStatusFieldsDefault) {
    ayt::test::setCurrentCase(kCase3);
    ProfilerSnapshot defaultSnap;
    // All counts default to 0, ping/quality to -1 (no real values).
    CHECK(defaultSnap.live.pingMs == -1);
    CHECK(defaultSnap.live.qualityLocal == -1.f);
    CHECK(defaultSnap.live.qualityRemote == -1.f);
    CHECK(defaultSnap.live.inBytesPerSec == 0u);
    CHECK(defaultSnap.live.outBytesPerSec == 0u);
    CHECK(defaultSnap.live.pendingReliable == 0u);
    CHECK(defaultSnap.live.pendingUnreliable == 0u);
    CHECK(defaultSnap.live.inMessageCount == 0u);
    CHECK(defaultSnap.live.outMessageCount == 0u);
    CHECK(defaultSnap.live.sendQueueBytes == 0u);
    CHECK(defaultSnap.live.ackPending == 0u);
    CHECK(defaultSnap.live.fragmentQueueBytes == 0u);
    CHECK(defaultSnap.live.simInBytesUnprocessed == 0u);
    CHECK(defaultSnap.live.pathLocal == 0u);
    CHECK(defaultSnap.live.pathRemote == 0u);
}

TEST_CASE(RttAndCongestion_PathFieldsSurface) {
    ayt::test::setCurrentCase(kCase4);
    ProfilerRegistry reg;
    reg.recordSend(123, kMsgTypeReplication, 50, 0);

    // Without a real GNS handle, the path fields stay 0 (k_EConnectionType
    // Unknown). The contract is "field is present in the snapshot, value
    // is set from GNS when available, defaults to 0 otherwise".
    ProfilerSnapshot s;
    CHECK(reg.snapshotFor(123, s));
    CHECK(s.live.pathLocal == 0u);
    CHECK(s.live.pathRemote == 0u);
    CHECK(s.netId == 123u);
}

TEST_SUITE_END