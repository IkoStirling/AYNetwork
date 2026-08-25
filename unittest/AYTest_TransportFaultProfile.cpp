// AYNetwork/unittest/AYTest_TransportFaultProfile.cpp - R5.4 (2026-08-25)
// TransportFaultProfile struct tests. 5 cases: defaults, channel mask bits,
// affectsChannel, isNoOp behavior, RNG determinism.

#include <AYTest.h>
#include <AYNetwork/INetwork.h>
#include <AYNetwork/TransportFaultProfile.h>
#include "TransportFaultController.h"

#include <cstring>

using namespace ayt::net;

namespace
{
constexpr const char* kCase1 = "Profile_DefaultsAreNoOp";
constexpr const char* kCase2 = "Profile_ChannelMaskBits";
constexpr const char* kCase3 = "Profile_AffectsChannel";
constexpr const char* kCase4 = "Profile_IsNoOpVariants";
constexpr const char* kCase5 = "Profile_RngDeterminism";
} // anonymous namespace

TEST_SUITE(TransportFaultProfile)

TEST_CASE(Profile_DefaultsAreNoOp)
{
    ayt::test::setCurrentCase(kCase1);
    TransportFaultProfile p;
    CHECK(p.randomSeed == 0u);
    CHECK(p.channelMask == kFaultChannelAll);
    CHECK(p.latencyMeanMs == 0u);
    CHECK(p.latencyJitterMs == 0u);
    CHECK(p.lossPercent == 0.0f);
    CHECK(p.dupPercent == 0.0f);
    CHECK(p.reorderPercent == 0.0f);
    CHECK(p.rateLimitBytesPerSec == 0u);
    CHECK(p.rateLimitBurstBytes == 0u);
    CHECK(p.isNoOp());
}

TEST_CASE(Profile_ChannelMaskBits)
{
    ayt::test::setCurrentCase(kCase2);
    // Each bit index maps to a runtime channel.
    CHECK(kFaultChannelReliable   == (1u << 0));
    CHECK(kFaultChannelUnreliable == (1u << 1));
    CHECK(kFaultChannelFragmented == (1u << 2));
    CHECK(kFaultChannelAck        == (1u << 3));
    CHECK(kFaultChannelAll        == 0x0Fu);
    // Reserved bits must NOT overlap with used bits.
    CHECK((kFaultChannelReserved & kFaultChannelAll) == 0u);
}

TEST_CASE(Profile_AffectsChannel)
{
    ayt::test::setCurrentCase(kCase3);
    TransportFaultProfile p;
    p.channelMask = kFaultChannelReliable;
    CHECK(p.affectsChannel(CHANNEL_RELIABLE));
    CHECK(!p.affectsChannel(CHANNEL_UNRELIABLE));
    CHECK(!p.affectsChannel(CHANNEL_FRAGMENTED));
    CHECK(!p.affectsChannel(CHANNEL_ACK));

    p.channelMask = kFaultChannelAll;
    CHECK(p.affectsChannel(CHANNEL_RELIABLE));
    CHECK(p.affectsChannel(CHANNEL_UNRELIABLE));
    CHECK(p.affectsChannel(CHANNEL_FRAGMENTED));
    CHECK(p.affectsChannel(CHANNEL_ACK));

    // Out-of-range channel always returns false.
    CHECK(!p.affectsChannel(7));
}

TEST_CASE(Profile_IsNoOpVariants)
{
    ayt::test::setCurrentCase(kCase4);
    // Empty mask + zero knobs = no-op.
    TransportFaultProfile a;
    CHECK(a.isNoOp());

    // Non-zero latency alone = not a no-op.
    TransportFaultProfile b;
    b.latencyMeanMs = 100;
    CHECK(!b.isNoOp());

    // Loss alone = not a no-op.
    TransportFaultProfile c;
    c.lossPercent = 5.0f;
    CHECK(!c.isNoOp());

    // Rate limit alone = not a no-op.
    TransportFaultProfile d;
    d.rateLimitBytesPerSec = 1000;
    CHECK(!d.isNoOp());

    // Burst alone is not enough (burst without rate == disabled in tick).
    TransportFaultProfile e;
    e.rateLimitBurstBytes = 4096;
    CHECK(e.isNoOp());

    // mask=0 + non-zero knobs: isNoOp() returns false because knobs are
    // non-zero, BUT the interceptor will pass every channel through
    // because `affectsChannel` returns false for all of them. isNoOp()
    // intentionally only checks the numeric knobs.
    TransportFaultProfile g;
    g.channelMask = 0;
    g.lossPercent = 100.0f;
    CHECK(!g.isNoOp());
    CHECK(!g.affectsChannel(0));
    CHECK(!g.affectsChannel(1));
    CHECK(!g.affectsChannel(2));
    CHECK(!g.affectsChannel(3));
}

TEST_CASE(Profile_RngDeterminism)
{
    ayt::test::setCurrentCase(kCase5);
    // Same seed → same draw sequence. We can't test loss/dup/reorder
    // here (those are interceptor tests) but we can verify that the
    // controller creates a stable RNG per netId when seeded.
    TransportFaultController ctl;
    TransportFaultProfile p;
    p.randomSeed = 0xDEADBEEFULL;
    ctl.setProfile(/*netId=*/42, p);

    auto& rng1 = ctl.rngFor(42);
    const uint64_t a = rng1();
    const uint64_t b = rng1();
    CHECK(a != 0u);
    CHECK(b != a);

    // Reset profile (re-seats the RNG) and replay the same two draws.
    ctl.setProfile(/*netId=*/42, p);
    auto& rng2 = ctl.rngFor(42);
    CHECK(rng2() == a);
    CHECK(rng2() == b);
}

// R6 C4 (2026-08-25): B-09 finding. TransportFaultController::setSessionSeed
// drives the RNG fallback when a profile has randomSeed == 0. Two
// controllers seeded identically produce identical draw sequences — the
// determinism contract required for replay equality.
TEST_CASE(Controller_SessionSeed_ReproducesDraws)
{
    ayt::test::setCurrentCase("Controller_SessionSeed_ReproducesDraws");

    TransportFaultController a;
    TransportFaultController b;
    a.setSessionSeed(0xA1B2C3D4E5F6ULL);
    b.setSessionSeed(0xA1B2C3D4E5F6ULL);

    TransportFaultProfile p;  // randomSeed == 0 → use session seed
    a.setProfile(7, p);
    b.setProfile(7, p);

    auto& ra = a.rngFor(7);
    auto& rb = b.rngFor(7);
    for (int i = 0; i < 8; ++i) {
        CHECK(ra() == rb());
    }

    // Different session seed → different draw sequence.
    TransportFaultController c;
    c.setSessionSeed(0x112233445566ULL);
    TransportFaultProfile p2;
    c.setProfile(7, p2);
    auto& rc = c.rngFor(7);
    CHECK(rc() != ra());
}

TEST_CASE(Controller_DefaultSessionSeedIsStable)
{
    ayt::test::setCurrentCase("Controller_DefaultSessionSeedIsStable");
    // No setSessionSeed call → kDefaultSessionSeed (0xC0FFEE) drives
    // the fallback path. Two default-constructed controllers must
    // produce identical draws.
    TransportFaultController a;
    TransportFaultController b;
    TransportFaultProfile p;
    a.setProfile(11, p);
    b.setProfile(11, p);
    auto& ra = a.rngFor(11);
    auto& rb = b.rngFor(11);
    for (int i = 0; i < 4; ++i) {
        CHECK(ra() == rb());
    }
}

TEST_SUITE_END
