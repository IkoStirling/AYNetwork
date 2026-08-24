// AYNetwork/unittest/AYTest_TokenBucket.cpp - R5.4 (2026-08-25)
// TokenBucket header-only primitive tests. 4 cases: configuration, rate
// exhaustion + recovery, burst capacity, zero-rate disabled.

#include <AYTest.h>
#include "TokenBucket.h"

using namespace ayt::net;

namespace
{
constexpr const char* kCase1 = "TokenBucket_ConfigureDefaults";
constexpr const char* kCase2 = "TokenBucket_RateExhaustionAndRecovery";
constexpr const char* kCase3 = "TokenBucket_BurstCapacity";
constexpr const char* kCase4 = "TokenBucket_ZeroRateDisabled";
} // anonymous namespace

TEST_SUITE(TokenBucket)

TEST_CASE(TokenBucket_ConfigureDefaults)
{
    ayt::test::setCurrentCase(kCase1);
    TokenBucket b;
    // Default state = disabled (rate=0).
    CHECK(b.availableTokens() == 0u);
    CHECK(b.tryConsume(1024, 0.001));
    CHECK(b.tryConsume(UINT32_MAX, 0.0));

    b.configure(/*rate=*/0, /*burst=*/0);
    CHECK(b.availableTokens() == 0u);
    CHECK(b.tryConsume(1024, 1.0));
}

TEST_CASE(TokenBucket_RateExhaustionAndRecovery)
{
    ayt::test::setCurrentCase(kCase2);
    // 1000 bytes/sec, burst 1000 bytes. Token bucket starts at 0 (no
    // pre-fill on configure — tokens accrue only via dt).
    TokenBucket b;
    b.configure(/*rate=*/1000, /*burst=*/1000);

    // Initial state: empty (not pre-filled).
    CHECK(b.availableTokens() == 0u);

    // After dt=1.0 sec at 1000 bytes/sec, bucket fills to burst capacity.
    CHECK(b.tryConsume(1000, /*dt=*/1.0));
    CHECK(b.availableTokens() == 0u);

    // No more time elapsed → can't consume again.
    CHECK(!b.tryConsume(1, 0.0));
    CHECK(b.availableTokens() == 0u);

    // Wait 10 ms → adds 10 bytes; consume them.
    CHECK(b.tryConsume(10, 0.010));
    CHECK(b.availableTokens() == 0u);

    // Wait 100 ms → adds 100 bytes; consume 50 (50 left, then try 51 → fail).
    CHECK(b.tryConsume(50, 0.100));
    CHECK(b.availableTokens() == 50u);
    CHECK(!b.tryConsume(51, 0.0));  // only 50 left; need 51
    CHECK(b.availableTokens() == 50u);

    // After another 1 sec, bucket is fully refilled (50 + 1000 = 1050 → clamped to 1000).
    CHECK(b.tryConsume(1000, 1.0));
    CHECK(b.availableTokens() == 0u);
}

TEST_CASE(TokenBucket_BurstCapacity)
{
    ayt::test::setCurrentCase(kCase3);
    // Tiny rate, big burst: tokens accrue over time up to burst cap.
    TokenBucket b;
    b.configure(/*rate=*/100, /*burst=*/2000);

    // Start at 0.
    CHECK(b.availableTokens() == 0u);

    // After 20 s, 100 * 20 = 2000 tokens; consume them all.
    CHECK(b.tryConsume(2000, /*dt=*/20.0));
    CHECK(b.availableTokens() == 0u);

    // 1 ms later (dt=0.001) → 0.1 byte added; can't consume 1.
    CHECK(!b.tryConsume(1, 0.001));

    // After another 20 s, full burst again.
    CHECK(b.tryConsume(2000, 20.0));
    CHECK(b.availableTokens() == 0u);

    // Reconfigure with smaller burst; tokens clamped down.
    b.configure(/*rate=*/1000000, /*burst=*/500);
    // After a long dt, tokens are clamped to 500.
    CHECK(b.tryConsume(500, /*dt=*/10.0));
    CHECK(b.availableTokens() == 0u);
}

TEST_CASE(TokenBucket_ZeroRateDisabled)
{
    ayt::test::setCurrentCase(kCase4);
    // Zero rate = bucket disabled regardless of burst. Always returns true.
    TokenBucket b;
    b.configure(/*rate=*/0, /*burst=*/99999);

    // Disabled bucket reports burst capacity (the spec said "returns burst
    // when disabled"), and every tryConsume returns true.
    CHECK(b.availableTokens() == 99999u);
    for (int i = 0; i < 10; ++i) {
        CHECK(b.tryConsume(99999, 0.001));
    }
    // availableTokens is unchanged because disabled mode skips refill/deduct.
    CHECK(b.availableTokens() == 99999u);
}

TEST_SUITE_END
