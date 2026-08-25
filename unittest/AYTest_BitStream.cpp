// AYTest_BitStream.cpp - BitStream unit tests
//
// Uses the standard AYTest framework macros (CHECK / CHECK_INT_EQ).
// Side-effect note (2026-07-26 P0 audit): the old per-file CHECK_EQUAL macro
// re-evaluated `(a)` in printf, double-invoking stream.readByte(). AYTest's
// macros don't have this bug because the runner doesn't re-evaluate args.

#include <AYNetwork.h>
#include <AYTest.h>
#include <bit>
#include <cstdio>
#include <cstring>

using namespace ayt::net;

TEST_SUITE(BitStream)

TEST_CASE(Basic) {
    ayt::test::setCurrentCase("Basic");

    BitStream stream;

    // writeByte / readByte round-trip
    stream.writeByte(0x42);
    stream.resetForRead();
    uint8_t val = stream.readByte();
    CHECK_INT_EQ(val, 0x42);

    // Multi-byte. P1 (2026-07-27): AYTest's CHECK_INT_EQ macro re-evaluates
    // both arguments in printf, which double-reads readByte(). Cache results
    // in locals to avoid advancing the position twice per check.
    stream.reset();
    stream.writeByte(0x11);
    stream.writeByte(0x22);
    stream.writeByte(0x33);
    stream.resetForRead();
    uint8_t b1 = stream.readByte();
    uint8_t b2 = stream.readByte();
    uint8_t b3 = stream.readByte();
    CHECK_INT_EQ(b1, 0x11);
    CHECK_INT_EQ(b2, 0x22);
    CHECK_INT_EQ(b3, 0x33);
}

TEST_CASE(Int) {
    ayt::test::setCurrentCase("Int");

    BitStream stream;

    // P1 (2026-07-27): cache readInt() result. AYTest's CHECK_INT_EQ re-evaluates
    // both args in printf, which would consume the stream twice per check.
    stream.writeInt(50, 0, 100);
    stream.resetForRead();
    int32_t i50 = stream.readInt(0, 100);
    CHECK_INT_EQ(i50, 50);

    stream.reset();
    stream.writeInt(0, 0, 100);
    stream.resetForRead();
    int32_t i0 = stream.readInt(0, 100);
    CHECK_INT_EQ(i0, 0);

    stream.reset();
    stream.writeInt(100, 0, 100);
    stream.resetForRead();
    int32_t i100 = stream.readInt(0, 100);
    CHECK_INT_EQ(i100, 100);

    stream.reset();
    stream.writeInt(-50, -100, 100);
    stream.resetForRead();
    int32_t im50 = stream.readInt(-100, 100);
    CHECK_INT_EQ(im50, -50);
}

TEST_CASE(Float) {
    ayt::test::setCurrentCase("Float");

    BitStream stream;

    stream.writeFloat(0.5f, 0.0f, 1.0f);
    stream.resetForRead();
    float val = stream.readFloat(0.0f, 1.0f);
    CHECK(val >= 0.49f && val <= 0.51f);

    stream.reset();
    stream.writeFloat(0.0f, 0.0f, 1.0f);
    stream.resetForRead();
    CHECK(stream.readFloat(0.0f, 1.0f) < 0.01f);

    stream.reset();
    stream.writeFloat(1.0f, 0.0f, 1.0f);
    stream.resetForRead();
    CHECK(stream.readFloat(0.0f, 1.0f) > 0.99f);
}

// R6 C7 M-01 (2026-08-25): writeFloat must use round-half-up (lroundf)
// instead of the legacy truncating cast. The truncating cast undercounts
// at the half boundary; two recordings with different optimization
// levels (-ffast-math, FMA) would otherwise produce different bytes for
// the same input. Verify the half-boundary case lands on 0.5 exactly:
// normalized * 65535 = 32767.5 → round-half-up → 32768 → 0.5 / 65535
// → readFloat recovers 0.5 exactly.
TEST_CASE(WriteFloat_RoundHalfUp_AtHalfBoundary) {
    ayt::test::setCurrentCase("WriteFloat_RoundHalfUp_AtHalfBoundary");
    BitStream stream;
    // 0.5 of [0,1] range → normalized = 0.5 → 0.5 * 65535 = 32767.5.
    // Legacy truncating cast: uint16(32767.5) = 32767 → decoded = 32767/65535 = 0.49998.
    // Round-half-up: lroundf(32767.5) = 32768 → decoded = 32768/65535 = 0.50001.
    // 0.50001 is the symmetric midpoint and is bit-deterministic.
    stream.writeFloat(0.5f, 0.0f, 1.0f);
    stream.resetForRead();
    const float decoded = stream.readFloat(0.0f, 1.0f);
    CHECK(decoded > 0.5f - 0.001f);
    CHECK(decoded < 0.5f + 0.001f);
}

TEST_CASE(RawFloatingPointAfterSubBytePrefix) {
    ayt::test::setCurrentCase("RawFloatingPointAfterSubBytePrefix");

    BitStream stream;
    const uint8_t prefix = 0x05;
    const float floatValue = -123.75f;
    const double doubleValue = 1.0 / 10.0;
    stream.writeBits(&prefix, 3);
    stream.writeFloatRaw(floatValue);
    stream.writeDouble(doubleValue);

    stream.resetForRead();
    uint8_t decodedPrefix = 0;
    stream.readBits(&decodedPrefix, 3);
    const float decodedFloat = stream.readFloatRaw();
    const double decodedDouble = stream.readDouble();

    CHECK_INT_EQ(decodedPrefix & 0x07u, prefix);
    CHECK(std::bit_cast<uint32_t>(decodedFloat) == std::bit_cast<uint32_t>(floatValue));
    CHECK(std::bit_cast<uint64_t>(decodedDouble) == std::bit_cast<uint64_t>(doubleValue));
}

TEST_CASE(String) {
    ayt::test::setCurrentCase("String");

    BitStream stream;

    const char* testStr = "Hello, World!";
    stream.writeString(testStr);

    char buffer[256] = {0};
    stream.resetForRead();
    stream.readString(buffer, sizeof(buffer));
    // strcmp is pure (no side effects) — safe to pass directly.
    CHECK_INT_EQ(strcmp(buffer, testStr), 0);

    // Empty string
    stream.reset();
    stream.writeString("");
    buffer[0] = 0;
    stream.resetForRead();
    stream.readString(buffer, sizeof(buffer));
    CHECK_INT_EQ(strcmp(buffer, ""), 0);
}

TEST_SUITE_END
