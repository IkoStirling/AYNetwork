// BitStreamTest.cpp - BitStream unit tests

#include <AYNetwork.h>
#include <cstdio>
#include <cstring>

namespace ayt::net
{
namespace test
{

static int s_passed = 0;
static int s_failed = 0;

#define CHECK(cond) \
    do { \
        if (cond) { \
            s_passed++; \
            printf("[PASS] %s\n", #cond); \
        } else { \
            s_failed++; \
            printf("[FAIL] %s\n", #cond); \
        } \
    } while (false)

#define CHECK_EQUAL(a, b) \
    do { \
        if ((a) == (b)) { \
            s_passed++; \
            printf("[PASS] %s == %s (%d == %d)\n", #a, #b, (int)(a), (int)(b)); \
        } else { \
            s_failed++; \
            printf("[FAIL] %s == %s (%d != %d)\n", #a, #b, (int)(a), (int)(b)); \
        } \
    } while (false)

void testBitStreamBasic() {
    printf("\n=== BitStream Basic Tests ===\n");

    BitStream stream;

    // Test writeByte and readByte
    stream.writeByte(0x42);
    stream.resetForRead();
    uint8_t val = stream.readByte();
    CHECK_EQUAL(val, 0x42);

    // Test multiple bytes
    stream.reset();
    stream.writeByte(0x11);
    stream.writeByte(0x22);
    stream.writeByte(0x33);
    stream.resetForRead();
    CHECK_EQUAL(stream.readByte(), 0x11);
    CHECK_EQUAL(stream.readByte(), 0x22);
    CHECK_EQUAL(stream.readByte(), 0x33);
}

void testBitStreamInt() {
    printf("\n=== BitStream Int Tests ===\n");

    BitStream stream;

    // Test small range int
    stream.writeInt(50, 0, 100);
    stream.resetForRead();
    int32_t val = stream.readInt(0, 100);
    CHECK_EQUAL(val, 50);

    // Test boundary
    stream.reset();
    stream.writeInt(0, 0, 100);
    stream.resetForRead();
    CHECK_EQUAL(stream.readInt(0, 100), 0);

    stream.reset();
    stream.writeInt(100, 0, 100);
    stream.resetForRead();
    CHECK_EQUAL(stream.readInt(0, 100), 100);

    // Test negative offset
    stream.reset();
    stream.writeInt(-50, -100, 100);
    stream.resetForRead();
    CHECK_EQUAL(stream.readInt(-100, 100), -50);
}

void testBitStreamFloat() {
    printf("\n=== BitStream Float Tests ===\n");

    BitStream stream;

    // Test float
    stream.writeFloat(0.5f, 0.0f, 1.0f);
    stream.resetForRead();
    float val = stream.readFloat(0.0f, 1.0f);
    CHECK(val >= 0.49f && val <= 0.51f);

    // Test boundary
    stream.reset();
    stream.writeFloat(0.0f, 0.0f, 1.0f);
    stream.resetForRead();
    CHECK(stream.readFloat(0.0f, 1.0f) < 0.01f);

    stream.reset();
    stream.writeFloat(1.0f, 0.0f, 1.0f);
    stream.resetForRead();
    CHECK(stream.readFloat(0.0f, 1.0f) > 0.99f);
}

void testBitStreamString() {
    printf("\n=== BitStream String Tests ===\n");

    BitStream stream;

    // Test string
    const char* testStr = "Hello, World!";
    stream.writeString(testStr);

    char buffer[256] = {0};
    stream.resetForRead();
    stream.readString(buffer, sizeof(buffer));
    CHECK_EQUAL(strcmp(buffer, testStr), 0);

    // Test empty string
    stream.reset();
    stream.writeString("");
    buffer[0] = 0;
    stream.resetForRead();
    stream.readString(buffer, sizeof(buffer));
    CHECK_EQUAL(strcmp(buffer, ""), 0);
}

int runBitStreamTests() {
    printf("\n========== BitStream Unit Tests ==========\n");

    testBitStreamBasic();
    testBitStreamInt();
    testBitStreamFloat();
    testBitStreamString();

    printf("\n========== Results: %d passed, %d failed ==========\n\n", s_passed, s_failed);
    return s_failed;
}

} // namespace test
} // namespace ayt::net

// Standalone test runner
int main() {
    return ayt::net::test::runBitStreamTests();
}