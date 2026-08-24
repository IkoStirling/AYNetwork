// BitStream.cpp - 位流序列化实现

#include <AYNetwork.h>
#include <cstring>
#include <algorithm>

namespace ayt::net
{

BitStream::BitStream()
    : _data(nullptr)
    , _allocatedSize(0)
    , _bitPosition(0)
    , _bitCount(0)
    , _ownsData(true)
{
    _allocatedSize = 4096;
    _data = malloc(_allocatedSize);
}

BitStream::BitStream(void* data, size_t size)
    : _data(data)
    , _allocatedSize(size)
    , _bitPosition(0)
    , _bitCount(size * 8)
    , _ownsData(false)
{
}

BitStream::~BitStream() {
    if (_ownsData && _data) {
        free(_data);
    }
}

void BitStream::writeBits(const void* data, size_t bitCount) {
    if (!data || bitCount == 0) return;

    const uint8_t* src = static_cast<const uint8_t*>(data);
    size_t byteCount = (bitCount + 7) >> 3;

    ensureCapacity(byteCount);

    uint8_t* dest = static_cast<uint8_t*>(_data);
    for (size_t i = 0; i < byteCount; ++i) {
        dest[_bitPosition >> 3] = src[i];
        _bitPosition += 8;
    }
    _bitCount = std::max(_bitCount, _bitPosition);
}

void BitStream::writeByte(uint8_t byte) {
    ensureCapacity(1);

    uint8_t* ptr = static_cast<uint8_t*>(_data);
    ptr[_bitPosition >> 3] = byte;
    _bitPosition += 8;
    _bitCount = std::max(_bitCount, _bitPosition);
}

void BitStream::writeInt(int32_t value, int32_t minValue, int32_t maxValue) {
    // Quantize value into [0, range], then emit `bitsNeeded` low bits as LEB128-style bytes.
    // P0 audit fix (2026-07-26): previous version was a hybrid that computed
    // bitsNeeded for range quantization but then wrote a generic LEB128 (no
    // range awareness). For value=0 in any range, the writer produced zero
    // bytes while the reader expected at least one terminator, causing the
    // decoder to walk past the end of the stream and read uninitialized bytes.
    uint32_t range = static_cast<uint32_t>(maxValue - minValue);
    uint32_t unsignedValue = static_cast<uint32_t>(value - minValue);

    // If value falls outside [min, max] after quantization, clamp. The
    // original tests don't exercise this but it's the only sane behaviour
    // for a future-proof BitStream.
    if (unsignedValue > range) {
        unsignedValue = range;
    }

    // Required bits to represent any value in [0, range]. A range of 0 means
    // the only legal value is minValue; we still need 1 bit to mark "value
    // equals minValue" (decoder interprets 0 as the canonical value).
    uint32_t bitsNeeded = 0;
    if (range > 0) {
        bitsNeeded = 1;
        while ((1u << bitsNeeded) - 1 < range) {
            bitsNeeded++;
        }
    }

    // Emit `bitsNeeded` low bits as 7-bit-packed LE bytes with continuation bit.
    uint32_t remaining = unsignedValue;
    bool first = true;
    while (remaining > 0 || first) {
        uint8_t byte = remaining & 0x7F;
        remaining >>= 7;
        if (remaining > 0) {
            byte |= 0x80;
        }
        writeByte(static_cast<uint8_t>(byte));
        first = false;
    }
}

void BitStream::writeFloat(float value, float minValue, float maxValue) {
    // 归一化到 [0, 1]
    float normalized = (value - minValue) / (maxValue - minValue);
    normalized = std::clamp(normalized, 0.0f, 1.0f);

    // 定点数编码为 16 位
    uint16_t fixed = static_cast<uint16_t>(normalized * 65535.0f);

    writeByte(static_cast<uint8_t>(fixed & 0xFF));
    writeByte(static_cast<uint8_t>((fixed >> 8) & 0xFF));
}

void BitStream::writeString(const char* str) {
    if (!str) {
        writeInt(0, 0, INT32_MAX);
        return;
    }

    size_t len = strlen(str);
    writeInt(static_cast<int32_t>(len), 0, INT32_MAX);

    for (size_t i = 0; i < len; ++i) {
        writeByte(static_cast<uint8_t>(str[i]));
    }
}

void BitStream::readBits(void* data, size_t bitCount) {
    if (!data || bitCount == 0) return;

    uint8_t* dest = static_cast<uint8_t*>(data);
    size_t byteCount = (bitCount + 7) >> 3;

    uint8_t* src = static_cast<uint8_t*>(_data);
    for (size_t i = 0; i < byteCount; ++i) {
        dest[i] = src[_bitPosition >> 3];
        _bitPosition += 8;
    }
}

uint8_t BitStream::readByte() {
    uint8_t* ptr = static_cast<uint8_t*>(_data);
    uint8_t value = ptr[_bitPosition >> 3];
    _bitPosition += 8;
    return value;
}

int32_t BitStream::readInt(int32_t minValue, int32_t maxValue) {
    uint32_t range = static_cast<uint32_t>(maxValue - minValue);

    // 计算位数
    uint32_t bitsNeeded = 0;
    if (range > 0) {
        bitsNeeded = 1;
        while ((1u << bitsNeeded) - 1 < range) {
            bitsNeeded++;
        }
    }

    // 读取位
    uint32_t result = 0;
    uint32_t shift = 0;
    uint8_t byte;

    do {
        byte = readByte();
        result |= (static_cast<uint32_t>(byte & 0x7F) << shift);
        shift += 7;
    } while ((byte & 0x80) != 0 && shift < 32);

    return static_cast<int32_t>(result) + minValue;
}

float BitStream::readFloat(float minValue, float maxValue) {
    uint16_t fixed = readByte();
    fixed |= (static_cast<uint16_t>(readByte()) << 8);

    float normalized = static_cast<float>(fixed) / 65535.0f;
    return minValue + normalized * (maxValue - minValue);
}

void BitStream::readString(char* out, size_t maxLen) {
    int32_t len = readInt(0, INT32_MAX);

    if (len <= 0 || !out) {
        if (out && maxLen > 0) out[0] = '\0';
        return;
    }

    size_t toRead = std::min(static_cast<size_t>(len), maxLen - 1);
    uint8_t* ptr = static_cast<uint8_t*>(_data);

    for (size_t i = 0; i < toRead; ++i) {
        out[i] = static_cast<char>(ptr[_bitPosition >> 3]);
        _bitPosition += 8;
    }
    out[toRead] = '\0';
}

void BitStream::reset() {
    _bitPosition = 0;
    _bitCount = 0;
}

void BitStream::resetForRead() {
    _bitPosition = 0;
}

void BitStream::setBitPosition(size_t bits) {
    // R5.0 (2026-08-24): allow callers to seek back to a previously
    // captured bit position. Used by the onReceive fallback path that
    // probes both R5.0 (with prefix) and R3.x (no prefix) wire layouts.
    // Clamp to [0, _bitCount] so a buggy caller can't read past the
    // end of the buffer.
    if (bits > _bitCount) bits = _bitCount;
    _bitPosition = bits;
}

void BitStream::ensureCapacity(size_t additionalBytes) {
    if (!_ownsData) return;

    size_t currentBytes = (_bitPosition + 7) >> 3;
    size_t needed = currentBytes + additionalBytes;

    if (needed > _allocatedSize) {
        size_t newSize = std::max(_allocatedSize * 2, needed);
        _data = realloc(_data, newSize);
        _allocatedSize = newSize;
    }
}

} // namespace ayt::net