#pragma once
// AYNetwork/Profiler/ProfilerMsgType.h - R5.5 (2026-08-25)
// Slot/index helpers between msgType wire constants (kMsgType* in INetwork.h)
// and ProfilerSnapshot's slot table / byMsgTypeExtras map key.

#include <cstdint>
#include <AYNetwork/INetwork.h>

namespace ayt::net::profiler
{

// Slot index for the 7 in-game msgTypes inside ProfilerSnapshot::byMsgType.
// Returns UINT8_MAX if msgType isn't an in-game msgType (AppAck / ClientInput /
// Handshake live in byMsgTypeExtras instead).
inline uint8_t slotForInGameMsgType(uint16_t msgType)
{
    switch (msgType) {
    case kMsgTypeReplication:    return 0;
    case kMsgTypeEntitySpawn:    return 1;
    case kMsgTypeEntityDespawn:  return 2;
    case kMsgTypeDelta:          return 3;
    case kMsgTypeRpcRequest:     return 4;
    case kMsgTypeRpcResponse:    return 5;
    case kMsgTypeRpcReject:      return 6;
    default:                     return UINT8_MAX;
    }
}

// True iff msgType is one of the 7 in-game slots. Inverse of slotForInGameMsgType
// (slot != UINT8_MAX).
inline bool isInGameMsgType(uint16_t msgType)
{
    return slotForInGameMsgType(msgType) != UINT8_MAX;
}

// Key for ProfilerSnapshot::byMsgTypeExtras. For AppAck / ClientInput this is
// just the msgType constant. For Handshake, the top 8 bits are kMsgTypeHandshake
// and the low 8 bits are HandshakeMsgType.
inline uint16_t extrasKey(uint16_t msgType, uint8_t subType = 0)
{
    // Combine into 16-bit key: msgType in top 8, subType in low 8.
    return static_cast<uint16_t>((msgType << 8) | subType);
}

// Convenience: extract msgType and subType from an extras key (inverse of
// extrasKey). Useful for test assertions and the periodic stderr dump.
inline void extrasKeyDecode(uint16_t key, uint16_t& msgTypeOut, uint8_t& subTypeOut)
{
    msgTypeOut = static_cast<uint16_t>((key >> 8) & 0xFF);
    subTypeOut = static_cast<uint8_t>(key & 0xFF);
}

// Build the extras key for a Handshake subType. Centralized so callers don't
// hand-pack the bits.
inline uint16_t handshakeExtrasKey(HandshakeMsgType subType)
{
    return extrasKey(kMsgTypeHandshake, static_cast<uint8_t>(subType));
}

} // namespace ayt::net::profiler