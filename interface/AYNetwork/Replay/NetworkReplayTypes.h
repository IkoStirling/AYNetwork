// AYNetwork/Replay/NetworkReplayTypes.h - R5.3 (2026-08-24) event IDs.
//
// Network adapter reserves the [0x10000, 0x1FFFF] event-type range.
// Future adapters (AYEntity, AYEditor) pick adjacent ranges so event-type
// collision is impossible by construction.

#pragma once
#include <cstdint>

namespace ayt::net::replay
{

// Range bounds (informational; the foundation reserves [0, 0xFFFF] for
// itself and allocates [0x10000, 0xFFFFFFFF] to consumers).
constexpr uint32_t kEvtNet_RangeFirst = 0x10000u;
constexpr uint32_t kEvtNet_RangeLast  = 0x1FFFFu;

constexpr uint32_t kEvtNet_InitialFullSnapshot = 0x10001u;
constexpr uint32_t kEvtNet_Spawn               = 0x10002u;
constexpr uint32_t kEvtNet_Despawn             = 0x10003u;
constexpr uint32_t kEvtNet_DeltaSnapshot       = 0x10004u;
constexpr uint32_t kEvtNet_InputBatch          = 0x10005u;
constexpr uint32_t kEvtNet_RpcBatch            = 0x10006u;
constexpr uint32_t kEvtNet_AuthorityChange     = 0x10007u;

} // namespace ayt::net::replay
