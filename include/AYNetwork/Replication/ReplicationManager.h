#pragma once
// AYNetwork/Replication/AYNetwork/Replication/AYNetwork/Replication/ReplicationManager.h - Replication manager for network sync
//
// P0 audit fix (2026-07-26): the canonical ReplicationManager definition
// lives in interface/AYNetwork/INetwork.h (line ~198) where it is colocated with
// INetworkSubSystem and other engine-facing types. A previous version of
// this header redefined the class with different members (_channel, etc.),
// causing ODR conflicts and silently mismatched linkage.
//
// This header now serves as a thin re-include so existing code that does
// `#include <AYNetwork/Replication/ReplicationManager.h>` keeps working. Any future member
// additions belong in AYNetwork/INetwork.h.

#include <AYNetwork/INetwork.h>