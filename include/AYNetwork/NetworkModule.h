#pragma once
// AYNetwork/NetworkModule.h - explicit GameLoop registration (static-lib safe)

#include <AYNetwork/INetwork.h>

#include <memory>

namespace ayt::net
{

// Factory used by AYModule adapters and tests. The caller owns the subsystem
// until it is transferred to GameLoop.
[[nodiscard]] std::unique_ptr<INetworkSubSystem> createNetworkSubSystem();

// Idempotent. Call before GameLoop::preparePlaySession() / run() so
// NetworkSubSystem is initialized with the other subsystems.
void registerNetworkSubSystem();

// Lookup the registered NetworkSubSystem (nullptr if not registered).
INetworkSubSystem* findRegisteredNetworkSubSystem();

} // namespace ayt::net
