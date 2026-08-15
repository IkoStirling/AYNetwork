#pragma once
// AYNetwork/NetworkModule.h - explicit GameLoop registration (static-lib safe)

#include <AYNetwork/INetwork.h>

namespace ayt::net
{

// Idempotent. Call before GameLoop::preparePlaySession() / run() so
// NetworkSubSystem is initialized with the other subsystems.
void registerNetworkSubSystem();

// Lookup the registered NetworkSubSystem (nullptr if not registered).
INetworkSubSystem* findRegisteredNetworkSubSystem();

} // namespace ayt::net
