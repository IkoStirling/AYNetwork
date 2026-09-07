#pragma once

#include <AYGameLoop/SubSystemModule.h>

#include <string_view>

namespace ayt::net
{

inline constexpr std::string_view kNetworkRuntimeModuleId =
    "AYNetwork.Runtime";

// Runtime transport/replication adapter. AYEntity is optional so dedicated
// network tools can use the module without linking the ECS integration layer.
class NetworkRuntimeModule final : public ayt::game::SubSystemModule
{
public:
    NetworkRuntimeModule();
};

} // namespace ayt::net
