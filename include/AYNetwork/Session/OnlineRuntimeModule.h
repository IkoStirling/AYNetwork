#pragma once

#include <AYGameLoop/SubSystemModule.h>
#include <AYNetwork/Session/OnlineSubSystem.h>

#include <string_view>

namespace ayt::event
{
class EventBus;
}

namespace ayt::net
{

inline constexpr std::string_view kOnlineRuntimeModuleId =
    "AYNetwork.Online";

// Optional online-services node. The Network subsystem is resolved from the
// active module context during install, so this adapter works with custom
// hosts as well as the process-default GameLoop.
class OnlineRuntimeModule final : public ayt::game::SubSystemModule
{
public:
    OnlineRuntimeModule(
        OnlineSubSystemConfig config,
        OnlineSubSystemDependencies dependencies = {},
        ayt::event::EventBus* eventBus = nullptr);
};

} // namespace ayt::net
