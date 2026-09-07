#pragma once

#include <AYGameLoop/SubSystemModule.h>
#include <AYNetwork/Session/OnlineFlowSubSystem.h>

#include <string_view>

namespace ayt::event
{
class EventBus;
}

namespace ayt::net
{

inline constexpr std::string_view kOnlineFlowRuntimeModuleId =
    "AYNetwork.OnlineFlow";

// Optional application-flow node. Online is located through the active
// module context and remains owned by GameLoop.
class OnlineFlowRuntimeModule final : public ayt::game::SubSystemModule
{
public:
    explicit OnlineFlowRuntimeModule(
        OnlineFlowConfig config = {},
        ayt::event::EventBus* eventBus = nullptr);
};

} // namespace ayt::net
