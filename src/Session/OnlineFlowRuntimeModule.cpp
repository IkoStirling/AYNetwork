#include <AYNetwork/Session/OnlineFlowRuntimeModule.h>

#include <AYNetwork/Session/OnlineRuntimeModule.h>

#include <string>
#include <utility>

namespace ayt::net
{
namespace
{

IOnlineSubSystem* findOnline(
    ayt::module::IModuleContext& context) noexcept
{
    auto* service = context.findServiceAs<ayt::game::ISubSystemModuleService>(
        ayt::game::kSubSystemModuleService);
    if (service == nullptr) {
        return nullptr;
    }
    return dynamic_cast<IOnlineSubSystem*>(
        service->findSubSystem("Online"));
}

} // namespace

OnlineFlowRuntimeModule::OnlineFlowRuntimeModule(
    OnlineFlowConfig config,
    ayt::event::EventBus* eventBus)
    : SubSystemModule(
          ayt::module::ModuleDescriptor{
              .id = std::string(kOnlineFlowRuntimeModuleId),
              .displayName = "AYNetwork Online Flow",
              .version = "1.0.0",
              .dependencies = {
                  ayt::module::ModuleDependency::required(
                      std::string(kOnlineRuntimeModuleId))}},
          "OnlineFlow",
          [config = std::move(config), eventBus](
              ayt::module::IModuleContext& context) {
              IOnlineSubSystem* online = findOnline(context);
              if (online == nullptr || !config.isValid()) {
                  return std::unique_ptr<ayt::game::ISubSystem>{};
              }
              return std::unique_ptr<ayt::game::ISubSystem>(
                  createOnlineFlowSubSystem(
                      *online, config, eventBus));
          })
{
}

} // namespace ayt::net
