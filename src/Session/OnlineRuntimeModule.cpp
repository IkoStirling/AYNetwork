#include <AYNetwork/Session/OnlineRuntimeModule.h>

#include <AYNetwork/INetwork.h>
#include <AYNetwork/NetworkRuntimeModule.h>

#include <string>
#include <utility>

namespace ayt::net
{
namespace
{

INetworkSubSystem* findNetwork(
    ayt::module::IModuleContext& context) noexcept
{
    auto* service = context.findServiceAs<ayt::game::ISubSystemModuleService>(
        ayt::game::kSubSystemModuleService);
    if (service == nullptr) {
        return nullptr;
    }
    return dynamic_cast<INetworkSubSystem*>(
        service->findSubSystem("Network"));
}

bool hasValidDependencies(
    const OnlineSubSystemConfig& config,
    const OnlineSubSystemDependencies& dependencies)
{
    const bool hasInjectedServices = dependencies.hasAnyBackendService();
    return config.isValid()
        && (hasInjectedServices == dependencies.hasCompleteBackendServices())
        && (hasInjectedServices || config.isValidForHttp());
}

} // namespace

OnlineRuntimeModule::OnlineRuntimeModule(
    OnlineSubSystemConfig config,
    OnlineSubSystemDependencies dependencies,
    ayt::event::EventBus* eventBus)
    : SubSystemModule(
          ayt::module::ModuleDescriptor{
              .id = std::string(kOnlineRuntimeModuleId),
              .displayName = "AYNetwork Online Services",
              .version = "1.0.0",
              .dependencies = {
                  ayt::module::ModuleDependency::required(
                      std::string(kNetworkRuntimeModuleId))}},
          "Online",
          [config = std::move(config),
           dependencies = std::move(dependencies),
           eventBus](ayt::module::IModuleContext& context) {
              INetworkSubSystem* network = findNetwork(context);
              if (network == nullptr
                  || !hasValidDependencies(config, dependencies)) {
                  return std::unique_ptr<ayt::game::ISubSystem>{};
              }
              return std::unique_ptr<ayt::game::ISubSystem>(
                  createOnlineSubSystem(
                      *network, config, dependencies, eventBus));
          })
{
}

} // namespace ayt::net
