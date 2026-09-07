#include <AYNetwork/NetworkRuntimeModule.h>

#include <AYNetwork/NetworkModule.h>

#include <string>

namespace ayt::net
{

NetworkRuntimeModule::NetworkRuntimeModule()
    : SubSystemModule(
          ayt::module::ModuleDescriptor{
              .id = std::string(kNetworkRuntimeModuleId),
              .displayName = "AYNetwork Runtime",
              .version = "1.0.0",
              .dependencies = {
                  ayt::module::ModuleDependency::optional(
                      "AYEntity.Runtime")}},
          "Network",
          []() {
              return createNetworkSubSystem();
          })
{
}

} // namespace ayt::net
