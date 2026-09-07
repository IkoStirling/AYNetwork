#include <AYNetwork/NetworkModule.h>
#include <AYNetwork/NetworkRuntimeModule.h>
#include <AYNetwork/Session/OnlineFlowRuntimeModule.h>
#include <AYNetwork/Session/OnlineRuntimeModule.h>

#include <AYGameLoop/IGameLoop.h>
#include <AYGameLoop/SubSystemModule.h>
#include <AYModule/ModuleManager.h>
#include <AYModule/ModuleTypes.h>
#include <AYTest.h>

#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace ayt::net::test
{
namespace
{

class ModuleContext final
    : public ayt::module::IModuleContext,
      public ayt::game::ISubSystemModuleService
{
public:
    void* findService(std::string_view key) const noexcept override
    {
        if (key != ayt::game::kSubSystemModuleService) {
            return nullptr;
        }
        return static_cast<ayt::game::ISubSystemModuleService*>(
            const_cast<ModuleContext*>(this));
    }

    ayt::game::ISubSystem* findSubSystem(
        std::string_view name) noexcept override
    {
        const auto found = _systems.find(std::string(name));
        return found == _systems.end() ? nullptr : found->second.get();
    }

    bool installSubSystem(
        std::unique_ptr<ayt::game::ISubSystem> system) override
    {
        if (!system || system->getName() == nullptr) {
            return false;
        }
        const std::string name = system->getName();
        return _systems.emplace(name, std::move(system)).second;
    }

    void uninstallSubSystem(
        std::string_view name,
        ayt::game::ISubSystem* expectedInstance) noexcept override
    {
        const auto found = _systems.find(std::string(name));
        if (found != _systems.end()
            && found->second.get() == expectedInstance) {
            _systems.erase(found);
        }
    }

    std::size_t size() const noexcept { return _systems.size(); }

private:
    std::unordered_map<
        std::string,
        std::unique_ptr<ayt::game::ISubSystem>> _systems;
};

OnlineSubSystemConfig makeModuleOnlineConfig()
{
    OnlineSubSystemConfig config;
    config.localPeerId = PeerId{"module-client"};
    config.backend.serverPort = 443;
    config.backend.useTls = true;
    config.p2p.p2p.localPeerId = config.localPeerId;
    config.sessions.localPeerId = config.localPeerId;
    return config;
}

} // namespace

TEST_SUITE(NetworkRuntimeModuleTests)

TEST_CASE(module_installs_and_withdraws_network_subsystem)
{
    ModuleContext context;
    NetworkRuntimeModule module;

    CHECK(module.descriptor().id == "AYNetwork.Runtime");
    CHECK_TRUE(module.install(context).succeeded());
    CHECK_NOT_NULL(module.installedSubSystem());
    CHECK_NOT_NULL(dynamic_cast<INetworkSubSystem*>(
        module.installedSubSystem()));
    CHECK_TRUE(module.ownsRegistration());

    module.shutdown(context);
    CHECK(module.installedSubSystem() == nullptr);
    CHECK(context.findSubSystem("Network") == nullptr);
}

TEST_CASE(direct_compatibility_registration_can_restart_in_one_process)
{
    auto& loop = ayt::game::IGameLoop::instance();
    loop.unregisterSubSystem("Network");
    CHECK(findRegisteredNetworkSubSystem() == nullptr);

    registerNetworkSubSystem();
    INetworkSubSystem* first = findRegisteredNetworkSubSystem();
    CHECK_NOT_NULL(first);

    registerNetworkSubSystem();
    CHECK(findRegisteredNetworkSubSystem() == first);

    loop.unregisterSubSystem("Network");
    CHECK(findRegisteredNetworkSubSystem() == nullptr);

    registerNetworkSubSystem();
    CHECK_NOT_NULL(findRegisteredNetworkSubSystem());
    loop.unregisterSubSystem("Network");
}

TEST_CASE(optional_online_stack_resolves_and_installs_in_dependency_order)
{
    ModuleContext context;
    ayt::module::ModuleManager modules;

    CHECK_TRUE(modules.emplace<NetworkRuntimeModule>().succeeded());
    CHECK_TRUE(modules.emplace<OnlineRuntimeModule>(
        makeModuleOnlineConfig()).succeeded());
    CHECK_TRUE(modules.emplace<OnlineFlowRuntimeModule>().succeeded());

    CHECK_TRUE(modules.resolve().succeeded());
    const std::vector<ayt::module::ModuleId> expectedOrder = {
        std::string(kNetworkRuntimeModuleId),
        std::string(kOnlineRuntimeModuleId),
        std::string(kOnlineFlowRuntimeModuleId),
    };
    CHECK(modules.orderedModuleIds() == expectedOrder);
    CHECK_TRUE(modules.registerTypes(context).succeeded());
    CHECK_TRUE(modules.install(context).succeeded());

    CHECK_NOT_NULL(dynamic_cast<INetworkSubSystem*>(
        context.findSubSystem("Network")));
    CHECK_NOT_NULL(dynamic_cast<IOnlineSubSystem*>(
        context.findSubSystem("Online")));
    CHECK_NOT_NULL(dynamic_cast<IOnlineFlowSubSystem*>(
        context.findSubSystem("OnlineFlow")));
    CHECK(context.size() == 3);

    modules.shutdown(context);
    CHECK(context.size() == 0);
}

TEST_CASE(online_module_rejects_invalid_backend_configuration)
{
    ModuleContext context;
    ayt::module::ModuleManager modules;
    CHECK_TRUE(modules.emplace<NetworkRuntimeModule>().succeeded());

    OnlineSubSystemConfig invalid = makeModuleOnlineConfig();
    invalid.backend.serverPort = 0;
    CHECK_TRUE(modules.emplace<OnlineRuntimeModule>(
        std::move(invalid)).succeeded());
    CHECK_TRUE(modules.resolve().succeeded());
    CHECK_TRUE(modules.registerTypes(context).succeeded());

    const auto installed = modules.install(context);
    CHECK_FALSE(installed.succeeded());
    CHECK(installed.code()
        == ayt::module::ModuleErrorCode::InstallationFailed);
    CHECK(context.size() == 0);
}

TEST_SUITE_END

} // namespace ayt::net::test
