#include <AYNetwork/Session/OnlineFlowSubSystem.h>

#include <AYEventSystem/EventBus.h>
#include <AYGameLoop/SubSystemRegistry.h>

#include <utility>

namespace ayt::net
{
namespace
{

class OnlineFlowSubSystem final : public IOnlineFlowSubSystem {
public:
    OnlineFlowSubSystem(IOnlineSubSystem& online,
                        OnlineFlowConfig config,
                        ::ayt::event::EventBus* eventBus)
        : _online(online),
          _config(std::move(config)),
          _eventBus(eventBus ? eventBus
                             : &::ayt::event::EventBus::instance()) {}

    const char* getName() const override { return "OnlineFlow"; }

    const ::ayt::game::SubSystemDescriptor& getDescriptor() const override {
        static ::ayt::game::SubSystemDescriptor descriptor = {
            .name = "OnlineFlow",
            .dependencies = {},
            .basePriority = 100,
            .timeType = ::ayt::game::SubSystemDescriptor::TimeType::Real,
            .phases = ::ayt::game::phaseBit(::ayt::game::FramePhase::Ingress),
            .clock = ::ayt::game::ClockDomain::RealWall,
            .initializeAfter = {"Online"},
            .runsAfter = {"Online"},
            .phasePriority = 100,
            .reads = {"Online.SessionState"},
            .writes = {"Application.OnlineFlow"},
        };
        return descriptor;
    }

    bool initialize() override {
        if (_flow) return true;
        if (!_online.isReady() || !_config.isValid()) return false;
        _flow = std::make_unique<OnlineFlowCoordinator>(
            _online, _config, _eventBus);
        return _flow->getStatus().state != OnlineFlowState::Failed;
    }

    void update(float) override {
        if (_flow) _flow->update();
    }

    void fixedUpdate(float) override {}

    void shutdown() override { _flow.reset(); }

    bool isReady() const override { return _flow != nullptr; }
    OnlineFlowCoordinator* coordinator() override { return _flow.get(); }
    const OnlineFlowCoordinator* coordinator() const override {
        return _flow.get();
    }

private:
    IOnlineSubSystem& _online;
    OnlineFlowConfig _config;
    ::ayt::event::EventBus* _eventBus = nullptr;
    std::unique_ptr<OnlineFlowCoordinator> _flow;
};

} // namespace

std::unique_ptr<IOnlineFlowSubSystem> createOnlineFlowSubSystem(
    IOnlineSubSystem& online,
    OnlineFlowConfig config,
    ::ayt::event::EventBus* eventBus) {
    return std::make_unique<OnlineFlowSubSystem>(
        online, std::move(config), eventBus);
}

IOnlineFlowSubSystem* findRegisteredOnlineFlowSubSystem() {
    auto* system = ::ayt::game::SubSystemRegistry::instance().findSubSystem(
        "OnlineFlow");
    return dynamic_cast<IOnlineFlowSubSystem*>(system);
}

bool registerOnlineFlowSubSystem(OnlineFlowConfig config,
                                 ::ayt::event::EventBus* eventBus) {
    if (findRegisteredOnlineFlowSubSystem()) return true;
    auto* online = findRegisteredOnlineSubSystem();
    if (!online || !config.isValid()) return false;
    auto system = createOnlineFlowSubSystem(
        *online, std::move(config), eventBus);
    ::ayt::game::IGameLoop::instance().registerSubSystem(system.release());
    return findRegisteredOnlineFlowSubSystem() != nullptr;
}

} // namespace ayt::net
