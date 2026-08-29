#pragma once
// GameLoop-owned application flow, scheduled immediately after Online.

#include <AYNetwork/Session/OnlineFlowCoordinator.h>

#include <memory>

namespace ayt::event { class EventBus; }

namespace ayt::net
{

class IOnlineFlowSubSystem : public ::ayt::game::ISubSystem {
public:
    ~IOnlineFlowSubSystem() override = default;

    virtual bool isReady() const = 0;
    virtual OnlineFlowCoordinator* coordinator() = 0;
    virtual const OnlineFlowCoordinator* coordinator() const = 0;
};

std::unique_ptr<IOnlineFlowSubSystem> createOnlineFlowSubSystem(
    IOnlineSubSystem& online,
    OnlineFlowConfig config = {},
    ::ayt::event::EventBus* eventBus = nullptr);

// Online must already be registered because its backend/application config is
// project-owned. Registration is idempotent.
bool registerOnlineFlowSubSystem(
    OnlineFlowConfig config = {},
    ::ayt::event::EventBus* eventBus = nullptr);
IOnlineFlowSubSystem* findRegisteredOnlineFlowSubSystem();

} // namespace ayt::net
