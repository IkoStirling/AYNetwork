#pragma once
// Headless Dedicated Server lifecycle and concrete client connector.

#include <AYNetwork/INetwork.h>
#include <AYNetwork/OnlineServices.h>
#include <AYNetwork/Session/OnlineSessionCoordinator.h>

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace ayt::net
{

struct DedicatedAdmission {
    DedicatedAllocationId allocationId = 0;
    PeerId peerId;
    std::string reservationToken;

    bool isValid() const {
        return allocationId != 0 && peerId.isValid() &&
               reservationToken.size() == 64;
    }
};

bool encodeDedicatedAdmission(const DedicatedAdmission& admission,
                              std::vector<uint8_t>& bytes);
bool decodeDedicatedAdmission(const uint8_t* bytes, size_t size,
                              DedicatedAdmission& admission);

class IDedicatedWorldHost {
public:
    virtual ~IDedicatedWorldHost() = default;
    virtual bool startAuthoritativeWorld(
        const DedicatedAllocation& allocation) = 0;
    virtual void stopAuthoritativeWorld(
        DedicatedAllocationId allocationId) = 0;
    virtual void playerConnected(DedicatedAllocationId allocationId,
                                 const PeerId& peerId,
                                 NetConnection* connection) = 0;
    virtual void playerDisconnected(DedicatedAllocationId allocationId,
                                    const PeerId& peerId) = 0;
    virtual void tickAuthoritativeWorlds(float deltaTime) = 0;
};

enum class DedicatedServerRuntimeState : uint8_t {
    Stopped = 0,
    Ready,
    Draining,
    Failed,
};

enum class DedicatedServerRuntimeError : uint8_t {
    None = 0,
    InvalidConfiguration,
    RegistrationFailed,
    ListenFailed,
    BackendUnavailable,
    WorldStartFailed,
    CredentialRejected,
};

struct DedicatedServerRuntimeConfig {
    DedicatedServerRegistration registration;
    uint32_t heartbeatIntervalMs = 5000;
    uint32_t allocationPollIntervalMs = 250;
    uint32_t retryIntervalMs = 500;
    uint32_t maximumConsecutiveBackendFailures = 5;
    uint32_t drainTimeoutMs = 30000;
    std::function<uint64_t()> nowMonotonicMilliseconds;
    std::function<uint64_t()> nowUnixSeconds;

    bool isValid() const;
};

struct DedicatedServerRuntimeStatus {
    DedicatedServerRuntimeState state = DedicatedServerRuntimeState::Stopped;
    DedicatedServerRuntimeError error = DedicatedServerRuntimeError::None;
    OnlineServiceError serviceError = OnlineServiceError::None;
    std::string message;
    DedicatedServerInfo server;
    size_t activeAllocations = 0;
    size_t connectedPlayers = 0;
    uint64_t rejectedAdmissions = 0;
};

// Owns the network listener while active. Backend operations after startup run
// on one bounded worker so heartbeat outages never block the simulation tick.
class DedicatedServerRuntime {
public:
    DedicatedServerRuntime(
        INetworkSubSystem& network,
        std::shared_ptr<IDedicatedServerService> service,
        std::shared_ptr<IDedicatedWorldHost> worlds,
        DedicatedServerRuntimeConfig config);
    ~DedicatedServerRuntime();

    DedicatedServerRuntime(const DedicatedServerRuntime&) = delete;
    DedicatedServerRuntime& operator=(const DedicatedServerRuntime&) = delete;

    bool start();
    void update(float deltaTime);
    void beginDrain();
    void stop();
    DedicatedServerRuntimeStatus getStatus() const;

private:
    struct Impl;
    std::unique_ptr<Impl> _impl;
};

struct NetworkDedicatedSessionConnectorConfig {
    PeerId localPeerId;
    uint32_t connectTimeoutMs = 10000;
    std::function<uint64_t()> nowMonotonicMilliseconds;

    bool isValid() const;
};

// Concrete assignment handoff for the normal IP transport. It installs the
// reservation in the GNS handshake before connect(), so an unauthenticated
// connection never reaches the server's application callbacks.
class NetworkDedicatedSessionConnector final
    : public IDedicatedSessionConnector {
public:
    NetworkDedicatedSessionConnector(
        INetworkSubSystem& network,
        NetworkDedicatedSessionConnectorConfig config);
    ~NetworkDedicatedSessionConnector() override;

    bool connect(const DedicatedAllocation& allocation) override;
    void disconnect() override;
    void update() override;
    DedicatedSessionConnectionStatus getStatus() const override;

private:
    struct Impl;
    std::unique_ptr<Impl> _impl;
};

} // namespace ayt::net
