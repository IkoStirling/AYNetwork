// AYNetwork/Replay/NetworkReplayRecorderAdapter.h - R5.3 (2026-08-24).
//
// Adapter that turns AYNetwork wire-tap hooks into foundation-level
// IReplayRecorder calls. Owns the underlying IReplayRecorder (usually
// a FileReplayRecorder) by composition.
//
// Server-only by design: every recordXxx method consults a small
// _authorityGateOk flag that the wiring code toggles from
// ReplicationManager::isAuthority() before each call. This avoids
// pulling authority logic into the adapter (which would cycle into
// ReplicationManager::setReplayRecorder's two-way dependency).

#pragma once
#include <cstdint>
#include <cstddef>
#include <functional>
#include <memory>
#include <vector>

#include <AYReplay/IReplayRecorder.h>
#include <AYReplay/ReplayTypes.h>

namespace ayt::net::replay
{

// NetworkReplayRecorderAdapter implements IReplayRecorder and adds
// network-specific typed recordXxx methods that build small packed
// payloads and forward to the inner recorder.
class NetworkReplayRecorderAdapter final : public ayt::replay::IReplayRecorder
{
public:
    // Inner must outlive the adapter. The adapter does NOT take
    // ownership of StateBytesProvider closures — callers must keep
    // them alive as long as they invoke recordPeriodicCheckpoint.
    explicit NetworkReplayRecorderAdapter(
        std::unique_ptr<ayt::replay::IReplayRecorder> inner);
    ~NetworkReplayRecorderAdapter() override;

    NetworkReplayRecorderAdapter(const NetworkReplayRecorderAdapter&)            = delete;
    NetworkReplayRecorderAdapter& operator=(const NetworkReplayRecorderAdapter&) = delete;

    // ------ High-level network record methods ------
    bool recordInitialFullSnapshot(uint32_t connectionId, uint32_t serverTick,
                                   uint8_t frameFlags,
                                   const uint8_t* sealedPayload, size_t size);
    bool recordSpawn(uint32_t connectionId, uint32_t serverTick,
                     uint32_t netId, uint64_t schemaHash,
                     const uint8_t* spawnPayload, size_t size);
    bool recordDespawn(uint32_t connectionId, uint32_t serverTick, uint32_t netId);
    bool recordDeltaSnapshot(uint32_t connectionId, uint32_t serverTick,
                             uint8_t frameFlags,
                             const uint8_t* sealedPayload, size_t size);
    bool recordInput(uint32_t connectionId, uint32_t serverTick,
                     uint32_t inputSeq, uint32_t serverTickAtSend,
                     const uint8_t* payload, size_t size);
    bool recordRpc(uint16_t messageType, uint32_t connectionId,
                   uint32_t serverTick,
                   const uint8_t* body, size_t size);
    bool recordAuthorityChange(uint32_t netId, uint32_t serverTick,
                               uint8_t oldKind, uint8_t newKind,
                               uint32_t connectionId);

    // Computes one FNV-1a state hash across all ghosts registered with
    // `provider` and writes a single ReplayCheckpointHeader. Returns
    // false if the provider returns false for every netId (no state).
    // `registeredNetIds` enumerates the netIds the caller wants hashed.
    using StateBytesProvider =
        std::function<bool(uint32_t netId, std::vector<uint8_t>& out)>;
    bool recordPeriodicCheckpoint(uint32_t serverTick,
                                 const std::vector<uint32_t>& registeredNetIds,
                                 const StateBytesProvider& provider);

    // IReplayRecorder
    bool beginSession(const ayt::replay::ReplayFileHeader& h) override;
    bool endSession() override;
    bool recordEvent(ayt::replay::ReplayTick tick,
                     ayt::replay::ReplayEventType type,
                     const uint8_t* payload, size_t size) override;
    bool recordCheckpoint(ayt::replay::ReplayTick tick, uint64_t stateHash,
                          const uint8_t* snapshotBytes,
                          size_t snapshotSize) override;
    bool     flush() override;
    bool     isOpen()      const override;
    size_t   bytesWritten() const override;
    uint32_t rotationIndex() const override;

    // Authority gate: when false, all recordXxx methods no-op and
    // return true (no error) so existing caller behavior is unchanged.
    // The wire-tap wiring code flips this immediately around each call.
    void     setAuthorityGate(bool ok) { _authorityGateOk = ok; }
    bool     authorityGate() const     { return _authorityGateOk; }

    // Direct access to the inner recorder (tests use it to assert
    // bytes-on-disk, etc).
    ayt::replay::IReplayRecorder* inner() const { return _inner.get(); }

private:
    std::unique_ptr<ayt::replay::IReplayRecorder> _inner;
    bool _authorityGateOk = false;
};

} // namespace ayt::net::replay
