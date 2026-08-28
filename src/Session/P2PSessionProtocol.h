#pragma once

#include <AYNetwork/P2P.h>

#include <cstddef>
#include <cstdint>
#include <vector>

namespace ayt::net::session
{

struct JoinRequest {
    std::vector<uint8_t> ticket;
    uint64_t sessionId = 0;
    uint32_t epoch = 0;
    uint32_t seatId = 0;
    bool resume = false;
};

struct JoinResult {
    P2PJoinDecision decision;
    uint64_t sessionId = 0;
    uint32_t epoch = 0;
    uint32_t seatId = 0;
    bool resumed = false;
};

struct RosterMember {
    PeerId peerId;
    uint32_t seatId = 0;
    bool connected = false;
    bool ready = false;
    bool host = false;
};

struct Roster {
    uint64_t sessionId = 0;
    uint32_t epoch = 0;
    uint32_t revision = 0;
    PeerId hostPeerId;
    std::vector<RosterMember> members;
};

struct MigrationPlan {
    uint64_t sessionId = 0;
    uint32_t currentEpoch = 0;
    uint32_t nextEpoch = 0;
    PeerId electedHostPeerId;
    uint64_t replicatedStateHash = 0;
    uint64_t applicationStateHash = 0;
    std::vector<uint8_t> applicationState;
};

struct MigrationAck {
    uint64_t sessionId = 0;
    uint32_t currentEpoch = 0;
    uint32_t nextEpoch = 0;
    uint64_t replicatedStateHash = 0;
    uint64_t applicationStateHash = 0;
    bool accepted = false;
    P2PMigrationFailureReason failure = P2PMigrationFailureReason::None;
};

struct MigrationDecision {
    uint64_t sessionId = 0;
    uint32_t currentEpoch = 0;
    uint32_t nextEpoch = 0;
    PeerId electedHostPeerId;
    bool commit = false;
    P2PMigrationFailureReason failure = P2PMigrationFailureReason::None;
};

struct MigrationDecisionAck {
    uint64_t sessionId = 0;
    uint32_t currentEpoch = 0;
    uint32_t nextEpoch = 0;
    PeerId electedHostPeerId;
};

bool encodeJoinRequest(const JoinRequest& request, std::vector<uint8_t>& out);
bool decodeJoinRequest(const uint8_t* data, size_t size, JoinRequest& request);

bool encodeJoinResult(const JoinResult& result, std::vector<uint8_t>& out);
bool decodeJoinResult(const uint8_t* data, size_t size,
                      JoinResult& result);

void encodeReadyState(bool ready, std::vector<uint8_t>& out);
bool decodeReadyState(const uint8_t* data, size_t size, bool& ready);

void encodeBarrier(uint64_t sessionId, uint32_t epoch, uint32_t revision,
                   uint16_t readyMembers,
                   uint16_t totalMembers, bool open,
                   std::vector<uint8_t>& out);
bool decodeBarrier(const uint8_t* data, size_t size,
                   uint64_t& sessionId, uint32_t& epoch, uint32_t& revision,
                   uint16_t& readyMembers,
                   uint16_t& totalMembers, bool& open);

bool encodeRoster(const Roster& roster, std::vector<uint8_t>& out);
bool decodeRoster(const uint8_t* data, size_t size, Roster& roster);
bool encodeMigrationPlan(const MigrationPlan& plan, std::vector<uint8_t>& out);
bool decodeMigrationPlan(const uint8_t* data, size_t size, MigrationPlan& plan);
bool encodeMigrationAck(const MigrationAck& ack, std::vector<uint8_t>& out);
bool decodeMigrationAck(const uint8_t* data, size_t size, MigrationAck& ack);
bool encodeMigrationDecision(const MigrationDecision& decision,
                             std::vector<uint8_t>& out);
bool decodeMigrationDecision(const uint8_t* data, size_t size,
                             MigrationDecision& decision);
bool encodeMigrationDecisionAck(const MigrationDecisionAck& ack,
                                std::vector<uint8_t>& out);
bool decodeMigrationDecisionAck(const uint8_t* data, size_t size,
                                MigrationDecisionAck& ack);

} // namespace ayt::net::session
