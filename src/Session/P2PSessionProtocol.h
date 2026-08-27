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

} // namespace ayt::net::session
