#include "P2PSessionProtocol.h"

#include <limits>
#include <string>
#include <unordered_set>

namespace ayt::net::session
{
namespace
{

constexpr uint8_t kSessionWireVersion = 2;
constexpr size_t kJoinRequestHeaderBytes = 20;
constexpr size_t kJoinResultBytes = 20;

void appendU16(std::vector<uint8_t>& out, uint16_t value) {
    out.push_back(static_cast<uint8_t>(value & 0xffu));
    out.push_back(static_cast<uint8_t>((value >> 8u) & 0xffu));
}
void appendU32(std::vector<uint8_t>& out, uint32_t value) {
    for (unsigned shift = 0; shift < 32; shift += 8) {
        out.push_back(static_cast<uint8_t>((value >> shift) & 0xffu));
    }
}
void appendU64(std::vector<uint8_t>& out, uint64_t value) {
    for (unsigned shift = 0; shift < 64; shift += 8) {
        out.push_back(static_cast<uint8_t>((value >> shift) & 0xffu));
    }
}
uint16_t readU16(const uint8_t* data) {
    return static_cast<uint16_t>(data[0]) |
           static_cast<uint16_t>(static_cast<uint16_t>(data[1]) << 8u);
}
uint32_t readU32(const uint8_t* data) {
    uint32_t value = 0;
    for (unsigned shift = 0; shift < 32; shift += 8) {
        value |= static_cast<uint32_t>(data[shift / 8]) << shift;
    }
    return value;
}
uint64_t readU64(const uint8_t* data) {
    uint64_t value = 0;
    for (unsigned shift = 0; shift < 64; shift += 8) {
        value |= static_cast<uint64_t>(data[shift / 8]) << shift;
    }
    return value;
}
bool validRejectReason(uint8_t value) {
    return value <= static_cast<uint8_t>(P2PJoinRejectReason::MalformedRequest);
}
bool appendPeerId(std::vector<uint8_t>& out, const PeerId& peer) {
    if (!peer.isValid() || peer.value.size() > std::numeric_limits<uint8_t>::max()) {
        return false;
    }
    out.push_back(static_cast<uint8_t>(peer.value.size()));
    out.insert(out.end(), peer.value.begin(), peer.value.end());
    return true;
}
bool readPeerId(const uint8_t* data, size_t size, size_t& cursor, PeerId& peer) {
    if (cursor >= size) return false;
    const size_t length = data[cursor++];
    if (length == 0 || cursor + length > size) return false;
    peer = PeerId{std::string(reinterpret_cast<const char*>(data + cursor), length)};
    cursor += length;
    return peer.isValid();
}

} // namespace

bool encodeJoinRequest(const JoinRequest& request, std::vector<uint8_t>& out) {
    out.clear();
    if (request.ticket.size() > kP2PMaxJoinTicketBytes ||
        request.ticket.size() > std::numeric_limits<uint16_t>::max()) return false;
    if (request.resume &&
        (request.sessionId == 0 || request.epoch == 0 || request.seatId == 0)) return false;
    if (!request.resume &&
        (request.sessionId != 0 || request.epoch != 0 || request.seatId != 0)) return false;
    out.reserve(kJoinRequestHeaderBytes + request.ticket.size());
    out.push_back(kSessionWireVersion);
    out.push_back(request.resume ? 1u : 0u);
    appendU16(out, static_cast<uint16_t>(request.ticket.size()));
    appendU64(out, request.sessionId);
    appendU32(out, request.epoch);
    appendU32(out, request.seatId);
    out.insert(out.end(), request.ticket.begin(), request.ticket.end());
    return true;
}

bool decodeJoinRequest(const uint8_t* data, size_t size, JoinRequest& request) {
    request = {};
    if (!data || size < kJoinRequestHeaderBytes || data[0] != kSessionWireVersion ||
        data[1] > 1) return false;
    const size_t ticketSize = readU16(data + 2);
    if (ticketSize > kP2PMaxJoinTicketBytes || size != kJoinRequestHeaderBytes + ticketSize) {
        return false;
    }
    request.resume = data[1] != 0;
    request.sessionId = readU64(data + 4);
    request.epoch = readU32(data + 12);
    request.seatId = readU32(data + 16);
    if (request.resume &&
        (request.sessionId == 0 || request.epoch == 0 || request.seatId == 0)) return false;
    if (!request.resume &&
        (request.sessionId != 0 || request.epoch != 0 || request.seatId != 0)) return false;
    request.ticket.assign(data + kJoinRequestHeaderBytes, data + size);
    return true;
}

bool encodeJoinResult(const JoinResult& result, std::vector<uint8_t>& out) {
    const P2PJoinDecision& decision = result.decision;
    if ((decision.accepted && decision.reason != P2PJoinRejectReason::None) ||
        (!decision.accepted && decision.reason == P2PJoinRejectReason::None) ||
        !validRejectReason(static_cast<uint8_t>(decision.reason))) return false;
    if (decision.accepted &&
        (result.sessionId == 0 || result.epoch == 0 || result.seatId == 0)) return false;
    if (!decision.accepted && result.resumed) return false;
    out.clear();
    out.reserve(kJoinResultBytes);
    out.push_back(kSessionWireVersion);
    out.push_back(decision.accepted ? 1u : 0u);
    out.push_back(static_cast<uint8_t>(decision.reason));
    out.push_back(result.resumed ? 1u : 0u);
    appendU64(out, result.sessionId);
    appendU32(out, result.epoch);
    appendU32(out, result.seatId);
    return true;
}

bool decodeJoinResult(const uint8_t* data, size_t size, JoinResult& result) {
    result = {};
    if (!data || size != kJoinResultBytes || data[0] != kSessionWireVersion ||
        data[1] > 1 || !validRejectReason(data[2]) || data[3] > 1) return false;
    result.decision.accepted = data[1] != 0;
    result.decision.reason = static_cast<P2PJoinRejectReason>(data[2]);
    result.resumed = data[3] != 0;
    result.sessionId = readU64(data + 4);
    result.epoch = readU32(data + 12);
    result.seatId = readU32(data + 16);
    if (result.decision.accepted) {
        return result.decision.reason == P2PJoinRejectReason::None &&
               result.sessionId != 0 && result.epoch != 0 && result.seatId != 0;
    }
    return result.decision.reason != P2PJoinRejectReason::None && !result.resumed;
}

void encodeReadyState(bool ready, std::vector<uint8_t>& out) {
    out.assign(1, static_cast<uint8_t>(ready ? 1 : 0));
}
bool decodeReadyState(const uint8_t* data, size_t size, bool& ready) {
    if (!data || size != 1 || data[0] > 1) return false;
    ready = data[0] != 0;
    return true;
}

void encodeBarrier(uint64_t sessionId, uint32_t epoch, uint32_t revision,
                   uint16_t readyMembers, uint16_t totalMembers, bool open,
                   std::vector<uint8_t>& out) {
    out.clear();
    out.reserve(21);
    appendU64(out, sessionId);
    appendU32(out, epoch);
    appendU32(out, revision);
    appendU16(out, readyMembers);
    appendU16(out, totalMembers);
    out.push_back(static_cast<uint8_t>(open ? 1 : 0));
}
bool decodeBarrier(const uint8_t* data, size_t size,
                   uint64_t& sessionId, uint32_t& epoch, uint32_t& revision,
                   uint16_t& readyMembers, uint16_t& totalMembers, bool& open) {
    if (!data || size != 21 || data[20] > 1) return false;
    sessionId = readU64(data);
    epoch = readU32(data + 8);
    revision = readU32(data + 12);
    readyMembers = readU16(data + 16);
    totalMembers = readU16(data + 18);
    open = data[20] != 0;
    if (sessionId == 0 || epoch == 0 || totalMembers == 0 ||
        readyMembers > totalMembers || open != (readyMembers == totalMembers)) return false;
    return true;
}

bool encodeRoster(const Roster& roster, std::vector<uint8_t>& out) {
    out.clear();
    if (roster.sessionId == 0 || roster.epoch == 0 ||
        roster.members.empty() || roster.members.size() > kP2PMaxSessionMembers ||
        !roster.hostPeerId.isValid()) return false;
    out.reserve(24 + roster.members.size() * 16);
    out.push_back(kSessionWireVersion);
    out.push_back(static_cast<uint8_t>(roster.members.size()));
    appendU16(out, 0);
    appendU64(out, roster.sessionId);
    appendU32(out, roster.epoch);
    appendU32(out, roster.revision);
    if (!appendPeerId(out, roster.hostPeerId)) return false;
    std::unordered_set<std::string> peerIds;
    std::unordered_set<uint32_t> seats;
    size_t hostCount = 0;
    for (const RosterMember& member : roster.members) {
        const bool designatedHost = member.peerId == roster.hostPeerId;
        if (!member.peerId.isValid() || member.seatId == 0 ||
            member.host != designatedHost || (member.ready && !member.connected) ||
            (member.host && !member.connected) ||
            !peerIds.insert(member.peerId.value).second ||
            !seats.insert(member.seatId).second) return false;
        uint8_t flags = member.connected ? 0x01u : 0u;
        if (member.ready) flags |= 0x02u;
        if (member.host) { flags |= 0x04u; ++hostCount; }
        appendU32(out, member.seatId);
        out.push_back(flags);
        if (!appendPeerId(out, member.peerId)) return false;
    }
    return hostCount == 1 && peerIds.count(roster.hostPeerId.value) == 1;
}

bool decodeRoster(const uint8_t* data, size_t size, Roster& roster) {
    roster = {};
    if (!data || size < 21 || data[0] != kSessionWireVersion ||
        readU16(data + 2) != 0) return false;
    const size_t count = data[1];
    if (count == 0 || count > kP2PMaxSessionMembers) return false;
    roster.sessionId = readU64(data + 4);
    roster.epoch = readU32(data + 12);
    roster.revision = readU32(data + 16);
    if (roster.sessionId == 0 || roster.epoch == 0) return false;
    size_t cursor = 20;
    if (!readPeerId(data, size, cursor, roster.hostPeerId)) return false;
    std::unordered_set<std::string> peerIds;
    std::unordered_set<uint32_t> seats;
    size_t hostCount = 0;
    roster.members.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        if (cursor + 5 > size) return false;
        RosterMember member;
        member.seatId = readU32(data + cursor);
        cursor += 4;
        const uint8_t flags = data[cursor++];
        if ((flags & 0xf8u) != 0 || member.seatId == 0 ||
            !readPeerId(data, size, cursor, member.peerId) ||
            !peerIds.insert(member.peerId.value).second ||
            !seats.insert(member.seatId).second) return false;
        member.connected = (flags & 0x01u) != 0;
        member.ready = (flags & 0x02u) != 0;
        member.host = (flags & 0x04u) != 0;
        const bool designatedHost = member.peerId == roster.hostPeerId;
        if (member.host != designatedHost || (member.ready && !member.connected) ||
            (member.host && !member.connected)) return false;
        if (member.host) ++hostCount;
        roster.members.push_back(std::move(member));
    }
    return cursor == size && hostCount == 1 &&
           peerIds.count(roster.hostPeerId.value) == 1;
}

bool encodeMigrationPlan(const MigrationPlan& plan, std::vector<uint8_t>& out) {
    out.clear();
    if (plan.sessionId == 0 || plan.currentEpoch == 0 ||
        plan.currentEpoch == std::numeric_limits<uint32_t>::max() ||
        plan.nextEpoch != plan.currentEpoch + 1 ||
        !plan.electedHostPeerId.isValid()) return false;
    out.reserve(18 + plan.electedHostPeerId.value.size());
    out.push_back(kSessionWireVersion);
    out.push_back(0);
    appendU64(out, plan.sessionId);
    appendU32(out, plan.currentEpoch);
    appendU32(out, plan.nextEpoch);
    return appendPeerId(out, plan.electedHostPeerId);
}

bool decodeMigrationPlan(const uint8_t* data, size_t size, MigrationPlan& plan) {
    plan = {};
    if (!data || size < 19 || data[0] != kSessionWireVersion || data[1] != 0) {
        return false;
    }
    plan.sessionId = readU64(data + 2);
    plan.currentEpoch = readU32(data + 10);
    plan.nextEpoch = readU32(data + 14);
    size_t cursor = 18;
    if (!readPeerId(data, size, cursor, plan.electedHostPeerId)) return false;
    return cursor == size && plan.sessionId != 0 && plan.currentEpoch != 0 &&
           plan.currentEpoch != std::numeric_limits<uint32_t>::max() &&
           plan.nextEpoch == plan.currentEpoch + 1;
}

} // namespace ayt::net::session
