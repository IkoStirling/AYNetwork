#include "P2PSessionProtocol.h"

namespace ayt::net::session
{
namespace
{

void appendU16(std::vector<uint8_t>& out, uint16_t value) {
    out.push_back(static_cast<uint8_t>(value & 0xffu));
    out.push_back(static_cast<uint8_t>((value >> 8u) & 0xffu));
}

void appendU32(std::vector<uint8_t>& out, uint32_t value) {
    out.push_back(static_cast<uint8_t>(value & 0xffu));
    out.push_back(static_cast<uint8_t>((value >> 8u) & 0xffu));
    out.push_back(static_cast<uint8_t>((value >> 16u) & 0xffu));
    out.push_back(static_cast<uint8_t>((value >> 24u) & 0xffu));
}

uint16_t readU16(const uint8_t* data) {
    return static_cast<uint16_t>(data[0]) |
           static_cast<uint16_t>(static_cast<uint16_t>(data[1]) << 8u);
}

uint32_t readU32(const uint8_t* data) {
    return static_cast<uint32_t>(data[0]) |
           (static_cast<uint32_t>(data[1]) << 8u) |
           (static_cast<uint32_t>(data[2]) << 16u) |
           (static_cast<uint32_t>(data[3]) << 24u);
}

bool validRejectReason(uint8_t value) {
    return value <= static_cast<uint8_t>(P2PJoinRejectReason::MalformedRequest);
}

} // namespace

bool encodeJoinRequest(const uint8_t* ticket, size_t ticketSize,
                       std::vector<uint8_t>& out) {
    out.clear();
    if ((ticket == nullptr && ticketSize != 0) ||
        ticketSize > kP2PMaxJoinTicketBytes) return false;
    out.reserve(2 + ticketSize);
    appendU16(out, static_cast<uint16_t>(ticketSize));
    if (ticketSize != 0) out.insert(out.end(), ticket, ticket + ticketSize);
    return true;
}

bool decodeJoinRequest(const uint8_t* data, size_t size,
                       std::vector<uint8_t>& ticket) {
    ticket.clear();
    if (!data || size < 2) return false;
    const size_t ticketSize = readU16(data);
    if (ticketSize > kP2PMaxJoinTicketBytes || size != 2 + ticketSize) return false;
    ticket.assign(data + 2, data + size);
    return true;
}

void encodeJoinResult(const P2PJoinDecision& decision,
                      std::vector<uint8_t>& out) {
    out = {
        static_cast<uint8_t>(decision.accepted ? 1 : 0),
        static_cast<uint8_t>(decision.accepted
            ? P2PJoinRejectReason::None : decision.reason),
    };
}

bool decodeJoinResult(const uint8_t* data, size_t size,
                      P2PJoinDecision& decision) {
    if (!data || size != 2 || data[0] > 1 || !validRejectReason(data[1])) return false;
    decision.accepted = data[0] != 0;
    decision.reason = static_cast<P2PJoinRejectReason>(data[1]);
    if (decision.accepted && decision.reason != P2PJoinRejectReason::None) return false;
    if (!decision.accepted && decision.reason == P2PJoinRejectReason::None) return false;
    return true;
}

void encodeReadyState(bool ready, std::vector<uint8_t>& out) {
    out.assign(1, static_cast<uint8_t>(ready ? 1 : 0));
}

bool decodeReadyState(const uint8_t* data, size_t size, bool& ready) {
    if (!data || size != 1 || data[0] > 1) return false;
    ready = data[0] != 0;
    return true;
}

void encodeBarrier(uint32_t revision, uint16_t readyMembers,
                   uint16_t totalMembers, bool open,
                   std::vector<uint8_t>& out) {
    out.clear();
    out.reserve(9);
    appendU32(out, revision);
    appendU16(out, readyMembers);
    appendU16(out, totalMembers);
    out.push_back(static_cast<uint8_t>(open ? 1 : 0));
}

bool decodeBarrier(const uint8_t* data, size_t size,
                   uint32_t& revision, uint16_t& readyMembers,
                   uint16_t& totalMembers, bool& open) {
    if (!data || size != 9 || data[8] > 1) return false;
    revision = readU32(data);
    readyMembers = readU16(data + 4);
    totalMembers = readU16(data + 6);
    open = data[8] != 0;
    if (readyMembers > totalMembers || open != (readyMembers == totalMembers)) return false;
    return totalMembers != 0;
}

} // namespace ayt::net::session
