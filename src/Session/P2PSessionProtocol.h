#pragma once

#include <AYNetwork/P2P.h>

#include <cstddef>
#include <cstdint>
#include <vector>

namespace ayt::net::session
{

bool encodeJoinRequest(const uint8_t* ticket, size_t ticketSize,
                       std::vector<uint8_t>& out);
bool decodeJoinRequest(const uint8_t* data, size_t size,
                       std::vector<uint8_t>& ticket);

void encodeJoinResult(const P2PJoinDecision& decision,
                      std::vector<uint8_t>& out);
bool decodeJoinResult(const uint8_t* data, size_t size,
                      P2PJoinDecision& decision);

void encodeReadyState(bool ready, std::vector<uint8_t>& out);
bool decodeReadyState(const uint8_t* data, size_t size, bool& ready);

void encodeBarrier(uint32_t revision, uint16_t readyMembers,
                   uint16_t totalMembers, bool open,
                   std::vector<uint8_t>& out);
bool decodeBarrier(const uint8_t* data, size_t size,
                   uint32_t& revision, uint16_t& readyMembers,
                   uint16_t& totalMembers, bool& open);

} // namespace ayt::net::session
