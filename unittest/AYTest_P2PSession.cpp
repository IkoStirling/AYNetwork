// AYTest_P2PSession.cpp - authority-session control wire validation.

#include "P2PSessionProtocol.h"

#include <AYTest.h>

#include <array>
#include <cstdint>
#include <vector>

using namespace ayt::net;

TEST_SUITE(P2PSessionProtocol)

TEST_CASE(JoinRequestRoundTripsAndRejectsMalformedFrames) {
    ayt::test::setCurrentCase("JoinRequestRoundTripsAndRejectsMalformedFrames");
    const std::array<uint8_t, 5> ticket{0x00, 0x10, 0x7f, 0x80, 0xff};
    std::vector<uint8_t> wire;
    std::vector<uint8_t> decoded;

    CHECK(session::encodeJoinRequest(ticket.data(), ticket.size(), wire));
    CHECK(session::decodeJoinRequest(wire.data(), wire.size(), decoded));
    CHECK(decoded == std::vector<uint8_t>(ticket.begin(), ticket.end()));

    CHECK(session::encodeJoinRequest(nullptr, 0, wire));
    CHECK(session::decodeJoinRequest(wire.data(), wire.size(), decoded));
    CHECK(decoded.empty());

    std::vector<uint8_t> maximum(kP2PMaxJoinTicketBytes, 0x5a);
    CHECK(session::encodeJoinRequest(maximum.data(), maximum.size(), wire));
    CHECK(session::decodeJoinRequest(wire.data(), wire.size(), decoded));
    CHECK(decoded == maximum);

    std::vector<uint8_t> oversized(kP2PMaxJoinTicketBytes + 1, 0x01);
    CHECK(!session::encodeJoinRequest(oversized.data(), oversized.size(), wire));
    CHECK(!session::encodeJoinRequest(nullptr, 1, wire));

    const std::array<uint8_t, 1> truncated{0x00};
    CHECK(!session::decodeJoinRequest(truncated.data(), truncated.size(), decoded));
    const std::array<uint8_t, 3> lengthMismatch{0x02, 0x00, 0xaa};
    CHECK(!session::decodeJoinRequest(lengthMismatch.data(), lengthMismatch.size(), decoded));
    const std::array<uint8_t, 3> trailingBytes{0x00, 0x00, 0xaa};
    CHECK(!session::decodeJoinRequest(trailingBytes.data(), trailingBytes.size(), decoded));
}

TEST_CASE(JoinResultRequiresCanonicalDecision) {
    ayt::test::setCurrentCase("JoinResultRequiresCanonicalDecision");
    std::vector<uint8_t> wire;
    P2PJoinDecision decoded;

    session::encodeJoinResult(P2PJoinDecision::accept(), wire);
    CHECK(session::decodeJoinResult(wire.data(), wire.size(), decoded));
    CHECK(decoded.accepted);
    CHECK(decoded.reason == P2PJoinRejectReason::None);

    session::encodeJoinResult(
        P2PJoinDecision::reject(P2PJoinRejectReason::SessionFull), wire);
    CHECK(session::decodeJoinResult(wire.data(), wire.size(), decoded));
    CHECK(!decoded.accepted);
    CHECK(decoded.reason == P2PJoinRejectReason::SessionFull);

    const std::array<uint8_t, 2> acceptedWithReason{1, 2};
    CHECK(!session::decodeJoinResult(
        acceptedWithReason.data(), acceptedWithReason.size(), decoded));
    const std::array<uint8_t, 2> rejectedWithoutReason{0, 0};
    CHECK(!session::decodeJoinResult(
        rejectedWithoutReason.data(), rejectedWithoutReason.size(), decoded));
    const std::array<uint8_t, 2> invalidReason{0, 0xff};
    CHECK(!session::decodeJoinResult(invalidReason.data(), invalidReason.size(), decoded));
}

TEST_CASE(ReadyAndBarrierFramesAreStrict) {
    ayt::test::setCurrentCase("ReadyAndBarrierFramesAreStrict");
    std::vector<uint8_t> wire;
    bool ready = false;
    session::encodeReadyState(true, wire);
    CHECK(session::decodeReadyState(wire.data(), wire.size(), ready));
    CHECK(ready);
    const std::array<uint8_t, 1> invalidReady{2};
    CHECK(!session::decodeReadyState(invalidReady.data(), invalidReady.size(), ready));

    uint32_t revision = 0;
    uint16_t readyMembers = 0;
    uint16_t totalMembers = 0;
    bool open = false;
    session::encodeBarrier(17, 3, 3, true, wire);
    CHECK(session::decodeBarrier(wire.data(), wire.size(), revision,
                                 readyMembers, totalMembers, open));
    CHECK_INT_EQ(revision, 17);
    CHECK_INT_EQ(readyMembers, 3);
    CHECK_INT_EQ(totalMembers, 3);
    CHECK(open);

    session::encodeBarrier(18, 4, 3, false, wire);
    CHECK(!session::decodeBarrier(wire.data(), wire.size(), revision,
                                  readyMembers, totalMembers, open));
    session::encodeBarrier(19, 2, 2, false, wire);
    CHECK(!session::decodeBarrier(wire.data(), wire.size(), revision,
                                  readyMembers, totalMembers, open));
    session::encodeBarrier(20, 0, 0, true, wire);
    CHECK(!session::decodeBarrier(wire.data(), wire.size(), revision,
                                  readyMembers, totalMembers, open));
}

TEST_SUITE_END
