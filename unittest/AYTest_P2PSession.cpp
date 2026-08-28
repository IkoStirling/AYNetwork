// AYTest_P2PSession.cpp - authority-session control wire validation.

#include "P2PSessionProtocol.h"

#include <AYTest.h>

#include <array>
#include <limits>
#include <vector>

using namespace ayt::net;

TEST_SUITE(P2PSessionProtocol)

TEST_CASE(JoinRequestSeparatesFreshAndResumeForms) {
    ayt::test::setCurrentCase("JoinRequestSeparatesFreshAndResumeForms");
    session::JoinRequest request;
    request.ticket = {0x00, 0x7f, 0xff};
    std::vector<uint8_t> wire;
    session::JoinRequest decoded;
    CHECK(session::encodeJoinRequest(request, wire));
    CHECK(session::decodeJoinRequest(wire.data(), wire.size(), decoded));
    CHECK(decoded.ticket == request.ticket);
    CHECK(!decoded.resume);

    request = {};
    request.resume = true;
    request.sessionId = 0x1122334455667788ull;
    request.epoch = 7;
    request.seatId = 3;
    CHECK(session::encodeJoinRequest(request, wire));
    CHECK(session::decodeJoinRequest(wire.data(), wire.size(), decoded));
    CHECK(decoded.resume);
    CHECK(decoded.sessionId == request.sessionId);
    CHECK_INT_EQ(decoded.epoch, 7);
    CHECK_INT_EQ(decoded.seatId, 3);

    request.seatId = 0;
    CHECK(!session::encodeJoinRequest(request, wire));
    request = {};
    request.sessionId = 1;
    CHECK(!session::encodeJoinRequest(request, wire));
    std::vector<uint8_t> oversized(kP2PMaxJoinTicketBytes + 1, 1);
    request = {};
    request.ticket = std::move(oversized);
    CHECK(!session::encodeJoinRequest(request, wire));
}

TEST_CASE(JoinResultCarriesStableSeatAndEpoch) {
    ayt::test::setCurrentCase("JoinResultCarriesStableSeatAndEpoch");
    session::JoinResult result;
    result.decision = P2PJoinDecision::accept();
    result.sessionId = 99;
    result.epoch = 4;
    result.seatId = 12;
    result.resumed = true;
    std::vector<uint8_t> wire;
    session::JoinResult decoded;
    CHECK(session::encodeJoinResult(result, wire));
    CHECK(session::decodeJoinResult(wire.data(), wire.size(), decoded));
    CHECK(decoded.decision.accepted);
    CHECK(decoded.resumed);
    CHECK(decoded.sessionId == 99);
    CHECK_INT_EQ(decoded.epoch, 4);
    CHECK_INT_EQ(decoded.seatId, 12);

    result = {};
    result.decision = P2PJoinDecision::reject(P2PJoinRejectReason::SessionClosed);
    CHECK(session::encodeJoinResult(result, wire));
    CHECK(session::decodeJoinResult(wire.data(), wire.size(), decoded));
    CHECK(!decoded.decision.accepted);
    CHECK(decoded.decision.reason == P2PJoinRejectReason::SessionClosed);

    result = {};
    result.decision = P2PJoinDecision::accept();
    CHECK(!session::encodeJoinResult(result, wire));
}

TEST_CASE(BarrierIsScopedBySessionAndEpoch) {
    ayt::test::setCurrentCase("BarrierIsScopedBySessionAndEpoch");
    std::vector<uint8_t> wire;
    session::encodeBarrier(55, 3, 17, 2, 2, true, wire);
    uint64_t sessionId = 0;
    uint32_t epoch = 0;
    uint32_t revision = 0;
    uint16_t ready = 0;
    uint16_t total = 0;
    bool open = false;
    CHECK(session::decodeBarrier(wire.data(), wire.size(), sessionId, epoch,
                                 revision, ready, total, open));
    CHECK(sessionId == 55);
    CHECK_INT_EQ(epoch, 3);
    CHECK_INT_EQ(revision, 17);
    CHECK_INT_EQ(ready, 2);
    CHECK_INT_EQ(total, 2);
    CHECK(open);

    session::encodeBarrier(55, 3, 18, 3, 2, false, wire);
    CHECK(!session::decodeBarrier(wire.data(), wire.size(), sessionId, epoch,
                                  revision, ready, total, open));
}

TEST_CASE(RosterRoundTripIsStrictAndDeterministic) {
    ayt::test::setCurrentCase("RosterRoundTripIsStrictAndDeterministic");
    session::Roster roster;
    roster.sessionId = 0xaabbccdd;
    roster.epoch = 2;
    roster.revision = 9;
    roster.hostPeerId = PeerId{"host-a"};
    roster.members = {
        {PeerId{"host-a"}, 1, true, true, true},
        {PeerId{"client-b"}, 2, true, false, false},
        {PeerId{"client-c"}, 3, false, false, false},
    };
    std::vector<uint8_t> wire;
    session::Roster decoded;
    CHECK(session::encodeRoster(roster, wire));
    CHECK(session::decodeRoster(wire.data(), wire.size(), decoded));
    CHECK(decoded.sessionId == roster.sessionId);
    CHECK_INT_EQ(decoded.epoch, 2);
    CHECK_INT_EQ(decoded.revision, 9);
    CHECK(decoded.hostPeerId == PeerId{"host-a"});
    CHECK_INT_EQ(decoded.members.size(), 3);
    CHECK(decoded.members[2].peerId == PeerId{"client-c"});
    CHECK(!decoded.members[2].connected);

    roster.members[2].seatId = 2;
    CHECK(!session::encodeRoster(roster, wire));
    roster.members[2].seatId = 3;
    roster.members[1].host = true;
    CHECK(!session::encodeRoster(roster, wire));
    roster.members[1].host = false;
    roster.hostPeerId = PeerId{"client-b"};
    CHECK(!session::encodeRoster(roster, wire));
    roster.hostPeerId = PeerId{"host-a"};
    roster.members[2].ready = true;
    CHECK(!session::encodeRoster(roster, wire));
}

TEST_CASE(MigrationPlanAdvancesExactlyOneEpoch) {
    ayt::test::setCurrentCase("MigrationPlanAdvancesExactlyOneEpoch");
    session::MigrationPlan plan;
    plan.sessionId = 0x1122334455667788ull;
    plan.currentEpoch = 7;
    plan.nextEpoch = 8;
    plan.electedHostPeerId = PeerId{"client-a"};
    plan.replicatedStateHash = 0xAABBCCDDEEFF0011ull;
    plan.applicationStateHash = 0x1122334455667788ull;
    plan.applicationState = {1, 3, 5, 7, 9};
    std::vector<uint8_t> wire;
    session::MigrationPlan decoded;
    CHECK(session::encodeMigrationPlan(plan, wire));
    CHECK(session::decodeMigrationPlan(wire.data(), wire.size(), decoded));
    CHECK(decoded.sessionId == plan.sessionId);
    CHECK_INT_EQ(decoded.currentEpoch, 7);
    CHECK_INT_EQ(decoded.nextEpoch, 8);
    CHECK(decoded.electedHostPeerId == PeerId{"client-a"});
    CHECK(decoded.replicatedStateHash == plan.replicatedStateHash);
    CHECK(decoded.applicationStateHash == plan.applicationStateHash);
    CHECK(decoded.applicationState == plan.applicationState);

    plan.nextEpoch = 9;
    CHECK(!session::encodeMigrationPlan(plan, wire));
    plan.currentEpoch = std::numeric_limits<uint32_t>::max();
    plan.nextEpoch = 0;
    CHECK(!session::encodeMigrationPlan(plan, wire));

    plan.currentEpoch = 7;
    plan.nextEpoch = 8;
    plan.applicationState.resize(kP2PMaxMigrationStateBytes + 1);
    CHECK(!session::encodeMigrationPlan(plan, wire));
}

TEST_CASE(MigrationAckAndDecisionRoundTrip) {
    ayt::test::setCurrentCase("MigrationAckAndDecisionRoundTrip");
    session::MigrationAck ack;
    ack.sessionId = 91;
    ack.currentEpoch = 4;
    ack.nextEpoch = 5;
    ack.replicatedStateHash = 0x1234;
    ack.applicationStateHash = 0x5678;
    ack.accepted = true;
    std::vector<uint8_t> wire;
    session::MigrationAck decodedAck;
    CHECK(session::encodeMigrationAck(ack, wire));
    CHECK(session::decodeMigrationAck(wire.data(), wire.size(), decodedAck));
    CHECK(decodedAck.sessionId == ack.sessionId);
    CHECK(decodedAck.accepted);
    CHECK(decodedAck.failure == P2PMigrationFailureReason::None);
    CHECK(decodedAck.replicatedStateHash == ack.replicatedStateHash);

    ack.accepted = false;
    ack.failure = P2PMigrationFailureReason::ApplicationStateRejected;
    CHECK(session::encodeMigrationAck(ack, wire));
    CHECK(session::decodeMigrationAck(wire.data(), wire.size(), decodedAck));
    CHECK(!decodedAck.accepted);
    CHECK(decodedAck.failure ==
          P2PMigrationFailureReason::ApplicationStateRejected);
    ack.failure = P2PMigrationFailureReason::None;
    CHECK(!session::encodeMigrationAck(ack, wire));
    ack.accepted = true;

    session::MigrationDecision decision;
    decision.sessionId = ack.sessionId;
    decision.currentEpoch = ack.currentEpoch;
    decision.nextEpoch = ack.nextEpoch;
    decision.electedHostPeerId = PeerId{"next-host"};
    decision.commit = true;
    session::MigrationDecision decodedDecision;
    CHECK(session::encodeMigrationDecision(decision, wire));
    CHECK(session::decodeMigrationDecision(
        wire.data(), wire.size(), decodedDecision));
    CHECK(decodedDecision.commit);
    CHECK(decodedDecision.failure == P2PMigrationFailureReason::None);
    CHECK(decodedDecision.electedHostPeerId == decision.electedHostPeerId);

    decision.commit = false;
    decision.failure = P2PMigrationFailureReason::ParticipantLost;
    CHECK(session::encodeMigrationDecision(decision, wire));
    CHECK(session::decodeMigrationDecision(
        wire.data(), wire.size(), decodedDecision));
    CHECK(!decodedDecision.commit);
    CHECK(decodedDecision.failure ==
          P2PMigrationFailureReason::ParticipantLost);
    decision.failure = P2PMigrationFailureReason::None;
    CHECK(!session::encodeMigrationDecision(decision, wire));
    decision.commit = true;

    session::MigrationDecisionAck decisionAck;
    decisionAck.sessionId = decision.sessionId;
    decisionAck.currentEpoch = decision.currentEpoch;
    decisionAck.nextEpoch = decision.nextEpoch;
    decisionAck.electedHostPeerId = decision.electedHostPeerId;
    session::MigrationDecisionAck decodedDecisionAck;
    CHECK(session::encodeMigrationDecisionAck(decisionAck, wire));
    CHECK(session::decodeMigrationDecisionAck(
        wire.data(), wire.size(), decodedDecisionAck));
    CHECK(decodedDecisionAck.sessionId == decisionAck.sessionId);
    CHECK(decodedDecisionAck.currentEpoch == decisionAck.currentEpoch);
    CHECK(decodedDecisionAck.nextEpoch == decisionAck.nextEpoch);
    CHECK(decodedDecisionAck.electedHostPeerId ==
          decisionAck.electedHostPeerId);

    decision.nextEpoch = 7;
    CHECK(!session::encodeMigrationDecision(decision, wire));
    decisionAck.nextEpoch = 7;
    CHECK(!session::encodeMigrationDecisionAck(decisionAck, wire));
}

TEST_SUITE_END
