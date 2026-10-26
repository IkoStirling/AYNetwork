#include <AYTest.h>
#include <AYNetwork/Replication/SharedObjectAuthority.h>

using namespace ayt::net;
TEST_SUITE(SharedObjectAuthorityTests)
TEST_CASE(claim_update_transfer_release_and_late_join_snapshot) {
    SharedObjectAuthority registry;
    CHECK(registry.beginEpoch(10));
    CHECK(registry.publish(2, 0, {7}));
    CHECK(registry.publish(1, 0, {8}));
    CHECK_INT_EQ(registry.apply({10, 42, 1, 2, 1, SharedObjectAction::Claim}).status,
                 SharedObjectStatus::Applied);
    CHECK_INT_EQ(registry.get(2)->ownerConnection, 42);
    CHECK_INT_EQ(registry.apply({10, 42, 2, 2, 2, SharedObjectAction::Update, 0, {9, 10}}).status,
                 SharedObjectStatus::Applied);
    CHECK_INT_EQ(registry.apply({10, 42, 3, 2, 3, SharedObjectAction::Transfer, 77}).status,
                 SharedObjectStatus::Applied);
    CHECK_INT_EQ(registry.apply({10, 77, 1, 2, 4, SharedObjectAction::Release}).status,
                 SharedObjectStatus::Applied);
    const auto snapshot = registry.snapshot();
    CHECK_INT_EQ(snapshot.size(), 2);
    CHECK_INT_EQ(snapshot[0].objectId, 1);
    CHECK_INT_EQ(snapshot[1].objectId, 2);
    CHECK_INT_EQ(snapshot[1].revision, 5);
    CHECK_INT_EQ(snapshot[1].ownerConnection, 0);
    CHECK_INT_EQ(snapshot[1].bytes.size(), 2);
    CHECK_INT_EQ(snapshot[1].bytes[0], 9);
}
TEST_CASE(stale_and_unauthorized_requests_are_atomic) {
    SharedObjectAuthority registry;
    CHECK(registry.beginEpoch(1));
    CHECK(registry.publish(1, 7, {1}));
    CHECK_INT_EQ(registry.apply({1, 8, 1, 1, 1, SharedObjectAction::Update, 0, {2}}).status,
                 SharedObjectStatus::NotOwner);
    CHECK_INT_EQ(registry.apply({1, 7, 2, 1, 0, SharedObjectAction::Update, 0, {2}}).status,
                 SharedObjectStatus::RevisionConflict);
    CHECK_INT_EQ(registry.apply({2, 7, 3, 1, 1, SharedObjectAction::Update, 0, {2}}).status,
                 SharedObjectStatus::WrongEpoch);
    CHECK_INT_EQ(registry.apply({1, 7, 4, 99, 1, SharedObjectAction::Update, 0, {2}}).status,
                 SharedObjectStatus::Missing);
    CHECK_INT_EQ(registry.apply({1, 7, 5, 1, 1, SharedObjectAction::Transfer, 0}).status,
                 SharedObjectStatus::Invalid);
    CHECK_INT_EQ(registry.get(1)->revision, 1);
    CHECK_INT_EQ(registry.get(1)->bytes[0], 1);
}
TEST_CASE(retries_are_idempotent_and_request_id_reuse_is_rejected) {
    SharedObjectAuthority registry;
    CHECK(registry.beginEpoch(1));
    CHECK(registry.publish(1, 7, {}));
    const SharedObjectCommand request{1, 7, 9, 1, 1, SharedObjectAction::Update, 0, {3}};
    CHECK_INT_EQ(registry.apply(request).status, SharedObjectStatus::Applied);
    CHECK_INT_EQ(registry.apply(request).status, SharedObjectStatus::Replayed);
    auto reused = request;
    reused.bytes = {4};
    CHECK_INT_EQ(registry.apply(reused).status, SharedObjectStatus::Invalid);
    CHECK_INT_EQ(registry.get(1)->revision, 2);
    CHECK_INT_EQ(registry.get(1)->bytes[0], 3);
    CHECK(registry.beginEpoch(2));
    CHECK_INT_EQ(registry.apply(request).status, SharedObjectStatus::WrongEpoch);
    CHECK_INT_EQ(registry.apply({2, 7, 9, 1, 2, SharedObjectAction::Update, 0, {5}}).status,
                 SharedObjectStatus::Applied);
    CHECK_INT_EQ(registry.get(1)->revision, 3);
}
TEST_CASE(disconnect_release_is_once_only_and_does_not_rewrite_payload) {
    SharedObjectAuthority registry;
    CHECK(registry.beginEpoch(1));
    CHECK(registry.publish(2, 9, {2}));
    CHECK(registry.publish(1, 9, {1}));
    CHECK(registry.publish(3, 8, {3}));
    const auto released = registry.releaseConnection(9);
    CHECK_INT_EQ(released.size(), 2);
    CHECK_INT_EQ(released[0], 1);
    CHECK_INT_EQ(released[1], 2);
    CHECK_INT_EQ(registry.releaseConnection(9).size(), 0);
    CHECK_INT_EQ(registry.get(1)->revision, 2);
    CHECK_INT_EQ(registry.get(1)->bytes[0], 1);
    CHECK_INT_EQ(registry.get(3)->ownerConnection, 8);
}
TEST_CASE(payload_limit_rejects_before_mutation_and_receipt_eviction_stays_safe) {
    SharedObjectAuthority registry;
    CHECK(registry.beginEpoch(1));
    CHECK_FALSE(registry.publish(1, 1,
        std::vector<std::uint8_t>(SharedObjectAuthority::MaxStateBytes + 1, 0)));
    CHECK(registry.publish(1, 1, {1}));
    CHECK_INT_EQ(registry.apply({1, 1, 1, 1, 1, SharedObjectAction::Update, 0,
        std::vector<std::uint8_t>(SharedObjectAuthority::MaxStateBytes + 1, 0)}).status,
        SharedObjectStatus::Invalid);
    CHECK_INT_EQ(registry.get(1)->revision, 1);
    for (std::uint64_t id = 1; id <= SharedObjectAuthority::MaxReceipts + 1; ++id)
        CHECK_INT_EQ(registry.apply({1, 1, id, 1, id, SharedObjectAction::Update, 0, {2}}).status,
                     SharedObjectStatus::Applied);
    CHECK_INT_EQ(registry.apply({1, 1, 1, 1, 1, SharedObjectAction::Update, 0, {2}}).status,
                 SharedObjectStatus::RevisionConflict);
    CHECK_INT_EQ(registry.get(1)->revision, SharedObjectAuthority::MaxReceipts + 2);
}
TEST_CASE(retire_publishes_final_tombstone_and_reserves_stable_identity) {
    SharedObjectAuthority registry;
    CHECK(registry.beginEpoch(1));
    CHECK(registry.publish(42, 7, {1,2,3}));
    const SharedObjectCommand retire{1,7,10,42,1,SharedObjectAction::Retire};
    CHECK_INT_EQ(registry.apply(retire).status,SharedObjectStatus::Applied);
    CHECK_INT_EQ(registry.apply(retire).status,SharedObjectStatus::Replayed);
    const auto state=registry.get(42);
    CHECK(state.has_value());
    CHECK(state->retired && state->revision==2);
    CHECK(state->ownerConnection==0 && state->bytes.empty());
    CHECK_FALSE(registry.publish(42,0,{9}));
    CHECK_INT_EQ(registry.apply({1,7,11,42,2,SharedObjectAction::Update,0,{4}}).status,
                 SharedObjectStatus::Retired);
    CHECK(registry.releaseConnection(7).empty());
    CHECK(registry.beginEpoch(2));
    CHECK(registry.snapshot()[0].retired);
    CHECK_INT_EQ(registry.apply({2,0,1,42,2,SharedObjectAction::Claim}).status,
                 SharedObjectStatus::Retired);
}
TEST_SUITE_END
