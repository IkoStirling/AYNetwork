#include <AYTest.h>
#include <AYNetwork/Replication/SharedObjectReplica.h>

using namespace ayt::net;
TEST_SUITE(SharedObjectReplicaTests)
TEST_CASE(late_join_snapshot_and_ordered_revisions_follow_authority) {
    SharedObjectAuthority authority;
    CHECK(authority.beginEpoch(7));
    CHECK(authority.publish(2,0,{2}));
    CHECK(authority.publish(1,0,{1}));
    SharedObjectReplica replica;
    CHECK_INT_EQ(replica.installSnapshot(7,authority.snapshot()),SharedReplicaStatus::Applied);
    CHECK_INT_EQ(replica.size(),2);
    CHECK_INT_EQ(replica.snapshot()[0].objectId,1);
    CHECK_INT_EQ(authority.apply({7,42,1,2,1,SharedObjectAction::Claim}).status,
                 SharedObjectStatus::Applied);
    auto claimed=*authority.get(2);
    CHECK_INT_EQ(replica.applyDelta(7,claimed),SharedReplicaStatus::Applied);
    CHECK_INT_EQ(replica.applyDelta(7,claimed),SharedReplicaStatus::Replayed);
    CHECK_INT_EQ(replica.get(2)->ownerConnection,42);
    CHECK_INT_EQ(authority.apply({7,42,2,2,2,SharedObjectAction::Update,0,{9}}).status,
                 SharedObjectStatus::Applied);
    auto updated=*authority.get(2);
    CHECK_INT_EQ(replica.applyDelta(7,updated),SharedReplicaStatus::Applied);
    CHECK_INT_EQ(replica.get(2)->bytes[0],9);
    CHECK(authority.publish(3,0,{3}));
    CHECK_INT_EQ(replica.applyDelta(7,*authority.get(3)),SharedReplicaStatus::Applied);
    CHECK_INT_EQ(replica.size(),3);
}
TEST_CASE(gap_and_bad_snapshot_leave_mirror_unchanged_until_full_recovery) {
    SharedObjectReplica replica;
    CHECK_INT_EQ(replica.installSnapshot(1,{{1,0,1,{1}}}),SharedReplicaStatus::Applied);
    CHECK_INT_EQ(replica.applyDelta(1,{1,0,3,{3}}),SharedReplicaStatus::Gap);
    CHECK_INT_EQ(replica.get(1)->revision,1);
    CHECK_INT_EQ(replica.installSnapshot(1,{{1,0,0,{4}}}),SharedReplicaStatus::Invalid);
    CHECK_INT_EQ(replica.installSnapshot(1,{{1,0,2,{2}},{1,0,2,{2}}}),
                 SharedReplicaStatus::Invalid);
    CHECK_INT_EQ(replica.installSnapshot(1,{{1,0,2,{2}}}),SharedReplicaStatus::Applied);
    CHECK_INT_EQ(replica.applyDelta(1,{1,0,3,{3}}),SharedReplicaStatus::Applied);
    CHECK_INT_EQ(replica.installSnapshot(1,{{1,0,2,{2}}}),SharedReplicaStatus::Stale);
    CHECK_INT_EQ(replica.installSnapshot(1,{}),SharedReplicaStatus::Stale);
    CHECK_INT_EQ(replica.get(1)->revision,3);
}
TEST_CASE(epoch_and_conflicting_replay_cannot_corrupt_object_state) {
    SharedObjectReplica replica;
    CHECK_INT_EQ(replica.installSnapshot(5,{{10,8,4,{8}}}),SharedReplicaStatus::Applied);
    CHECK_INT_EQ(replica.applyDelta(4,{10,8,5,{9}}),SharedReplicaStatus::WrongEpoch);
    CHECK_INT_EQ(replica.applyDelta(5,{10,8,4,{9}}),SharedReplicaStatus::Invalid);
    CHECK_INT_EQ(replica.applyDelta(5,{10,8,3,{7}}),SharedReplicaStatus::Stale);
    CHECK_INT_EQ(replica.applyDelta(5,{11,0,2,{1}}),SharedReplicaStatus::Gap);
    CHECK_INT_EQ(replica.installSnapshot(4,{{10,8,5,{9}}}),SharedReplicaStatus::Stale);
    CHECK_INT_EQ(replica.get(10)->bytes[0],8);
    CHECK_INT_EQ(replica.installSnapshot(6,{}),SharedReplicaStatus::Stale);
    CHECK_INT_EQ(replica.installSnapshot(6,{{10,0,3,{9}}}),SharedReplicaStatus::Stale);
    CHECK_INT_EQ(replica.installSnapshot(6,{{10,0,5,{9}}}),SharedReplicaStatus::Applied);
    CHECK_INT_EQ(replica.applyDelta(5,{10,0,6,{10}}),SharedReplicaStatus::WrongEpoch);
    CHECK_INT_EQ(replica.get(10)->ownerConnection,0);
}
TEST_CASE(tombstone_delta_and_snapshot_prevent_resurrection) {
    SharedObjectAuthority authority;
    CHECK(authority.beginEpoch(5));
    CHECK(authority.publish(8,9,{8}));
    SharedObjectReplica replica;
    CHECK_INT_EQ(replica.installSnapshot(5,authority.snapshot()),SharedReplicaStatus::Applied);
    CHECK_INT_EQ(authority.apply({5,9,1,8,1,SharedObjectAction::Retire}).status,
                 SharedObjectStatus::Applied);
    const auto retired=*authority.get(8);
    CHECK_INT_EQ(replica.applyDelta(5,retired),SharedReplicaStatus::Applied);
    CHECK_INT_EQ(replica.applyDelta(5,retired),SharedReplicaStatus::Replayed);
    CHECK_INT_EQ(replica.size(),1);
    CHECK_INT_EQ(replica.activeCount(),0);
    CHECK(replica.get(8)->retired);
    CHECK_INT_EQ(replica.applyDelta(5,{8,0,3,{9}}),SharedReplicaStatus::Invalid);
    CHECK_INT_EQ(replica.installSnapshot(5,{{8,0,3,{9}}}),SharedReplicaStatus::Invalid);
    CHECK_INT_EQ(replica.installSnapshot(5,{}),SharedReplicaStatus::Stale);
    CHECK(authority.beginEpoch(6));
    CHECK_INT_EQ(replica.installSnapshot(6,authority.snapshot()),SharedReplicaStatus::Applied);
    CHECK(replica.get(8)->retired);
    CHECK_INT_EQ(replica.installSnapshot(6,{{8,1,3,{},true}}),SharedReplicaStatus::Invalid);
}
TEST_SUITE_END
