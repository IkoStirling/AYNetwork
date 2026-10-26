#include <AYTest.h>
#include <AYNetwork/Replication/SharedObjectSnapshotAssembler.h>

using namespace ayt::net;

TEST_SUITE(SharedObjectWireTests)
TEST_CASE(snapshot_and_delta_round_trip_preserve_tombstone) {
    SharedObjectSnapshotPage page{7, 11, 1, 2, SharedObjectState{99, 0, 4, {}, true}};
    std::vector<std::uint8_t> bytes;
    CHECK(SharedObjectWireCodec::encodeSnapshotPage(page, bytes));
    CHECK(bytes.size() < SharedObjectWireCodec::MaxBodyBytes);
    SharedObjectSnapshotPage decoded;
    CHECK(SharedObjectWireCodec::decodeSnapshotPage(bytes.data(), bytes.size(), decoded));
    CHECK_INT_EQ(decoded.epoch, 7);
    CHECK_INT_EQ(decoded.snapshotId, 11);
    CHECK_INT_EQ(decoded.index, 1);
    CHECK(decoded.state->retired);
    CHECK_INT_EQ(decoded.state->revision, 4);

    SharedObjectDelta delta{7, *decoded.state};
    CHECK(SharedObjectWireCodec::encodeDelta(delta, bytes));
    SharedObjectDelta decodedDelta;
    CHECK(SharedObjectWireCodec::decodeDelta(bytes.data(), bytes.size(), decodedDelta));
    CHECK_INT_EQ(decodedDelta.epoch, 7);
    CHECK(decodedDelta.state.retired);
    CHECK_INT_EQ(decodedDelta.state.objectId, 99);
}

TEST_CASE(wire_rejects_malformed_and_preserves_output) {
    SharedObjectSnapshotPage source{3, 8, 0, 1, SharedObjectState{1, 0, 1, {4, 5}}};
    std::vector<std::uint8_t> bytes;
    CHECK(SharedObjectWireCodec::encodeSnapshotPage(source, bytes));
    SharedObjectSnapshotPage out{99, 99, 0, 1, {}};
    for (std::size_t n = 0; n < bytes.size(); ++n) {
        CHECK(!SharedObjectWireCodec::decodeSnapshotPage(bytes.data(), n, out));
        CHECK_INT_EQ(out.epoch, 99);
    }
    auto bad = bytes;
    bad[4] = 2;
    CHECK(!SharedObjectWireCodec::decodeSnapshotPage(bad.data(), bad.size(), out));
    bad = bytes;
    bad[6] = 1;
    CHECK(!SharedObjectWireCodec::decodeSnapshotPage(bad.data(), bad.size(), out));
    bad = bytes;
    bad.push_back(0);
    CHECK(!SharedObjectWireCodec::decodeSnapshotPage(bad.data(), bad.size(), out));
    bad = bytes;
    bad[8 + 8 + 8 + 2 + 2] = 2;
    CHECK(!SharedObjectWireCodec::decodeSnapshotPage(bad.data(), bad.size(), out));
    CHECK_INT_EQ(out.epoch, 99);
    CHECK(!SharedObjectWireCodec::encodeSnapshotPage({3,8,1,1,source.state}, bad));
    CHECK_INT_EQ(bad.size(), bytes.size());
}

TEST_CASE(maximum_payload_fits_packet_body_and_rejects_oversize) {
    SharedObjectState state{42, 0, 1,
        std::vector<std::uint8_t>(SharedObjectAuthority::MaxStateBytes, 0xAB)};
    std::vector<std::uint8_t> bytes;
    CHECK(SharedObjectWireCodec::encodeSnapshotPage({1,1,0,1,state}, bytes));
    CHECK_INT_EQ(bytes.size(), SharedObjectWireCodec::MaxBodyBytes);
    CHECK(bytes.size() <= 0xFFFFu);
    SharedObjectSnapshotPage decoded;
    CHECK(SharedObjectWireCodec::decodeSnapshotPage(bytes.data(), bytes.size(), decoded));
    CHECK_INT_EQ(decoded.state->bytes.size(), SharedObjectAuthority::MaxStateBytes);
    state.bytes.push_back(1);
    CHECK(!SharedObjectWireCodec::encodeSnapshotPage({1,1,0,1,state}, bytes));
    CHECK_INT_EQ(bytes.size(), SharedObjectWireCodec::MaxBodyBytes);
}

TEST_CASE(out_of_order_pages_publish_only_complete_snapshot) {
    SharedObjectAuthority authority;
    CHECK(authority.beginEpoch(2));
    CHECK(authority.publish(1, 0, {1}));
    CHECK(authority.publish(2, 0, {2}));
    auto states = authority.snapshot();
    SharedObjectReplica replica;
    SharedObjectSnapshotAssembler assembler;
    SharedObjectSnapshotPage first{2, 5, 0, 2, states[0]};
    SharedObjectSnapshotPage second{2, 5, 1, 2, states[1]};
    CHECK_INT_EQ(assembler.accept(second, replica), SharedSnapshotReceiveStatus::Pending);
    CHECK_INT_EQ(replica.size(), 0);
    CHECK_INT_EQ(assembler.accept(second, replica), SharedSnapshotReceiveStatus::Replayed);
    CHECK_INT_EQ(assembler.accept(first, replica), SharedSnapshotReceiveStatus::Applied);
    CHECK_INT_EQ(replica.size(), 2);
    CHECK_INT_EQ(assembler.accept(first, replica), SharedSnapshotReceiveStatus::Stale);
    CHECK_INT_EQ(assembler.pendingPages(), 0);
}

TEST_CASE(conflicting_pages_and_incomplete_resync_leave_replica_unchanged) {
    SharedObjectReplica replica;
    CHECK_INT_EQ(replica.installSnapshot(1, {{1,0,1,{1}}}), SharedReplicaStatus::Applied);
    SharedObjectSnapshotAssembler assembler;
    SharedObjectSnapshotPage page{1, 10, 0, 2, SharedObjectState{1,0,2,{2}}};
    CHECK_INT_EQ(assembler.accept(page, replica), SharedSnapshotReceiveStatus::Pending);
    auto conflict = page;
    conflict.state->bytes = {9};
    CHECK_INT_EQ(assembler.accept(conflict, replica), SharedSnapshotReceiveStatus::Invalid);
    CHECK_INT_EQ(replica.get(1)->revision, 1);
    CHECK_INT_EQ(assembler.accept({1,11,0,1,SharedObjectState{1,0,3,{3}}}, replica),
                 SharedSnapshotReceiveStatus::Applied);
    CHECK_INT_EQ(replica.get(1)->revision, 3);
    CHECK_INT_EQ(assembler.accept({1,10,1,2,SharedObjectState{2,0,1,{2}}}, replica),
                 SharedSnapshotReceiveStatus::Stale);
}

TEST_CASE(empty_snapshot_and_tombstone_survive_epoch_change) {
    SharedObjectReplica replica;
    SharedObjectSnapshotAssembler assembler;
    CHECK_INT_EQ(assembler.accept({1,1,0,1,{}}, replica),
                 SharedSnapshotReceiveStatus::Applied);
    CHECK_INT_EQ(replica.size(), 0);
    CHECK_INT_EQ(assembler.accept({1,2,0,1,SharedObjectState{5,0,1,{5}}}, replica),
                 SharedSnapshotReceiveStatus::Applied);
    CHECK_INT_EQ(assembler.accept({1,3,0,1,SharedObjectState{5,0,2,{},true}}, replica),
                 SharedSnapshotReceiveStatus::Applied);
    CHECK_INT_EQ(replica.activeCount(), 0);
    CHECK_INT_EQ(assembler.accept({2,1,0,1,SharedObjectState{5,0,3,{5}}}, replica),
                 SharedSnapshotReceiveStatus::Invalid);
    CHECK_INT_EQ(replica.get(5)->revision, 2);
    CHECK_INT_EQ(assembler.accept({2,2,0,1,SharedObjectState{5,0,2,{},true}}, replica),
                 SharedSnapshotReceiveStatus::Applied);
}
}
