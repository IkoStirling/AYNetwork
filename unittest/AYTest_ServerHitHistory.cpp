#include <AYTest.h>
#include <AYNetwork/Replication/ServerHitHistory.h>
#include <limits>

using namespace ayt::net;
TEST_SUITE(ServerHitHistoryTests)
TEST_CASE(exact_tick_query_returns_sorted_dynamic_candidates) {
    ServerHitHistory history(4);
    CHECK(history.capture(1, 10, {
        {3, {8, -1, -1}, {9, 1, 1}},
        {2, {4, -1, -1}, {5, 1, 1}},
        {1, {4, -1, -1}, {5, 1, 1}},
        {4, {4, 3, -1}, {5, 4, 1}}
    }));
    const auto result = history.queryRay(1, 10, {{0, 0, 0}, {1, 0, 0}, 20});
    CHECK_INT_EQ(result.status, HistoryQueryStatus::Ready);
    CHECK_INT_EQ(result.candidates.size(), 3);
    CHECK_INT_EQ(result.candidates[0].netId, 1);
    CHECK_INT_EQ(result.candidates[1].netId, 2);
    CHECK_INT_EQ(result.candidates[2].netId, 3);
    CHECK_INT_EQ(result.candidates[0].distance, 4);
    CHECK_INT_EQ(history.queryRay(1, 11, {{0, 0, 0}, {1, 0, 0}, 20}).status,
                 HistoryQueryStatus::MissingTick);
}
TEST_CASE(validation_and_epoch_failure_leave_history_unchanged) {
    ServerHitHistory history;
    CHECK(history.capture(2, 10, {{1, {0, 0, 0}, {1, 1, 1}}}));
    CHECK_FALSE(history.capture(2, 11, {{1, {0, 0, 0}, {1, 1, 1}},
                                       {1, {2, 0, 0}, {3, 1, 1}}}));
    CHECK_FALSE(history.capture(2, 11, {{1, {2, 0, 0}, {1, 1, 1}}}));
    CHECK_FALSE(history.capture(1, 11, {}));
    CHECK_FALSE(history.capture(2, 10, {}));
    CHECK_INT_EQ(history.size(), 1);
    CHECK_INT_EQ(history.epoch(), 2);
    CHECK_INT_EQ(history.queryRay(1, 10, {{0, 0, 0}, {1, 0, 0}, 10}).status,
                 HistoryQueryStatus::WrongEpoch);
    CHECK_INT_EQ(history.queryRay(2, 10, {{0, 0, 0}, {2, 0, 0}, 10}).status,
                 HistoryQueryStatus::InvalidRay);
    CHECK(history.capture(3, 1, {{2, {3, -1, -1}, {4, 1, 1}}}));
    CHECK_INT_EQ(history.epoch(), 3);
    CHECK_INT_EQ(history.size(), 1);
    CHECK_INT_EQ(history.queryRay(3, 10, {{0, 0, 0}, {1, 0, 0}, 10}).status,
                 HistoryQueryStatus::MissingTick);
}
TEST_CASE(age_bound_and_tick_wrap) {
    ServerHitHistory history(3);
    const auto beforeWrap = std::numeric_limits<std::uint32_t>::max();
    CHECK(history.capture(1, beforeWrap - 1, {{1, {-1, -1, -1}, {1, 1, 1}}}));
    CHECK(history.capture(1, beforeWrap, {{2, {-1, -1, -1}, {1, 1, 1}}}));
    CHECK(history.capture(1, 0, {{3, {-1, -1, -1}, {1, 1, 1}}}));
    CHECK_INT_EQ(history.size(), 3);
    CHECK(history.capture(1, 1, {{4, {-1, -1, -1}, {1, 1, 1}}}));
    CHECK_INT_EQ(history.size(), 3);
    CHECK_INT_EQ(history.queryRay(1, beforeWrap - 1,
                 {{0, 0, -5}, {0, 0, 1}, 10}).status, HistoryQueryStatus::TooOld);
    const auto hit = history.queryRay(1, 1, {{0, 0, -5}, {0, 0, 1}, 10});
    CHECK_INT_EQ(hit.status, HistoryQueryStatus::Ready);
    CHECK_INT_EQ(hit.candidates[0].netId, 4);
}
TEST_CASE(reverse_ray_inside_box_and_invalid_sample_are_bounded) {
    ServerHitHistory history(2);
    CHECK(history.capture(1, 5, {{1, {-1, -1, -1}, {1, 1, 1}},
                                {2, {-5, -1, -1}, {-4, 1, 1}}}));
    const auto inside = history.queryRay(1, 5, {{0, 0, 0}, {-1, 0, 0}, 10});
    CHECK_INT_EQ(inside.status, HistoryQueryStatus::Ready);
    CHECK_INT_EQ(inside.candidates.size(), 2);
    CHECK_INT_EQ(inside.candidates[0].netId, 1);
    CHECK_INT_EQ(inside.candidates[0].distance, 0);
    CHECK_INT_EQ(inside.candidates[1].distance, 4);
    CHECK_FALSE(history.capture(1, 6, {{3, {0, 0, 0},
        {std::numeric_limits<float>::infinity(), 1, 1}}}));
    CHECK_INT_EQ(history.size(), 1);
    CHECK_INT_EQ(history.queryRay(1, 5, {{0, 0, 0}, {1, 0, 0}, 0}).status,
                 HistoryQueryStatus::InvalidRay);
}
TEST_SUITE_END
