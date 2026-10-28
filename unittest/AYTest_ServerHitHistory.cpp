#include <AYTest.h>
#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
// Exercise the public header after the platform header, as Editor consumers
// do. Windows defines near/far macros even in a 64-bit build.
#include <windows.h>
#endif
#include <AYNetwork/Replication/ServerHitHistory.h>
#include <array>
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
TEST_CASE(capsule_query_uses_exact_surface_and_mixes_with_boxes) {
    ServerHitHistory history;
    CHECK(history.capture(1, 8,
        {{9, {6, -1, -1}, {7, 1, 1}}},
        {{2, {4, -1, 0}, {4, 1, 0}, 1},
         {3, {10, 0, 0}, {10, 0, 0}, 1},
         {4, {4, 3, 0}, {4, 4, 0}, 1}}));
    const auto alongX = history.queryRay(1, 8, {{0, 0, 0}, {1, 0, 0}, 12});
    CHECK_INT_EQ(alongX.status, HistoryQueryStatus::Ready);
    CHECK_INT_EQ(alongX.candidates.size(), 3);
    CHECK_INT_EQ(alongX.candidates[0].netId, 2);
    CHECK_INT_EQ(alongX.candidates[0].distance, 3);
    CHECK_INT_EQ(alongX.candidates[1].netId, 9);
    CHECK_INT_EQ(alongX.candidates[2].netId, 3);
    CHECK_INT_EQ(alongX.candidates[2].distance, 9);

    const auto inside = history.queryRay(1, 8, {{4, 0, 0}, {0, 1, 0}, 3});
    CHECK_INT_EQ(inside.candidates[0].netId, 2);
    CHECK_INT_EQ(inside.candidates[0].distance, 0);
    const auto cap = history.queryRay(1, 8, {{4, -4, 0}, {0, 1, 0}, 5});
    CHECK_INT_EQ(cap.candidates[0].netId, 2);
    CHECK_INT_EQ(cap.candidates[0].distance, 2);
    const auto outsideRange = history.queryRay(1, 8, {{0, 0, 0}, {1, 0, 0}, 2});
    CHECK_INT_EQ(outsideRange.candidates.size(), 0);
    const auto tangent = history.queryRay(1, 8, {{0, 0, 1}, {1, 0, 0}, 8});
    CHECK_INT_EQ(tangent.candidates[0].netId, 2);
    CHECK_INT_EQ(tangent.candidates[0].distance, 4);
    const auto missedRoundedEnd = history.queryRay(1, 8,
        {{0, 1.9f, 0.9f}, {1, 0, 0}, 5});
    CHECK_INT_EQ(missedRoundedEnd.candidates.size(), 0);
}
TEST_CASE(capsule_capture_rejects_invalid_shapes_and_duplicate_ids_atomically) {
    ServerHitHistory history;
    CHECK(history.capture(1, 1, {}, {{1, {0, 0, 0}, {0, 1, 0}, 1}}));
    CHECK_FALSE(history.capture(1, 2,
        {{2, {0, 0, 0}, {1, 1, 1}}},
        {{2, {0, 0, 0}, {0, 1, 0}, 1}}));
    CHECK_FALSE(history.capture(1, 2, {},
        {{3, {0, 0, 0}, {0, 1, 0}, 0}}));
    CHECK_FALSE(history.capture(1, 2, {},
        {{3, {0, 0, 0}, {0, 1, 0},
             std::numeric_limits<float>::infinity()}}));
    CHECK_INT_EQ(history.size(), 1);
    CHECK_INT_EQ(history.queryRay(1, 1, {{0, -3, 0}, {0, 1, 0}, 5})
                     .candidates[0].distance, 2);
}
TEST_CASE(server_owned_rewind_window_rejects_old_and_future_ticks) {
    ServerHitHistory history(8);
    const auto wrap = std::numeric_limits<std::uint32_t>::max();
    CHECK(history.capture(7, wrap - 1,
                          {}, {{1, {3, 0, 0}, {3, 1, 0}, 1}}));
    CHECK(history.capture(7, wrap, {}));
    CHECK(history.capture(7, 0, {}));
    const HistoryRay ray{{0, 0, 0}, {1, 0, 0}, 10};
    CHECK_INT_EQ(history.queryRayRecent(7, wrap - 1, 1, ray).status,
                 HistoryQueryStatus::OutsideWindow);
    const auto permitted = history.queryRayRecent(7, wrap - 1, 2, ray);
    CHECK_INT_EQ(permitted.status, HistoryQueryStatus::Ready);
    CHECK_INT_EQ(permitted.candidates[0].netId, 1);
    CHECK_INT_EQ(history.queryRayRecent(7, 1, 2, ray).status,
                 HistoryQueryStatus::OutsideWindow);
    CHECK_INT_EQ(history.queryRayRecent(7, 0, 8, ray).status,
                 HistoryQueryStatus::InvalidWindow);
    CHECK_INT_EQ(history.queryRayRecent(6, 0, 2, ray).status,
                 HistoryQueryStatus::WrongEpoch);
    CHECK_INT_EQ(history.queryRayRecent(7, 0, 2,
                 {{0, 0, 0}, {2, 0, 0}, 10}).status,
                 HistoryQueryStatus::InvalidRay);
    CHECK_INT_EQ(history.queryRayRecent(7, wrap, 2, ray).status,
                 HistoryQueryStatus::Ready);
}
TEST_CASE(static_occluders_filter_only_candidates_behind_first_blocker) {
    ServerHitHistory history(4);
    CHECK(history.capture(9,10,
        {{1,{2,-1,-1},{3,1,1}},
         {2,{6,-1,-1},{7,1,1}},
         {3,{8,-1,-1},{9,1,1}}}));
    const HistoryRay ray{{0,0,0},{1,0,0},12};
    const std::array<HistoryStaticOccluder,1> wall{{
        {{6,-2,-2},{6.5f,2,2}}}};
    const auto visible=history.queryRayRecentOccluded(9,10,2,ray,wall);
    CHECK_INT_EQ(visible.status,HistoryQueryStatus::Ready);
    CHECK_INT_EQ(visible.candidates.size(),1);
    CHECK_INT_EQ(visible.candidates[0].netId,1);
    CHECK_INT_EQ(history.queryRayRecentOccluded(9,10,2,ray,{}).candidates.size(),3);
    const std::array<HistoryStaticOccluder,1> offRay{{
        {{4,3,-1},{5,4,1}}}};
    CHECK_INT_EQ(history.queryRayRecentOccluded(9,10,2,ray,offRay)
                     .candidates.size(),3);
    const std::array<HistoryStaticOccluder,1> inside{{
        {{-1,-1,-1},{1,1,1}}}};
    CHECK_INT_EQ(history.queryRayRecentOccluded(9,10,2,ray,inside)
                     .candidates.size(),0);
    const std::array<HistoryStaticOccluder,1> invalid{{
        {{4,0,0},{3,1,1}}}};
    CHECK_INT_EQ(history.queryRayRecentOccluded(9,10,2,ray,invalid).status,
                 HistoryQueryStatus::InvalidOccluder);
    CHECK_INT_EQ(history.queryRayRecentOccluded(9,10,0,ray,wall).status,
                 HistoryQueryStatus::Ready);
    CHECK(history.capture(9,11,{}));
    CHECK_INT_EQ(history.queryRayRecentOccluded(9,10,0,ray,wall).status,
                 HistoryQueryStatus::OutsideWindow);
}
TEST_SUITE_END
