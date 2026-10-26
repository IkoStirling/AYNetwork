#pragma once

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <deque>
#include <limits>
#include <optional>
#include <unordered_set>
#include <utility>
#include <vector>

namespace ayt::net {

struct HistoryVec3 { float x = 0, y = 0, z = 0; };
struct HistoryAabb {
    std::uint32_t netId = 0;
    HistoryVec3 min{}, max{};
};
/// Historical capsule in world space. A coincident axis is a sphere.
struct HistoryCapsule {
    std::uint32_t netId = 0;
    HistoryVec3 a{}, b{};
    float radius = 0;
};
struct HistoryRay { HistoryVec3 origin{}, direction{}; float maxDistance = 0; };
struct HistoryRayCandidate { std::uint32_t netId = 0; float distance = 0; };
enum class HistoryQueryStatus {
    Ready, InvalidRay, WrongEpoch, MissingTick, TooOld,
    InvalidWindow, OutsideWindow
};
struct HistoryRayResult {
    HistoryQueryStatus status = HistoryQueryStatus::MissingTick;
    std::vector<HistoryRayCandidate> candidates;
};

/// Bounded authority-side history of gameplay-supplied collision proxies.
/// This is a broad-phase candidate query, not a damage or visibility verdict.
/// A session epoch change clears all samples; capture once per server tick.
class ServerHitHistory {
public:
    static constexpr std::size_t MaxProxies = 4096;
    static constexpr std::uint32_t MaxTicks = 128;

    explicit ServerHitHistory(std::uint32_t retainedTicks = 32) noexcept
        : _retainedTicks(std::clamp(retainedTicks, 1u, MaxTicks)) {}

    /// Validate a complete sample before mutation. Duplicate IDs, nonfinite
    /// or inverted bounds, zero epoch, and non-forward ticks are rejected.
    bool capture(std::uint64_t epoch, std::uint32_t tick,
                 std::vector<HistoryAabb> proxies) {
        return capture(epoch, tick, std::move(proxies), {});
    }

    /// Capture a complete mixed set. IDs are unique across AABBs and capsules;
    /// invalid shapes reject the entire tick without changing retained history.
    bool capture(std::uint64_t epoch, std::uint32_t tick,
                 std::vector<HistoryAabb> proxies,
                 std::vector<HistoryCapsule> capsules) {
        if (!epoch || proxies.size() > MaxProxies ||
            capsules.size() > MaxProxies - proxies.size()) return false;
        std::unordered_set<std::uint32_t> ids;
        ids.reserve(proxies.size() + capsules.size());
        for (const auto& p : proxies) {
            if (!p.netId || !ids.insert(p.netId).second ||
                !valid(p.min) || !valid(p.max) ||
                p.min.x > p.max.x || p.min.y > p.max.y || p.min.z > p.max.z)
                return false;
        }
        for (const auto& p : capsules) {
            if (!p.netId || !ids.insert(p.netId).second ||
                !valid(p.a) || !valid(p.b) ||
                !std::isfinite(p.radius) || p.radius <= 0)
                return false;
        }
        if (_epoch && epoch < _epoch) return false;
        if (!_frames.empty() && epoch == _epoch) {
            const auto delta = static_cast<std::uint32_t>(tick - _frames.back().tick);
            if (!delta || delta >= 0x8000'0000u) return false;
        }
        if (epoch != _epoch) {
            _frames.clear();
            _epoch = epoch;
        }
        std::sort(proxies.begin(), proxies.end(), [](const auto& a, const auto& b) {
            return a.netId < b.netId;
        });
        std::sort(capsules.begin(), capsules.end(), [](const auto& a, const auto& b) {
            return a.netId < b.netId;
        });
        _frames.push_back({tick, std::move(proxies), std::move(capsules)});
        while (_frames.size() > _retainedTicks ||
               static_cast<std::uint32_t>(tick - _frames.front().tick) >= _retainedTicks)
            _frames.pop_front();
        return true;
    }

    void clear() noexcept { _frames.clear(); _epoch = 0; }
    std::uint64_t epoch() const noexcept { return _epoch; }
    std::size_t size() const noexcept { return _frames.size(); }

    /// Query only an exact retained server tick; no extrapolation or
    /// interpolation can silently change a competitive hit decision. Results
    /// are nearest-first, then NetId. The direction must be unit length.
    HistoryRayResult queryRay(std::uint64_t epoch, std::uint32_t tick,
                              const HistoryRay& ray) const {
        if (!validRay(ray))
            return {HistoryQueryStatus::InvalidRay, {}};
        if (!epoch || epoch != _epoch) return {HistoryQueryStatus::WrongEpoch, {}};
        if (_frames.empty()) return {HistoryQueryStatus::MissingTick, {}};
        const auto age = static_cast<std::uint32_t>(_frames.back().tick - tick);
        if (age < 0x8000'0000u && age >= _retainedTicks)
            return {HistoryQueryStatus::TooOld, {}};
        for (const auto& frame : _frames) {
            if (frame.tick != tick) continue;
            HistoryRayResult result{HistoryQueryStatus::Ready, {}};
            for (const auto& proxy : frame.proxies) {
                const auto distance = intersect(ray, proxy);
                if (distance) result.candidates.push_back({proxy.netId, *distance});
            }
            for (const auto& capsule : frame.capsules) {
                const auto distance = intersect(ray, capsule);
                if (distance) result.candidates.push_back({capsule.netId, *distance});
            }
            std::sort(result.candidates.begin(), result.candidates.end(),
                      [](const auto& a, const auto& b) {
                          if (a.distance != b.distance) return a.distance < b.distance;
                          return a.netId < b.netId;
                      });
            return result;
        }
        return {HistoryQueryStatus::MissingTick, {}};
    }

    /// Apply a server-owned rewind limit measured from the latest captured
    /// tick before querying. The caller still authenticates the actor and maps
    /// its input time to a server tick. maxAgeTicks is 0..retainedTicks-1.
    HistoryRayResult queryRayRecent(std::uint64_t epoch, std::uint32_t tick,
                                    std::uint32_t maxAgeTicks,
                                    const HistoryRay& ray) const {
        if (!validRay(ray)) return {HistoryQueryStatus::InvalidRay, {}};
        if (!epoch || epoch != _epoch) return {HistoryQueryStatus::WrongEpoch, {}};
        if (maxAgeTicks >= _retainedTicks)
            return {HistoryQueryStatus::InvalidWindow, {}};
        if (_frames.empty()) return {HistoryQueryStatus::MissingTick, {}};
        const auto age = static_cast<std::uint32_t>(_frames.back().tick - tick);
        if (age >= 0x8000'0000u || age > maxAgeTicks)
            return {HistoryQueryStatus::OutsideWindow, {}};
        return queryRay(epoch, tick, ray);
    }

private:
    struct Frame {
        std::uint32_t tick;
        std::vector<HistoryAabb> proxies;
        std::vector<HistoryCapsule> capsules;
    };
    static bool valid(HistoryVec3 v) noexcept {
        return std::isfinite(v.x) && std::isfinite(v.y) && std::isfinite(v.z);
    }
    static bool validRay(const HistoryRay& ray) noexcept {
        return valid(ray.origin) && valid(ray.direction) &&
               std::isfinite(ray.maxDistance) && ray.maxDistance > 0 &&
               std::fabs(lengthSquared(ray.direction) - 1.0f) <= 0.001f;
    }
    static float lengthSquared(HistoryVec3 v) noexcept {
        return v.x * v.x + v.y * v.y + v.z * v.z;
    }
    struct Vec3d { double x, y, z; };
    static Vec3d wide(HistoryVec3 v) noexcept { return {v.x, v.y, v.z}; }
    static double dot(Vec3d a, Vec3d b) noexcept {
        return a.x * b.x + a.y * b.y + a.z * b.z;
    }
    static Vec3d subtract(HistoryVec3 a, HistoryVec3 b) noexcept {
        return {static_cast<double>(a.x) - b.x,
                static_cast<double>(a.y) - b.y,
                static_cast<double>(a.z) - b.z};
    }
    static std::optional<float> intersect(const HistoryRay& ray,
                                          const HistoryCapsule& capsule) noexcept {
        const auto axis = subtract(capsule.b, capsule.a);
        const auto fromA = subtract(ray.origin, capsule.a);
        const double axisLength2 = dot(axis, axis);
        const auto direction = wide(ray.direction);
        const double projection = axisLength2 > 0
            ? std::clamp(dot(fromA, axis) / axisLength2, 0.0, 1.0) : 0.0;
        const double dx = fromA.x - projection * axis.x;
        const double dy = fromA.y - projection * axis.y;
        const double dz = fromA.z - projection * axis.z;
        const double radius2 = static_cast<double>(capsule.radius) * capsule.radius;
        if (dx * dx + dy * dy + dz * dz <= radius2) return 0.0f;

        double closest = std::numeric_limits<double>::infinity();
        const double maxDistance = ray.maxDistance;
        auto considerSphere = [&](HistoryVec3 center, bool lowerCap) {
            const auto offset = subtract(ray.origin, center);
            const double halfB = dot(offset, direction);
            const double c = dot(offset, offset) - radius2;
            const double discriminant = halfB * halfB - c;
            if (discriminant < 0) return;
            const double root = std::sqrt(discriminant);
            for (double t : {-halfB - root, -halfB + root}) {
                if (t < 0 || t > maxDistance) continue;
                const double axial = dot(fromA, axis) +
                                     t * dot(direction, axis);
                if ((lowerCap && axial <= 0) ||
                    (!lowerCap && axial >= axisLength2))
                    closest = std::min(closest, t);
            }
        };
        considerSphere(capsule.a, true);
        if (axisLength2 > 0) considerSphere(capsule.b, false);

        if (axisLength2 > 0) {
            const double alongRay = dot(direction, axis);
            const double alongOrigin = dot(fromA, axis);
            const double a = axisLength2 * dot(direction, direction) -
                             alongRay * alongRay;
            const double halfB = axisLength2 * dot(fromA, direction) -
                                 alongOrigin * alongRay;
            const double c = axisLength2 * (dot(fromA, fromA) - radius2) -
                             alongOrigin * alongOrigin;
            const double discriminant = halfB * halfB - a * c;
            if (a > 0 && discriminant >= 0) {
                const double root = std::sqrt(discriminant);
                for (double t : {(-halfB - root) / a, (-halfB + root) / a}) {
                    if (t < 0 || t > maxDistance) continue;
                    const double axial = alongOrigin + t * alongRay;
                    if (axial >= 0 && axial <= axisLength2)
                        closest = std::min(closest, t);
                }
            }
        }
        if (!std::isfinite(closest)) return std::nullopt;
        return static_cast<float>(closest);
    }
    static std::optional<float> intersect(const HistoryRay& ray,
                                           const HistoryAabb& box) noexcept {
        double near = 0, far = ray.maxDistance;
        const float origins[3] = {ray.origin.x, ray.origin.y, ray.origin.z};
        const float directions[3] = {ray.direction.x, ray.direction.y, ray.direction.z};
        const float mins[3] = {box.min.x, box.min.y, box.min.z};
        const float maxs[3] = {box.max.x, box.max.y, box.max.z};
        for (int axis = 0; axis < 3; ++axis) {
            if (directions[axis] == 0) {
                if (origins[axis] < mins[axis] || origins[axis] > maxs[axis])
                    return std::nullopt;
                continue;
            }
            auto first = (static_cast<double>(mins[axis]) - origins[axis]) /
                         directions[axis];
            auto last = (static_cast<double>(maxs[axis]) - origins[axis]) /
                        directions[axis];
            if (first > last) std::swap(first, last);
            near = std::max(near, first);
            far = std::min(far, last);
            if (near > far) return std::nullopt;
        }
        return static_cast<float>(near);
    }

    std::deque<Frame> _frames;
    std::uint32_t _retainedTicks;
    std::uint64_t _epoch = 0;
};

} // namespace ayt::net
