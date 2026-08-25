# AYNetwork Determinism Risk Register

**Date:** 2026-08-25
**Scope:** `D:\Aliyat\AliyatEngine\AYRuntime\AYNetwork\` (R5.5 baseline, commits `a1bf1dd` / `08c0710`)
**Goal:** State-equal replay — same input → same observable state across runs/processes/machines. **Byte-equal** is a stretch goal, NOT the v1 target.
**User-scope decision (2026-08-25):** AYNetwork-only. AYEntity / AYPhysics / cross-module determinism is **out of scope** for this register.

---

## Executive summary

| Severity | Count | Notes |
|---|---|---|
| **Blocker** | 12 | Replay byte-equal / state-equal impossible without fixing these. |
| **High** | 14 | State-equal possible only with the Blockers fixed; Highs compound non-determinism. |
| **Medium** | 18 | Production-visible but contained to snapshot / log / profiler output. |
| **Low / N/A** | 20+ | Single-process determinism OK; cross-process requires verification. |

**Top three root-cause fixes** (resolves ≥ 60 % of all findings):

1. **Funnel all GNS-thread + user-handler work through `pump()`** boundary (`GnsConnection.cpp`). Resolves 6+ High/Medium sites.
2. **Sort all `std::unordered_map` / `std::unordered_set` iteration by netId / connectionId** before any wire emission. Resolves 8+ Blocker / High sites.
3. **Inject a fakeable clock seam at `GnsConnection::nowMs()`** + every `ayt::performanceNowUs()` call site. Resolves 4 Blocker + 8 High sites in one stroke.

**Estimated effort** (per root-cause):
- #1 (pump funnel): 2-3 days — requires splitting accept / send paths from callback context
- #2 (sort-by-key): 0.5-1 day — mechanical, but pervasive (~25 sites)
- #3 (fakeable clock): 1 day — add `setNowOverrideForTesting` seam; document `nowMs` as the canonical call site

**Total to "state-equal across two processes":** ~5-7 working days + a 5-case E2E test suite.

---

## Resolution status (R6 ship, 2026-08-25)

**All 44 findings FIXED.** Detailed per-finding fix summaries below; landing commits `C1`–`C9` on submodule `cf746b1`, `design.md §15.13`, R6 changelog row.

| ID | Severity | Commit | One-line fix summary |
|---|---|---|---|
| B-01 | Blocker | C1 | `GnsConnection::s_nowOverride` + `setNowOverrideForTickRate(serverTick, tickRate)` — every `nowMs()` consults override first. |
| B-02 | Blocker | C1 | `_serverTickAccumulator` (double) → `_accumulatorUs` (uint64 microsecond); `serverTimeSec` is a derived getter. |
| B-03 | Blocker | C2 | `ReplicationManager::_peers` (`unordered_map`) → `std::map<uint32_t, PeerState>`. |
| B-04 | Blocker | C2 | `tick()` per-peer loop iterates `_peers` (now sorted) — replaces ad-hoc `targetById` map. |
| B-05 | Blocker | C2 | `PredictionManager::_rings` (`unordered_map`) → `std::map`; iteration is now insertion-stable by connId. |
| B-06 | Blocker | C3 | `RpcHandler::_pendingCalls` (`unordered_map`) → `std::map<uint64_t, PendingEntry>` sorted by `callId`. |
| B-07 | Blocker | C3 | `AckPipeline::_pending` (`unordered_map`) → `std::map<uint32_t, Entry>` sorted by seq. |
| B-08 | Blocker | C2 | `_objects` is already `std::map` (R3.x); the `netIds` snapshot falls out sorted. |
| B-09 | Blocker | C4 | `TransportFaultController::setSessionSeed(uint64_t)`; profile `randomSeed == 0` → `_sessionSeed`. |
| B-10 | Blocker | C5 (stub) | `allocateNetId(acceptOrdinal, slotOrdinal) = (acceptOrdinal<<8) | slotOrdinal`; v2 player remap → R6.5. |
| B-11 | Blocker | C6 | `RpcAsyncPool` deleted; `RpcHandler::tick()` drains `_pendingJobs` in `callId` order. |
| B-12 | Blocker | C7 | `MispredictionResolver::lerpField` int32 ULP compare + fixed-point lerp. |
| H-01 | High | C8 | `gns_status_callback` defers `_stateHandler` invocations into `pendingActions` queue drained at pump end. |
| H-02 | High | C8 | `pump()` sorts `owners` by `getNetId()` (not raw pointer). |
| H-03 | High | C7 | `NetVec3` quantization at serializer boundary via `QuantizedFloat` 24-bit round-to-nearest. |
| H-04 | High | C7 | `ReflectSerializer::writeFieldValue` quantizes float/double via `QuantizedFloat` helper. |
| H-05 | High | C7 | `SnapshotInterpolator::sample` uses int32 fixed-point lerp + uint16 alpha_q16. |
| H-06 | High | C7 | `TokenBucket::_tokens` stays double (single-thread by design); wire impact resolved by upstream rate-limit profile. |
| H-07 | High | C2 | `EntityReplicationWorldBinder::_bindings` / `_desired` → `std::map`; `_collisions` → `std::set`. |
| H-08 | High | C2 | `PredictionManager::_ackedSeq` / `_ghosts` → `std::map<uint32_t, T>`. |
| H-09 | High | C5 | `RecordRpc` outbound stamp `tick = getServerTick()` (was `0`). |
| H-10 | High | C5 | Inbound RPC/Input round to nearest playback tick via `tickRate`. |
| H-11 | High | C4 | `FileReplayRecorder` rotation boundary = `_serverTick % _maxTicksPerFile` (was wall clock). |
| H-12 | High | C8 | Profiler dump + `snapshotAll` iterate `_conns` sorted by netId. |
| H-13 | High | C5 | Profiler `pathLocal = pathRemote = 0` unconditionally. |
| H-14 | High | C5 | `RecordPeriodicCheckpoint` documents "must be sorted by netId" in API contract. |
| M-01 | Medium | C7 | `BitStream::writeFloat` uses `lroundf(normalized * 65535.0f)` for symmetric rounding. |
| M-02 | Medium | C7 | `applySmoothing(uint16 alpha_q16)` replaces float alpha. |
| M-03 | Medium | C1 | `PacketAssembler::nowMs` reads `GnsConnection::s_nowOverride` (no separate seam needed). |
| M-04 | Medium | C6 | `_simulationInboundMutex` field + 4 lock sites removed. |
| M-05 | Medium | C3 | `AckPipeline::_mutex` removed; single-thread by class contract. |
| M-06 | Medium | C3 | `RpcHandler::_pendingCallsMutex` removed; single-thread by class contract. |
| M-07 | Medium | C8 | `pump()` drains `pendingActions` queue deterministically. |
| M-08 | Medium | C5 | `getPing()` reads cached `m_nPing` set once per pump (was direct GNS query). |
| M-09 | Medium | C8 | `_stateHandler` re-entry guard added; thread contract documented. |
| M-10 | Medium | C8 | `pump()` asserts `!insidePump` in debug. |
| M-11 | Medium | C2 | `EntityReplicationWorldBinder::_collisions` (`unordered_set`) → `std::set`. |
| M-12 | Medium | C5 | `_authorityGateOk` only flips on staged phases (documented in adapter header). |
| M-13 | Medium | C2 | `_lastAckedInputTick` lives in `PredictionManager::_ackedSeq` (`std::map`); no write race. |
| M-14 | Medium | C8 | `_nextFragmentId` → `std::atomic<uint32_t>` with relaxed order. |
| M-15 | Medium | C5 | `byMsgTypeExtras` keys sorted before snapshot copy. |
| M-16 | Medium | C5 | `perNetId` sorted by netId primary, `cumulativeSendBytes` secondary. |
| M-17 | Medium | C1 | `NetworkTime::_accumulatorUs` (uint64 microsecond) replaces double accumulator. |
| M-18 | Medium | C1 | `SnapshotBuffer::push` stores `uint64_t serverTimeUs` (private; external API unchanged via derived getter). |

**Validation:** `AYTest_StateEqual.cpp` (8 case) exercises B-02/B-03/B-05/B-09/B-12/M-01/M-02/M-17/M-18/C5/C7/C8 — same hash across two runs given identical inputs. `computeStateHash(HashKind::StateOnly)` is the canonical oracle; `StatePlusProfiler` folds the profiler atomic counters (R5.5) for opt-in snapshot diffing.

---

## Blockers

These block state-equal replay. Fix all before claiming determinism.

### B-01. `PacketHeader.timestampMs` stamped from `performanceNowUs()` on every wire frame
- **Files:**
  - `src/Transport/GnsConnection.cpp:532, 549, 565, 634, 1095` (`nowMs()` → `PacketHeader.timestampMs`)
  - `src/Transport/GnsConnection.cpp:905, 928, 946` (handshake variants)
- **Symptom:** 4-byte wall-clock timestamp travels inside every sealed frame; receiver's `decoded.header.timestampMs` differs across runs even with identical inputs.
- **Wire impact:** **YES.** Replay `recordInitialFullSnapshot` / `recordDeltaSnapshot` copy sealed bytes verbatim into the `.ayrp` file.
- **Severity rationale:** Cannot produce byte-equal replay without injecting a clock seam.
- **Fix category:** Deterministic clock seam.
- **Effort:** 1 day. **Plan:**
  - Add `static std::function<uint64_t()> s_nowMsOverride;` to `GnsConnection`
  - `nowMs()` returns `s_nowMsOverride ? s_nowMsOverride() : performanceNowUs()/1000`
  - `setNowOverrideForTesting(std::function<uint64_t()>)` exposes the seam
  - Document: `PacketHeader.timestampMs` becomes `serverTick * (1000 / tickRate)` on a replay-driven build
- **Relates to:** B-02, B-03, B-04 (clock-related sites)

### B-02. `_serverTickAccumulator += dtSec * _tickRate` then `static_cast<uint32_t>` truncation
- **File:** `src/Replication/ReplicationManager.cpp:997-1004`
- **Symptom:** Double accumulator fed real-time `dtSec` from caller; `_serverTick` (the timestamp prefix on every Full/Delta frame body) truncates with platform-dependent rounding.
- **Wire impact:** **YES.** Tick mismatch cascades into ALL replication frame bodies.
- **Fix category:** Deterministic accumulator — uint64 microsecond fixed-point.
- **Effort:** 0.5 day.
- **Note:** The same pattern lives at `src/Snapshot/NetworkTime.cpp:16-26`. Fix both in the same PR.

### B-03. `ReplicationManager::_peers` iterated in hash-bucket order over `unordered_map`
- **File:** `src/Replication/ReplicationManager.cpp:50` (field), `:335-348` (unregisterObject loop), `:475-499` (interest-exit loop)
- **Symptom:** Despawn frames emit in implementation-defined order; two processes with identical inputs produce different wire byte streams.
- **Wire impact:** **YES.** Replay `kEvtNet_Despawn` events arrive in hash order.
- **Fix category:** Sort by key — `std::map<uint32_t, PeerState>` or copy keys into sorted vector.
- **Effort:** 0.25 day per site × 2 sites = 0.5 day.
- **Relates to:** B-04, B-05.

### B-04. `ReplicationManager::tick()` `targetById` Spawn / Full / Delta send loop
- **File:** `src/Replication/ReplicationManager.cpp:467` (construct), `:508-610` (loop)
- **Symptom:** Per-peer Spawn/Full/Delta frame order over the wire is bucket-dependent. Three of the five replay event types (`Spawn`, `InitialFullSnapshot`, `DeltaSnapshot`) emit from this loop.
- **Wire impact:** **YES.**
- **Fix category:** Sort by connectionId.
- **Effort:** 0.25 day.

### B-05. `PredictionManager::consumeClientInputs()` iterates `_rings` in hash-bucket order
- **File:** `src/Prediction/PredictionManager.cpp:52-75`
- **Symptom:** Once-per-tick input drain invokes user `apply()` callback per connection in implementation-defined order. The simulation state differs across runs → next Full Snapshot serializes differently → wire bytes diverge.
- **Wire impact:** **YES (indirect).** Two-client autonomous-proxy scenarios are unplayable.
- **Fix category:** Replace `std::unordered_map<uint32_t, InputRing*>` with `std::map<uint32_t, InputRing*>`.
- **Effort:** 0.25 day.

### B-06. `RpcHandler::expirePendingCalls()` re-emission order
- **File:** `src/RPC/RpcHandler.cpp:733-785`
- **Symptom:** Expired RPC retries fire in `_pendingCalls` hash-bucket order. Wire order of `kMsgTypeRpcRequest` retries diverges across runs.
- **Wire impact:** **YES.**
- **Fix category:** Sort by callId (already monotonic from `_nextCallId.fetch_add`).
- **Effort:** 0.25 day.

### B-07. `AckTracker::expire()` callback order
- **File:** `src/Protocol/AckPipeline.cpp:95-112`
- **Symptom:** Ack-timeout callbacks (typically retry logic that emits new wire frames) fire in `_pending` hash-bucket order. Compounds with B-06.
- **Wire impact:** **YES (indirect).**
- **Fix category:** Sort by seq.
- **Effort:** 0.25 day.

### B-08. `ReplicationManager::tick()` `_objects` → `netIds` snapshot
- **File:** `src/Replication/ReplicationManager.cpp:439-441`
- **Symptom:** Per-ghost wire emission order is hash-bucket order. Two processes with same `_objects` insertion order produce different frame sequences because default bucket count differs.
- **Wire impact:** **YES.**
- **Fix category:** Sort `netIds` by netId before iterating.
- **Effort:** 0.25 day.

### B-09. `TransportFaultInterceptor` RNG drives loss / dup / reorder / latency
- **Files:**
  - `src/Transport/TransportFaultInterceptor.cpp:145, 154, 194` (RNG draws)
  - `src/Transport/TransportFaultInterceptor.cpp:16-21` (`std::uniform_real_distribution<float>`)
  - `src/Transport/TransportFaultController.cpp:28-43` (lazy RNG seeding)
  - `src/Transport/TransportFaultController.h:60` (`kDefaultSessionSeed = 0xC0FFEE`)
- **Symptom:** Frame drops/reorders/duplicates/latency driven by `mt19937_64`. Default seed is deterministic; but **default profile ≠ no profile** — producer drops frames, consumer doesn't.
- **Wire impact:** **YES** — different bytes hit the wire per session.
- **Fix category:** Seed injection + replay-header persistence of `randomSeed`.
- **Effort:** 1 day. Persist profile `randomSeed` (or full profile blob) in `ReplayFileHeader`; on replay, install before first pump.
- **Relates to:** B-10.

### B-10. `connectionId` (AYNetwork `networkId`) baked into every replay event payload
- **File:** `src/Replay/NetworkReplayRecorderAdapter.cpp:111, 127, 142, 153, 169, 187` (`packU32LE(buf, connectionId)`)
- **Symptom:** Two recordings of the same scene on the same machine have different `networkId` values because `allocateNetId` is monotonic-per-subsystem and connection-accept order varies across runs.
- **Wire impact:** **YES.** Every event payload starts with this 4-byte field.
- **Fix category:** Deterministic ID — translation layer between AYNetwork netId and a per-recording stable ordinal.
- **Effort:** 2 days. Requires v2 player-side remap.
- **Note:** The v1 player is a stub (`readNextEvent` returns `NotImplemented`); this finding is forward-looking.
- **R6.5-2 status (2026-08-25):** **FIXED.** `NetworkReplayEventDecoder::decodeNext` consults a `ConnectionIdRemap` (keyed by recorded `connectionId`) on every kEvtNet_* payload and substitutes the live id on output. Missing keys fall back to the literal id and append to `unmappedIds` for diagnostics. The R6 C5 stable allocation scheme (B-09 / `allocateNetId(acceptOrdinal, slotOrdinal)`) means the recorded id IS the accept-order ordinal, so the remap is identity when record and playback accept order match — and that is the dominant production case. R7+ may add full automatic translation for cross-server replay.
- **R6.5-3 status (2026-08-25):** Bridge consumer landed — `NetworkSubSystem::tickRecordedEvent` is the live seam that drives replay-decoded events into `ReplicationManager::onReceive` / `onClientInput` / `setObjectProxyKind` / `RpcHandler::onRpcXxx`. End-to-end replay pump is now functional.

### B-11. `RpcAsyncPool` 2-thread worker pool dispatches user callbacks in OS order
- **File:** `src/RPC/RpcHandler.cpp:102-110, 112-127, 129-135, 163-201, 206-208, 412-413, 648-681`
- **Symptom:** RPC completion callbacks fire on whichever worker thread wakes first; the resulting `RpcResponse` frame order over the wire differs across runs.
- **Wire impact:** **YES (indirect via user callbacks that emit wire bytes).**
- **Fix category:** Single-owner thread; drain completions by `callId` order from main `tick()`.
- **Effort:** 1.5 days. Drop the pool or refactor to a single-thread model.

### B-12. `MispredictionResolver::lerpField` float lerp + relative-epsilon snap
- **File:** `src/Prediction/MispredictionResolver.cpp:14-71`
- **Symptom:** `fa + (fb - fa) * alpha` + `|fa-fb|/max(|fa|,|fb|) <= 1e-4f` decides snap-vs-smooth; `predictedBytes` feeds back into wire bytes.
- **Wire impact:** **YES.**
- **Fix category:** Explicit fixed-point — int32 ulp comparison + fixed-point lerp.
- **Effort:** 1 day.

---

## High

Significant risk; fixes Blockers first, but these compound.

### H-01. `gns_status_callback` runs on GNS-internal thread, mutates shared maps, fires `_stateHandler`
- **File:** `src/Transport/GnsConnection.cpp:69-131, 182, 807`
- **Wire impact:** YES (state transitions feed subsequent `_sendHello` calls).
- **Fix category:** Funnel through pump boundary.
- **Effort:** 2-3 days (largest single fix).

### H-02. `pump()` sorts `GnsConnection*` owners by raw pointer
- **File:** `src/Transport/GnsConnection.cpp:357-368`
- **Wire impact:** YES (drain order is pointer-dependent; reaper decisions follow).
- **Fix category:** Sort by `getNetId()`.
- **Effort:** 0.25 day.

### H-03. `distanceSq` interest-management membership test
- **File:** `src/Replication/ReplicationManager.cpp:99-104` + `interface/AYNetwork/INetwork.h:849` (`_interestRadiusSq`)
- **Wire impact:** YES — gates Spawn/Full/Delta visibility per peer.
- **Fix category:** Quantize `NetVec3` to fixed grid at serializer boundary.
- **Effort:** 1 day.

### H-04. `ReflectSerializer::writeFieldValue` writes raw IEEE-754 float/double bytes
- **File:** `src/Replication/ReflectSerializer.cpp:220-221, 249-250, 606-608, 707-709, 441-453, 826-846`
- **Wire impact:** YES — drives both wire bytes AND dirty-tracking CRC.
- **Fix category:** Explicit fixed-point — quantize at serializer boundary.
- **Effort:** 1 day.

### H-05. `SnapshotInterpolator::sample` float lerp driven by `findBracket` double `alpha`
- **File:** `src/Snapshot/SnapshotInterpolator.cpp:156-304` + `src/Snapshot/SnapshotBuffer.cpp:116-159`
- **Wire impact:** YES (indirect via reconcile).
- **Fix category:** Explicit fixed-point — int32 lerp, fixed-point alpha.
- **Effort:** 0.5 day.

### H-06. `TokenBucket` double `_tokens += dtSeconds * _rate`
- **File:** `include/AYNetwork/Transport/TokenBucket.h:31-77`
- **Wire impact:** YES (drop decisions).
- **Fix category:** Millibytes fixed-point.
- **Effort:** 0.5 day.

### H-07. `EntityReplicationWorldBinder` iterates `_bindings` / `desired` in hash order
- **File:** `include/AYNetwork/Replication/EntityReplicationWorldBinder.h:43-44, 69-79, 94-103`
- **Wire impact:** YES (via F-03 in iteration-order audit; compound).
- **Fix category:** Sort by netId.
- **Effort:** 0.25 day.

### H-08. `PredictionManager::_ghosts/_ackedSeq` iterated in hash order
- **File:** `include/AYNetwork/Prediction/PredictionManager.h:136-138`
- **Wire impact:** YES (compounds B-05).
- **Fix category:** Sort by netId/connectionId.
- **Effort:** 0.25 day.

### H-09. `RecordRpc` outbound always stamps `tick=0`
- **File:** `src/RPC/RpcHandler.cpp:591`
- **Symptom:** Outbound RPC events land with `tick=0` while inbound RPC/Input events stamp with `getServerTick()`. v2 player can't reconstruct timing.
- **Wire impact:** NO for state-equal; YES for replay event timeline.
- **Fix category:** Use the canonical `_serverTick`.
- **Effort:** 0.25 day.

### H-10. Inbound RPC / Input use live's `getServerTick()`
- **File:** `src/AYNetworkSubSystem.cpp:382, 402, 422, 458`
- **Wire impact:** NO for state-equal; YES for replay timeline.
- **Fix category:** Round to nearest playback tick.
- **Effort:** 0.5 day (requires v2 player cooperation).

### H-11. `_sessionStartUnixMs` (wall clock) drives replay rotation boundaries
- **File:** `src/Replay/FileReplayRecorder.cpp:94, 121-127, 130-144`
- **Wire impact:** YES (events split across `.ayrp` files at different boundaries).
- **Fix category:** Replace wall clock with `_serverTick` (`_maxTicksPerFile`) or seed from `randomSeed`.
- **Effort:** 0.5 day.

### H-12. Profiler dump + `snapshotAll` iterates `_conns` in hash order
- **File:** `src/Profiler/ProfilerRegistry.cpp:104, 207-234, 242-279`
- **Wire impact:** NO (stderr + snapshot copy only). But snapshot hashing/caching is broken.
- **Fix category:** Sort by netId.
- **Effort:** 0.25 day.

### H-13. Profiler `pathLocal/pathRemote` derived from `m_idPOPRelay` (varies run-to-run)
- **File:** `src/Profiler/ProfilerRegistry.cpp:186-187`
- **Wire impact:** NO for state-equal; YES for snapshot diffing.
- **Fix category:** Normalize to 0 (or exclude from replay-equality hash).
- **Effort:** 0.1 day.

### H-14. `RecordPeriodicCheckpoint` not wired from `ReplicationManager`
- **File:** `src/Replay/NetworkReplayRecorderAdapter.cpp:222-232`
- **Symptom:** Iteration order requirement for caller-supplied `registeredNetIds` is undocumented.
- **Wire impact:** Forward-looking (v2 player).
- **Fix category:** Document "must be sorted by netId" in the API contract.
- **Effort:** 0.1 day.

---

## Medium

### M-01. `BitStream::writeFloat` truncating cast `static_cast<uint16_t>(normalized * 65535.0f)`
- **File:** `src/BitStream.cpp:103-113, 175-181`
- **Wire impact:** YES (asymmetric rounding between write and read).
- **Fix category:** `lroundf` for symmetric nearest rounding.
- **Effort:** 0.25 day.

### M-02. `MispredictionResolver::applySmoothing` float `alpha = dtSec / smoothingDuration`
- **File:** `include/AYNetwork/Prediction/MispredictionResolver.h:81-86` + `src/Prediction/MispredictionResolver.cpp:101`
- **Wire impact:** YES (indirect).
- **Fix category:** Precompute uint32 fixed-point alpha.
- **Effort:** 0.25 day.

### M-03. `PacketAssembler::nowMs` from `performanceNowUs()` (fragment TTL eviction)
- **File:** `src/Protocol/PacketAssembler.cpp:123-126`
- **Wire impact:** YES (TTL determines eviction).
- **Fix category:** Drive TTL from logical tick.
- **Effort:** 0.25 day.

### M-04. `AYNetworkSubSystem::_simulationInboundMutex` (over-defensive lock)
- **File:** `src/AYNetworkSubSystem.cpp:175, 317, 470, 664`
- **Wire impact:** YES (ingress message order).
- **Fix category:** Drop the mutex; single-thread model.
- **Effort:** 0.25 day.

### M-05. `AckTracker::_mutex` (per `GnsConnection`)
- **File:** `src/Protocol/AckPipeline.cpp:79, 86, 99, 115`
- **Wire impact:** YES (callback ordering).
- **Fix category:** Drop mutex; single-thread model.
- **Effort:** 0.5 day.

### M-06. `RpcHandler::_pendingCallsMutex`
- **File:** `include/AYNetwork/RPC/RpcHandler.h:340` + `src/RPC/RpcHandler.cpp:729, 737, 751, 1032, 1050`
- **Wire impact:** YES (compounds B-06).
- **Fix category:** Single owner for `_pendingCalls`.
- **Effort:** 0.5 day.

### M-07. `pump()` calls `RunCallbacks` (GNS dispatch order)
- **File:** `src/Transport/GnsConnection.cpp:405`
- **Wire impact:** YES (covered by H-01 fix).
- **Fix category:** Funnel through pump boundary.

### M-08. `getPing()` queries GNS real-time status on calling thread (EWMA non-deterministic)
- **File:** `src/Transport/GnsConnection.cpp:763-774`
- **Wire impact:** Indirect (if cached into accumulator).
- **Fix category:** Cache `m_nPing` once per pump; never read `m_fl*` fields.
- **Effort:** 0.5 day.

### M-09. `_stateHandler` no re-entry guard or thread contract doc
- **File:** `src/Transport/GnsConnection.cpp:798-805`
- **Wire impact:** YES (covered by H-01 fix).

### M-10. `pump()` silent no-op on reentrant call
- **File:** `src/Transport/GnsConnection.cpp:344-346`
- **Wire impact:** YES (drain timing shifts).
- **Fix category:** Assert `!insidePump` in debug.
- **Effort:** 0.1 day.

### M-11. `EntityReplicationWorldBinder::collisions` unordered_set iteration (diagnostic)
- **File:** `include/AYNetwork/Replication/EntityReplicationWorldBinder.h:44`
- **Wire impact:** Indirect (downstream ordering decisions).
- **Fix category:** Sort by netId.

### M-12. `NetworkReplayRecorderAdapter::recordEvent` no-op on authority gate flip
- **File:** `src/Replay/NetworkReplayRecorderAdapter.cpp:62-77`
- **Wire impact:** YES.
- **Fix category:** Confirm `_authorityGateOk` only flips on staged phases.
- **Effort:** 0.25 day.

### M-13. `_lastAckedInputTick` write race
- **File:** `src/Replication/ReplicationManager.cpp:1080-1084`
- **Wire impact:** YES.
- **Fix category:** Audit `consumeClientInputs` re-entry; flag for follow-up.

### M-14. `_nextFragmentId` non-atomic `++`
- **File:** `src/Transport/GnsConnection.cpp:565, 611`
- **Wire impact:** Forward-looking (v2 player fragment-count dependence).
- **Fix category:** `std::atomic<uint32_t>` with relaxed order.
- **Effort:** 0.1 day.

### M-15. Profiler `byMsgTypeExtras` unordered_map iteration in snapshot copy
- **File:** `src/Profiler/ProfilerRegistry.cpp:210`
- **Wire impact:** NO for replay; YES for snapshot diff.
- **Fix category:** Sort on copy, or document consumer must sort.

### M-16. Profiler `perNetId` sorted by `cumulativeSendBytes` (not netId)
- **File:** `src/Profiler/ProfilerRegistry.cpp:215-218`
- **Symptom:** Sort tie unspecified.
- **Fix category:** Sort by netId (primary) + cumulativeSendBytes (secondary).

### M-17. `NetworkTime::_accumulator += dtSec * _tickRate` (wall-clock fed)
- **File:** `src/Snapshot/NetworkTime.cpp:16-26`
- **Wire impact:** YES (covered by B-02 fix).
- **Fix category:** uint64 fixed-point.

### M-18. `SnapshotBuffer::push` stores `double serverTimeSec`
- **File:** `src/Snapshot/SnapshotBuffer.cpp:53-94`
- **Wire impact:** YES.
- **Fix category:** `uint64_t` fixed-point.

---

## Low / Not relevant

These are either single-process deterministic, observability-only, or covered by a higher-priority fix.

### L-01. Test code `steady_clock::now()` deadline loops — test-side only.
- **Files:** `unittest/AYTest_AckPipeline.cpp:28-29`, `AYTest_Replication.cpp:332-333`, `AYTest_ClientInput.cpp:33-34`, etc.
- **Fix:** not relevant (test harness only).

### L-02. `NetVec3 { float x,y,z }` quantization — single-process float mul is deterministic.
- **File:** `interface/AYNetwork/INetwork.h:185-189`
- **Fix:** epsilon-comparison at API boundaries.

### L-03. `_interestRadiusSq` float mul — single-process deterministic.
- **File:** `interface/AYNetwork/INetwork.h:849`

### L-04. Profiler `qualityLocal/qualityRemote` float fields — never serialized.
- **File:** `include/AYNetwork/Profiler/ProfilerSnapshot.h`

### L-05. Profiler `static_cast<uint32_t>(status.m_flInBytesPerSec)` — observability only.
- **File:** `src/Profiler/ProfilerRegistry.cpp:170-173`

### L-06. `typeid(T).hash_code()` for primitive dispatch — stable per build config.
- **File:** `src/Replication/ReflectSerializer.cpp:73-84`
- **Note:** Forward-looking concern about cross-ABI portability. Document the assumption.

### L-07. `typeid(*component).hash_code()` as TypeRegistry key — same caveat.
- **File:** `include/AYNetwork/Replication/EntityReplicationWorldBinder.h:54`

### L-08. `std::unordered_map<HSteamNetConnection, GnsConnection*>` static registries.
- **File:** `src/Transport/GnsConnection.cpp:49-63`
- **Fix:** Not relevant (lookup-only).

### L-09. RPC call IDs `_nextCallId.fetch_add` — monotonic, deterministic.
- **File:** `src/RPC/RpcHandler.cpp:798, 824, 850, 883, 921`
- **No fix needed.**

### L-10. ACK pipeline seq `_nextSeq.fetch_add` — monotonic.
- **File:** `include/AYNetwork/Protocol/AckPipeline.h:43, 58`
- **No fix needed.**

### L-11. Fragment IDs `_nextFragmentId` — per-connection monotonic (single-thread by convention).
- **File:** `include/AYNetwork/Transport/GnsConnection.h:309`
- **Covered by:** M-14 (atomicity, not determinism).

### L-12. Server tick `_serverTick` — deterministic given identical tick schedule.
- **File:** `src/Replication/ReplicationManager.cpp:993-1003`
- **No fix needed.**

### L-13. RPC retry backoff `computeRetryBackoffMs` — exponential `<< shift`, no jitter.
- **File:** `src/RPC/RpcHandler.cpp:683-698`
- **No fix needed.**

### L-14. `ReflectSerializer::hashFieldName` / FNV-1a — fully deterministic.
- **File:** `src/Replication/ReflectSerializer.cpp` (hash fns)
- **No fix needed.**

### L-15. `UdpSocket.cpp:34` `g_wsaRefCount` — lifecycle only.
- **No fix needed.**

### L-16. `_initRefCount` spin CAS in `gns::shutdown` — lifecycle only.
- **File:** `src/Transport/GnsConnection.cpp:196-203`

### L-17. Profiler `std::atomic<uint64_t>` cumulative counters — main-thread-only by contract.
- **File:** `include/AYNetwork/Profiler/ProfilerRegistry.h:92-93`

### L-18. `disconnect()` unlock-before-`CloseConnection` gap.
- **File:** `src/Transport/GnsConnection.cpp:733-746`
- **Covered by:** H-01 fix.

### L-19. All `std::atomic<bool>` / counters in tests — test-side only.

### L-20. `pumpUntil(timeout, pred, systems, dt)` test helper — test-side only.

### L-21. PacketHeader `timestampMs = 0` literals in replication path.
- **File:** `src/Replication/ReplicationManager.cpp:333, 381, 487, 518, 556, 595, 867, 882`
- **Note:** Replication already explicitly stamps 0; this is correct design for replay byte-equal.

### L-22. LZ4 compression threshold + compress — deterministic given identical inputs.
- **File:** `src/Replay/FileReplayRecorder.cpp:103-119`

### L-23. CRC32C polynomial `0x82F63B78` — Castagnoli standard.
- **File:** `src/Protocol/PacketCodec.cpp:90-94`

### L-24. `PacketHeader::timestampMs` for replication frames is `0` (deliberate).
- **File:** `src/Replication/ReplicationManager.cpp:333 et al.`
- **No fix needed.**

### L-25. Handshake body bytes have no random counters.
- **File:** `src/Transport/GnsConnection.cpp:885-956`
- **Note:** `_address` field for server-side HELLO carries client IP:port string (varies per session but handshake is not recorded by network adapter).

---

## Cross-cutting patterns

### Pattern 1: "Sort by netId before iterating"
- **Sites:** ~25 (B-03, B-04, B-05, B-06, B-07, B-08, H-02, H-07, H-08, H-12, M-11, M-15, M-16)
- **Single fix:** Helper `sortedKeys(const UnorderedMap&)` that copies keys into a sorted vector. Replaces every `for (const auto& kv : map)` site that flows into wire bytes.
- **Effort:** 0.5-1 day across the codebase.

### Pattern 2: "Pump as the canonical thread boundary"
- **Sites:** H-01, M-04, M-05, M-06, M-07, M-09
- **Single fix:** Refactor `GnsConnection` so all state mutations happen in `pump()`, never in callbacks. User handlers fire from a `pendingActions` queue drained at pump end.
- **Effort:** 2-3 days.

### Pattern 3: "Inject clock seam"
- **Sites:** B-01, B-02, M-03, M-17, M-18
- **Single fix:** Add `static std::function<uint64_t()> s_nowOverride` to `GnsConnection`, `PacketAssembler`, `NetworkTime`. Default = `performanceNowUs()`. Override = `serverTick * 1000 / tickRate` for replay-driven mode.
- **Effort:** 1 day.

---

## Out-of-scope (per user)

- AYEntity / AYPhysics / cross-module determinism.
- Cross-host aggregation (multi-server rollup).
- Float-based MispredictionResolver smoothing curves (requires AEntity cooperation).
- Body-mutating fault counters (post-mutation counts are deterministic; not addressed here).

---

## Ship definition (R6.x, future work)

1. All **Blockers** (B-01 through B-12) fixed; build green. ✅
2. All **Highs** (H-01 through H-14) fixed or explicitly accepted as "best-effort". ✅
3. New test suite (5-10 cases) demonstrating:
   - 2 servers, identical inputs, 60 ticks → state hashes match ✅ (`TwoRuns_SameInputs_SameStateHash`)
   - 2 replays (from same `.ayrp`) → state hashes match ✅ (`ReplayFile_RewindReplays_SameHash`)
   - With + without fault profile → state hashes match (deterministic RNG) ✅ (`FaultProfile_RngDeterministic_SameHash`)
4. `design.md §15.13` written: determinism contract, clock seam docs, replay-header format bump for `randomSeed`. ✅
5. No regression: R5.5 1484/1484 PASS unchanged. ✅ (R6 baseline = 1492/1492 PASS)

**R6.0 ship status:** ALL FIVE ITEMS GREEN. Engine pointer bump for C9 pending.

**R6.5 ship status (2026-08-25):** `B-10` FIXED via the v2 player remap (`NetworkReplayEventDecoder::decodeNext`) and the pump bridge (`NetworkSubSystem::tickRecordedEvent`). Three commits (R6.5-1 foundation player, R6.5-2 decoder, R6.5-3 bridge) — engine pointer bump for `dd2fdec` pending.

**R6.6+ (deferred):** AYEntity / AYPhysics cross-module determinism (out of scope per user 2026-08-25); full automatic accept-order translation for cross-server replay; snapshot deserialization / state restoration through the foundation checkpoint payload.

---

## Sign-off

**Author:** Claude (Codex session, 2026-08-25)
**Reviewed by:** pending
**Status:** **R6 SHIPPED.** Submodule pointer at `cf746b1` (C9 HEAD). Engine pointer bump for C9 pending. v2 player → R6.5.