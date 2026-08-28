cmake_minimum_required(VERSION 3.20)

# =============================================================================
# AYNetwork unit tests
# =============================================================================
# P0 audit fix (2026-07-26): tests had no CMakeLists registration before.
# R1.5 (2026-07-27): restructured to match the standard unittest pattern used by
# every other module (AYAnimation, AYPhysics, AYDevice, ...):
#   - One executable target (AYNetwork_UnitTests)
#   - main.cpp + AYTest_<Feature>.cpp files
#   - Tests register via AYTest framework's TEST_CASE / TEST_SUITE macros
#   - Output + macros come from <AYTest.h>; no per-file CHECK macros
#   - File naming: AYTest_<Subject>.cpp (note: AYTest_ prefix)

set(MODULE_NAME AYNetwork_Test)

set(CORE_SRC_FILES
    main.cpp
    AYTest_Transport.cpp
    AYTest_BitStream.cpp
    AYTest_GnsEchoTest.cpp
    AYTest_Handshake.cpp
    AYTest_PacketCodec.cpp
    AYTest_Replication.cpp
    AYTest_RpcHandler.cpp
    AYTest_SubsystemIntegration.cpp
    AYTest_InterestManagement.cpp
    AYTest_RepNotify.cpp
    AYTest_AckPipeline.cpp
    AYTest_SnapshotInterpolation.cpp
    AYTest_ClientInput.cpp        # R5.2 (2026-08-24)
    AYTest_ReplayNetwork.cpp      # R5.3 (2026-08-24)
    AYTest_TransportFaultProfile.cpp   # R5.4 (2026-08-25)
    AYTest_TokenBucket.cpp             # R5.4
    AYTest_DelayedFrameQueue.cpp       # R5.4
    AYTest_TransportFaultInterceptor.cpp # R5.4
    AYTest_StateEqual.cpp              # R6 C9 (2026-08-25) — must precede FakeTransport (crash without gns::init kills the process)
    AYTest_FakeTransport.cpp           # R5.4
    AYTest_ProfilerAccumulator.cpp     # R5.5 (2026-08-25)
    AYTest_PerMsgType.cpp              # R5.5
    AYTest_PerNetId.cpp                # R5.5
    AYTest_RttAndCongestion.cpp        # R5.5
    AYTest_ProfilerSubsystem.cpp       # R5.5
)

add_executable(${MODULE_NAME} ${CORE_SRC_FILES})

target_compile_definitions(${MODULE_NAME} PRIVATE AYNETWORK_BUILD_TESTS)

# Link AYNetwork + AYTest framework + AYCore (for AYNetwork.h transitive includes).
# The PUBLIC link in AYNetwork/CMakeLists.txt propagates AYGameLoop/AYCore include
# dirs and GNS linkage so we don't need any additional flags here.
target_link_libraries(${MODULE_NAME} PRIVATE AYNetwork AYTest AYCore)
target_include_directories(${MODULE_NAME} PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/..
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/Transport  # R5.4 (2026-08-25): TransportFault* headers
)

ay_add_test(${MODULE_NAME})

# Standalone process: GNS has one identity per process, and ResetIdentity is
# intentionally rejected while any legacy IP test connection remains active.
# Isolating P2P also mirrors production where each peer is a separate process.
set(P2P_TEST_NAME AYNetwork_P2P_Test)
add_executable(${P2P_TEST_NAME}
    main.cpp
    AYTest_P2P.cpp
    AYTest_P2PSession.cpp
    AYTest_SecureSignaling.cpp
    AYTest_SessionService.cpp
    AYTest_SessionCoordinator.cpp
    AYTest_DurableSessionService.cpp
    AYTest_OnlineServices.cpp
    AYTest_DurableOnlineServices.cpp
)
target_compile_definitions(${P2P_TEST_NAME} PRIVATE AYNETWORK_BUILD_TESTS)
target_link_libraries(${P2P_TEST_NAME} PRIVATE AYNetwork AYTest AYCore)
target_include_directories(${P2P_TEST_NAME} PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/..
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/Session
)
ay_add_test(${P2P_TEST_NAME})

# AYEntity integration is optional at the AYNetwork library boundary. Keep its
# test in a separate executable so transport/protocol/replication tests do not
# pull the renderer-heavy AYEntity dependency graph into every network build.
if(TARGET AYEntity)
    set(ENTITY_TEST_NAME AYNetwork_EntityIntegration_Test)
    add_executable(${ENTITY_TEST_NAME}
        main.cpp
        AYTest_EntityReplicationIntegration.cpp
    )
    target_compile_definitions(${ENTITY_TEST_NAME} PRIVATE AYNETWORK_BUILD_TESTS)
    target_link_libraries(${ENTITY_TEST_NAME} PRIVATE AYNetwork AYTest AYCore AYEntity)
    target_include_directories(${ENTITY_TEST_NAME} PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR}/..
    )
    ay_add_test(${ENTITY_TEST_NAME})
endif()
