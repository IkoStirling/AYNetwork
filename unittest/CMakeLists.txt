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

set(SRC_FILES
    main.cpp
    AYTest_Transport.cpp
    AYTest_BitStream.cpp
    AYTest_GnsEchoTest.cpp
    AYTest_Handshake.cpp
    AYTest_PacketCodec.cpp
    AYTest_Replication.cpp
    AYTest_RpcHandler.cpp
    AYTest_SubsystemIntegration.cpp
    AYTest_EntityReplicationIntegration.cpp
    AYTest_InterestManagement.cpp
)

add_executable(${MODULE_NAME} ${SRC_FILES})

target_compile_definitions(${MODULE_NAME} PRIVATE AYNETWORK_BUILD_TESTS)

# Link AYNetwork + AYTest framework + AYCore (for AYNetwork.h transitive includes).
# The PUBLIC link in AYNetwork/CMakeLists.txt propagates AYGameLoop/AYCore include
# dirs and GNS linkage so we don't need any additional flags here.
target_link_libraries(${MODULE_NAME} PRIVATE AYNetwork AYTest AYCore AYEntity)
target_include_directories(${MODULE_NAME} PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/..
)

add_test(NAME ${MODULE_NAME} COMMAND ${MODULE_NAME})