cmake_minimum_required(VERSION 3.20)

project(AYNetwork)

set(MODULE_NAME AYNetwork)

set(SRC_FILES
    src/AYNetworkSubSystem.cpp
    src/NetConnectionImpl.cpp
    src/BitStream.cpp
    src/Transport/GnsConnection.cpp
    src/Transport/UdpSocket.cpp
    src/Signaling/UdpSignaling.cpp
    src/Signaling/SecureUdpSignaling.cpp
    src/Session/P2PSessionProtocol.cpp
    src/Session/SessionTicket.cpp
    src/Session/InMemorySessionService.cpp
    src/Session/SqliteSessionService.cpp
    src/Session/HttpSessionService.cpp
    src/Session/HttpOnlineServices.cpp
    src/Session/SqliteOnlineServices.cpp
    src/Session/PlayerAccessToken.cpp
    src/Session/P2PSessionBackend.cpp
    src/Session/P2PSessionLeaseKeeper.cpp
    src/Session/P2PSessionCoordinator.cpp
    src/Session/OnlineSessionCoordinator.cpp
    src/Session/InMemoryOnlineServices.cpp
    src/Protocol/PacketAssembler.cpp
    src/Protocol/PacketCodec.cpp
    src/Protocol/AckPipeline.cpp
    src/Replication/ReplicationManager.cpp
    src/Replication/ReplicationSystem.cpp
    src/Replication/ReflectSerializer.cpp
    src/RPC/RpcHandler.cpp
    src/Snapshot/NetworkTime.cpp
    src/Snapshot/SnapshotBuffer.cpp
    src/Snapshot/SnapshotInterpolator.cpp
    src/Prediction/InputRing.cpp                # R5.2 (2026-08-24)
    src/Prediction/PredictionManager.cpp       # R5.2
    src/Prediction/MispredictionResolver.cpp   # R5.2
    src/Replay/NetworkReplayRecorderAdapter.cpp  # R5.3 (2026-08-24)
    src/Replay/NetworkReplayEventDecoder.cpp      # R6.5-2 (2026-08-25)
    src/Transport/DelayedFrameQueue.cpp          # R5.4 (2026-08-25)
    src/Transport/TransportFaultController.cpp   # R5.4
    src/Transport/TransportFaultInterceptor.cpp   # R5.4
    src/Profiler/ProfilerRegistry.cpp            # R5.5 (2026-08-25)
)

# R1 (2026-07-26): wire up GameNetworkingSockets via vcpkg. See design.md §14.
# find_package is required; if GNS is not installed, the build fails — this is
# intentional: a stub transport (the prior KCP approach) gives no signal that
# the network layer isn't real, which is worse than a hard build break.
find_package(GameNetworkingSockets CONFIG REQUIRED)

# R2 (2026-07-27): PacketFlag::Compressed uses lz4 raw block API for the
# encode side. Decode side reuses ayt::storage::Lz4Decompressor from
# AYStorage. Pattern copied from AYTool/package_tool/CMakeLists.txt.
find_package(lz4 CONFIG REQUIRED)
find_package(httplib CONFIG REQUIRED)
find_package(nlohmann_json CONFIG REQUIRED)
find_package(unofficial-sodium CONFIG REQUIRED)
find_package(unofficial-sqlite3 CONFIG REQUIRED)

add_library(${MODULE_NAME} STATIC ${SRC_FILES})

target_link_libraries(${PROJECT_NAME} PUBLIC
    AYCore
    AYGameLoop
    AYStorage                              # R2: Lz4Decompressor (decode side)
    AYReflect                              # R3.0: ITypeInfo + FieldAttribute::NetReplicate walk
    AYReplay                               # R5.3 (2026-08-24): IReplayRecorder interface
    AYCrypto                               # authenticated P2P signaling tokens/HMAC
    GameNetworkingSockets::GameNetworkingSockets   # R1: GNS shared variant
    lz4::lz4                               # R2: PacketCodec encode side
)

# Session-service implementation dependencies stay out of AYNetwork's public
# compile interface.  CMake still propagates their link-only requirements to
# consumers of this static library.
target_link_libraries(${PROJECT_NAME} PRIVATE
    httplib::httplib                       # P2P session-service HTTP transport
    nlohmann_json::nlohmann_json           # SessionServer wire JSON
    unofficial-sodium::sodium              # Ed25519 Join Ticket signatures
    unofficial::sqlite3::sqlite3            # durable P2P authority store
)

# P0 audit note (2026-07-26): we previously linked AYEntity here because
# ReplicationSystem extended ayt::entity::ISystem. That pulled AYAnimation
# transitively, breaking isolated builds. ReplicationSystem now has its own
# minimal interface; R3 will plug it into AYEntity via an adapter (no link).
#
# Note (2026-07-26): AYCore / AYGameLoop are PUBLIC so their include dirs and
# compile definitions propagate to consumers (e.g. unit tests). The lib only
# needs the headers; the static-lib link transitively brings symbols in.

# P0 audit fix (2026-07-26): link Winsock2 on Windows. UdpSocket.cpp uses
# bind/socket/sendto/recvfrom/closesocket/WSAStartup/WSACleanup which all
# live in ws2_32.lib. Without this the library compiles but tests fail to
# link. R1 (gamenetworkingsockets) will replace UdpSocket entirely and this
# dependency will go away.
if(WIN32)
    target_link_libraries(${PROJECT_NAME} PUBLIC ws2_32)
endif()

option(AYNETWORK_BUILD_SIGNALING_SERVER "Build AYNetwork UDP signaling server" OFF)

# Reference authority-session service. HTTP room/lease control and secure UDP
# signaling are logically separate components but share one process and one
# in-memory credential directory in this first deployable implementation.
option(AYNETWORK_BUILD_SESSION_SERVER
    "Build AYNetwork reference P2P session service" ${AYNETWORK_BUILD_SIGNALING_SERVER})
if(AYNETWORK_BUILD_SESSION_SERVER)
    add_executable(AYNetwork_SessionServer tools/session_server/main.cpp)
    target_link_libraries(AYNetwork_SessionServer PRIVATE AYNetwork)
    add_executable(AYNetwork_SessionProbe tools/session_probe/main.cpp)
    target_link_libraries(AYNetwork_SessionProbe PRIVATE AYNetwork)
    add_executable(AYNetwork_OnlineProbe tools/online_probe/main.cpp)
    target_link_libraries(AYNetwork_OnlineProbe PRIVATE AYNetwork)
    if(WIN32 AND BUILD_TESTING)
        add_test(
            NAME AYNetwork_P2P_SessionService_E2E
            COMMAND powershell -NoProfile -ExecutionPolicy Bypass
                -File ${CMAKE_CURRENT_SOURCE_DIR}/unittest/RunP2PSessionServiceE2E.ps1
                -SessionServer $<TARGET_FILE:AYNetwork_SessionServer>
                -SessionProbe $<TARGET_FILE:AYNetwork_SessionProbe>)
        set_tests_properties(AYNetwork_P2P_SessionService_E2E
            PROPERTIES TIMEOUT 60 RUN_SERIAL TRUE)
        add_test(
            NAME AYNetwork_P2P_SessionService_Production_E2E
            COMMAND powershell -NoProfile -ExecutionPolicy Bypass
                -File ${CMAKE_CURRENT_SOURCE_DIR}/unittest/RunP2PSessionServiceE2E.ps1
                -SessionServer $<TARGET_FILE:AYNetwork_SessionServer>
                -SessionProbe $<TARGET_FILE:AYNetwork_SessionProbe>
                -Production)
        set_tests_properties(AYNetwork_P2P_SessionService_Production_E2E
            PROPERTIES TIMEOUT 60 RUN_SERIAL TRUE)
        add_test(
            NAME AYNetwork_OnlineServices_E2E
            COMMAND powershell -NoProfile -ExecutionPolicy Bypass
                -File ${CMAKE_CURRENT_SOURCE_DIR}/unittest/RunOnlineServicesE2E.ps1
                -SessionServer $<TARGET_FILE:AYNetwork_SessionServer>
                -OnlineProbe $<TARGET_FILE:AYNetwork_OnlineProbe>)
        set_tests_properties(AYNetwork_OnlineServices_E2E
            PROPERTIES TIMEOUT 60 RUN_SERIAL TRUE)
        add_test(
            NAME AYNetwork_OnlineServices_Production_E2E
            COMMAND powershell -NoProfile -ExecutionPolicy Bypass
                -File ${CMAKE_CURRENT_SOURCE_DIR}/unittest/RunOnlineServicesE2E.ps1
                -SessionServer $<TARGET_FILE:AYNetwork_SessionServer>
                -OnlineProbe $<TARGET_FILE:AYNetwork_OnlineProbe>
                -Production)
        set_tests_properties(AYNetwork_OnlineServices_Production_E2E
            PROPERTIES TIMEOUT 60 RUN_SERIAL TRUE)
    endif()
endif()

# Optional standalone rendezvous process for the built-in UDP signaling
# backend. It hosts no simulation state and may be replaced by an application's
# authenticated signaling service.
if(AYNETWORK_BUILD_SIGNALING_SERVER)
    add_executable(AYNetwork_SignalingServer tools/signaling/main.cpp)
    target_link_libraries(AYNetwork_SignalingServer PRIVATE AYNetwork)
    add_executable(AYNetwork_SecureSignalingServer tools/secure_signaling/main.cpp)
    target_link_libraries(AYNetwork_SecureSignalingServer PRIVATE AYNetwork)
    add_executable(AYNetwork_P2PSmokePeer tools/p2p_smoke/main.cpp)
    target_link_libraries(AYNetwork_P2PSmokePeer PRIVATE AYNetwork)
    if(WIN32 AND BUILD_TESTING)
        foreach(SCENARIO IN ITEMS normal authority reject drop)
            add_test(
                NAME AYNetwork_P2P_Migration_${SCENARIO}
                COMMAND powershell -NoProfile -ExecutionPolicy Bypass
                    -File ${CMAKE_CURRENT_SOURCE_DIR}/unittest/RunP2PMigrationE2E.ps1
                    -SignalingServer $<TARGET_FILE:AYNetwork_SignalingServer>
                    -SmokePeer $<TARGET_FILE:AYNetwork_P2PSmokePeer>
                    -Scenario ${SCENARIO})
            set_tests_properties(
                AYNetwork_P2P_Migration_${SCENARIO}
                PROPERTIES TIMEOUT 90 RUN_SERIAL TRUE)
        endforeach()
    endif()
endif()

# P0 audit fix (2026-07-26): avoid winsock.h (v1) vs winsock2.h (v2) collision
# caused by AYCore.h transitively pulling <windows.h> (which drags in legacy
# winsock.h) before UdpSocket.cpp can include <winsock2.h>. WIN32_LEAN_AND_MEAN
# makes <windows.h> skip the legacy winsock macros so winsock2.h can be included
# cleanly afterwards. See Microsoft KB182383 for the long-standing fix.
if(MSVC OR WIN32)
    target_compile_definitions(${MODULE_NAME} PRIVATE WIN32_LEAN_AND_MEAN NOMINMAX)
endif()

target_include_directories(${MODULE_NAME} PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}/include/Protocol
    ${CMAKE_CURRENT_SOURCE_DIR}/include/Replication
    ${CMAKE_CURRENT_SOURCE_DIR}/include/RPC
    ${CMAKE_CURRENT_SOURCE_DIR}/include/Transport
    ${CMAKE_CURRENT_SOURCE_DIR}/include/Snapshot
    ${CMAKE_CURRENT_SOURCE_DIR}/include/Prediction   # R5.2 (2026-08-24)
    ${CMAKE_CURRENT_SOURCE_DIR}/include/Profiler     # R5.5 (2026-08-25)
    ${CMAKE_CURRENT_SOURCE_DIR}/interface
    ${CMAKE_CURRENT_SOURCE_DIR}/interface/AYNetwork/Replay  # R5.3 (2026-08-24)
    ${CMAKE_CURRENT_SOURCE_DIR}
    PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src/Transport        # R5.4 (2026-08-25): TransportFault* headers are implementation-detail
    ${CMAKE_CURRENT_SOURCE_DIR}/src/Session
)

# 协议支持选项
option(AYNETWORK_ENABLE_UDP "Enable UDP support" ON)
option(AYNETWORK_ENABLE_WS "Enable WebSocket support" ON)

# 单元测试
option(AYNETWORK_BUILD_TESTS "Build AYNetwork unit tests" ON)
if(AYNETWORK_BUILD_TESTS)
    target_compile_definitions(${PROJECT_NAME} PRIVATE AYNETWORK_BUILD_TESTS)
    add_subdirectory(unittest)
endif()

if(NOT AYNETWORK_ENABLE_UDP)
    target_compile_definitions(${PROJECT_NAME} PRIVATE AYNETWORK_NO_UDP)
endif()

if(NOT AYNETWORK_ENABLE_WS)
    target_compile_definitions(${PROJECT_NAME} PRIVATE AYNETWORK_NO_WS)
endif()
