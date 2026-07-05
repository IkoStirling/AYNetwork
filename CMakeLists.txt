cmake_minimum_required(VERSION 3.20)

project(AYNetwork)

set(MODULE_NAME AYNetwork)

set(SRC_FILES
    src/AYNetworkSubSystem.cpp
    src/NetConnectionImpl.cpp
    src/ReplicationManager.cpp
    src/BitStream.cpp
    src/Transport/KcpConnection.cpp
    src/Transport/UdpSocket.cpp
    src/Transport/AYConnection.cpp
    src/Protocol/PacketAssembler.cpp
    src/Protocol/SequenceNumber.cpp
    src/Replication/ReplicationManager.cpp
    src/Replication/ReplicationSystem.cpp
)

add_library(${MODULE_NAME} STATIC ${SRC_FILES})

target_link_libraries(${PROJECT_NAME} PRIVATE
    AYCore
    AYGameLoop
)

target_include_directories(${MODULE_NAME} PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}/include/Protocol
    ${CMAKE_CURRENT_SOURCE_DIR}/include/Replication
    ${CMAKE_CURRENT_SOURCE_DIR}/include/Transport
    ${CMAKE_CURRENT_SOURCE_DIR}/interface
    ${CMAKE_CURRENT_SOURCE_DIR}
    PRIVATE
)

# 协议支持选项
option(AYNETWORK_ENABLE_UDP "Enable UDP support" ON)
option(AYNETWORK_ENABLE_WS "Enable WebSocket support" ON)

if(NOT AYNETWORK_ENABLE_UDP)
    target_compile_definitions(${PROJECT_NAME} PRIVATE AYNETWORK_NO_UDP)
endif()

if(NOT AYNETWORK_ENABLE_WS)
    target_compile_definitions(${PROJECT_NAME} PRIVATE AYNETWORK_NO_WS)
endif()