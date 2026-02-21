cmake_minimum_required(VERSION 3.24)

if(WIN32)
    add_compile_definitions(WIN32_LEAN_AND_MEAN NOCRYPT)
    add_compile_options(/Zc:preprocessor)
endif()



set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

project(Thruflux_C__ LANGUAGES CXX)

if(MSVC)
    set(CMAKE_MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>")
endif()


find_package(CLI11 CONFIG REQUIRED)
find_package(spdlog CONFIG REQUIRED)
find_package(unofficial-uwebsockets CONFIG REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(nlohmann_json CONFIG REQUIRED)
find_package(ixwebsocket CONFIG REQUIRED)
find_package(Boost REQUIRED COMPONENTS url)
find_package(llfio CONFIG REQUIRED)
find_package(PkgConfig REQUIRED)
find_package(lsquic CONFIG REQUIRED)
find_package(indicators CONFIG REQUIRED)
find_package(MbedTLS CONFIG REQUIRED)
pkg_check_modules(NICE REQUIRED IMPORTED_TARGET nice)



add_executable(thru
        app/Main.cpp
        server/ServerConfig.hpp
        server/ServerSocketHandler.hpp
        server/TransferSession.hpp
        server/TransferSessionStore.hpp
        server/SessionTracker.hpp
        server/ServerEntryPoint.hpp

        sender/SenderConfig.hpp
        sender/SenderSocketHandler.hpp
        sender/SenderStream.hpp
        sender/SenderEntryPoint.hpp

        receiver/ReceiverConfig.hpp
        receiver/ReceiverSocketHandler.hpp
        receiver/ReceiverStream.hpp
        receiver/ReceiverContexts.hpp
        receiver/ReceiverEntryPoint.hpp

        common/Utils.hpp
        common/Payloads.hpp
        common/Types.hpp
        common/TTLCache.hpp
        common/IceHandler.hpp
        common/ThreadManager.hpp
        common/Contexts.hpp
        common/Stream.hpp
)

target_link_libraries(thru PRIVATE
        CLI11::CLI11
        spdlog::spdlog
        nlohmann_json::nlohmann_json
        Boost::url
        Boost::boost
        OpenSSL::Crypto
        indicators::indicators
        ixwebsocket::ixwebsocket
        lsquic::lsquic
        unofficial::uwebsockets::uwebsockets
        MbedTLS::mbedtls
        PkgConfig::NICE
        llfio::sl
)
