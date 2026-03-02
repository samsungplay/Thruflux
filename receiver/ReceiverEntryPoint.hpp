#pragma once
#include "ReceiverConfig.hpp"
#include <ixwebsocket/IXWebSocket.h>
#include <ixwebsocket/IXNetSystem.h>
#include <spdlog/spdlog.h>
#include "../common/Utils.hpp"
#include <boost/algorithm/string.hpp>

#include "ReceiverSocketHandler.hpp"
namespace receiver {
    inline void run () {
        spdlog::set_pattern("%v");
        common::Utils::disableLibniceLogging();

        common::IceHandler::initialize();

        std::vector<std::string> rawStunUrls;
        boost::split(rawStunUrls, ReceiverConfig::stunServers, boost::is_any_of(","), boost::token_compress_on);

        for (const auto &rawStunUrl: rawStunUrls) {
            if (auto stunServer = common::Utils::toStunServer(rawStunUrl); stunServer.has_value()) {
                common::IceHandler::addStunServer(stunServer.value());
            }
        }

        std::vector<std::string> rawTurnUrls;
        boost::split(rawTurnUrls, ReceiverConfig::turnServers, boost::is_any_of(","), boost::token_compress_on);

        for (const auto &rawTurnUrl: rawTurnUrls) {
            if (auto turnServer = common::Utils::toTurnServer(rawTurnUrl); turnServer.has_value()) {
                common::IceHandler::addTurnServer(turnServer.value());
            }
        }


        ix::initNetSystem();

        ReceiverStream::initialize();
        ix::WebSocket socketClient;
        ix::SocketTLSOptions tlsOptions;
        tlsOptions.caFile = common::EMBEDDED_CA_BUNDLE;
        socketClient.setTLSOptions(tlsOptions);
        socketClient.disableAutomaticReconnection();


        socketClient.setUrl(ReceiverConfig::serverUrl);
        ix::WebSocketHttpHeaders headers;
        headers["x-role"] = "receiver";
        headers["x-id"] = common::Utils::generateNanoId();
        socketClient.setExtraHeaders(headers);
        socketClient.setPingInterval(30);

        socketClient.setOnMessageCallback([&socketClient](const ix::WebSocketMessagePtr &msg) {
            if (msg->type == ix::WebSocketMessageType::Open) {
                ReceiverSocketHandler::onConnect(socketClient);
            } else if (msg->type == ix::WebSocketMessageType::Message) {
                ReceiverSocketHandler::onMessage(socketClient, msg->str);
            } else if (msg->type == ix::WebSocketMessageType::Close) {
                ReceiverSocketHandler::onClose(socketClient,  msg->closeInfo.reason);
            }
            else if (msg->type == ix::WebSocketMessageType::Error) {
                spdlog::error("Could not connect to relay: HTTP Status: {}", msg->errorInfo.http_status);
                spdlog::error("Error Description: {}", msg->errorInfo.reason);
                ui::eventStream.sendMessage("connect_error",nlohmann::json{{"code", msg->errorInfo.http_status}, {"reason", msg->errorInfo.reason}}.dump());
                common::ThreadManager::terminate();
            }
        });

        spdlog::info("Connecting to signaling server... {}", ReceiverConfig::serverUrl);

        ui::eventStream.sendMessage("connecting","");

        socketClient.start();


        common::ThreadManager::runMainLoop();

        socketClient.stop();

        common::IceHandler::destroy();

        ReceiverStream::dispose();

        ix::uninitNetSystem();
    }
}
