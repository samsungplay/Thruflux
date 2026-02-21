#pragma once
#include "SenderConfig.hpp"
#include <ixwebsocket/IXWebSocket.h>
#include <ixwebsocket/IXNetSystem.h>
#include <spdlog/spdlog.h>

#include "SenderSocketHandler.hpp"
#include "../common/Utils.hpp"
#include <boost/algorithm/string.hpp>
#include <CLI/App.hpp>

#include "SenderStream.hpp"
namespace sender {
    inline int run(const int argc, char **argv) {
        spdlog::set_pattern("%v");
        common::Utils::disableLibniceLogging();

        std::vector<std::string> rawStunUrls;
        boost::split(rawStunUrls, sender::SenderConfig::stunServer, boost::is_any_of(","), boost::token_compress_on);

        for (const auto &rawStunUrl: rawStunUrls) {
            if (auto stunServer = common::Utils::toStunServer(rawStunUrl); stunServer.has_value()) {
                common::IceHandler::addStunServer(stunServer.value());
            }
        }

        std::vector<std::string> rawTurnUrls;
        boost::split(rawTurnUrls, sender::SenderConfig::turnServers, boost::is_any_of(","), boost::token_compress_on);

        for (const auto &rawTurnUrl: rawTurnUrls) {
            if (auto turnServer = common::Utils::toTurnServer(rawTurnUrl); turnServer.has_value()) {
                common::IceHandler::addTurnServer(turnServer.value());
            }
        }


        ix::initNetSystem();
        ix::WebSocket socketClient;
        ix::SocketTLSOptions tlsOptions;
        tlsOptions.caFile = common::EMBEDDED_CA_BUNDLE;
        socketClient.setTLSOptions(tlsOptions);
        socketClient.disableAutomaticReconnection();
        common::IceHandler::initialize();

        SenderStream::initialize();


        socketClient.setUrl(SenderConfig::serverUrl);
        ix::WebSocketHttpHeaders headers;
        headers["x-role"] = "sender";
        headers["x-id"] = common::Utils::generateNanoId();
        socketClient.setExtraHeaders(headers);
        socketClient.setPingInterval(30);

        socketClient.setOnMessageCallback([&socketClient](const ix::WebSocketMessagePtr &msg) {
            if (msg->type == ix::WebSocketMessageType::Open) {
                sender::SenderSocketHandler::onConnect(socketClient);
            } else if (msg->type == ix::WebSocketMessageType::Message) {
                sender::SenderSocketHandler::onMessage(socketClient, msg->str);
            } else if (msg->type == ix::WebSocketMessageType::Close) {
                sender::SenderSocketHandler::onClose(socketClient, msg->closeInfo.reason);
            } else if (msg->type == ix::WebSocketMessageType::Error) {
                spdlog::error("Could not connect to relay: HTTP Status: {}", msg->errorInfo.http_status);
                spdlog::error("Error Description: {}", msg->errorInfo.reason);
                common::ThreadManager::terminate();
            }
        });

        spdlog::info("Connecting to signaling server... {} ", sender::SenderConfig::serverUrl);

        socketClient.start();

        common::ThreadManager::runMainLoop();

        socketClient.stop();

        common::IceHandler::destroy();

        sender::SenderStream::dispose();


        ix::uninitNetSystem();
        return 0;
    }
}
