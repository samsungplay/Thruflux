#pragma once
#include <IXWebSocket.h>
#include <memory>
#include <nlohmann/json.hpp>
#include <nlohmann/json_fwd.hpp>
#include <spdlog/spdlog.h>
#include <latch>
#include "../common/Payloads.hpp"
#include "../common/Utils.hpp"
#include <boost/asio/post.hpp>
#include "../common/ThreadManager.hpp"
#include "ReceiverConfig.hpp"
#include "ReceiverStream.hpp"

namespace receiver {
    class ReceiverSocketHandler {
    public:
        static void onConnect(ix::WebSocket &socket) {
            spdlog::info("Signaling Server Connected: {}", ReceiverConfig::serverUrl);
        }

        static void onClose(ix::WebSocket &socket, std::string_view reason) {
            spdlog::info("Signaling Server Disconnected: {} Reason: {}", ReceiverConfig::serverUrl, reason);
            common::ThreadManager::terminate();
        }

        static void onMessage(ix::WebSocket &socket, std::string_view message) {
            try {
                nlohmann::json j = nlohmann::json::parse(message);
                const std::string type = j.value("type", "");
                if (type == "turn_credentials_payload") {
                    const auto turnCredentialsPayload = j.get<common::TurnCredentialsPayload>();
                    common::ThreadManager::postTask(
                        [turnCredentialsPayload = std::move(turnCredentialsPayload), &socket]() {
                            if (turnCredentialsPayload.username != "none" || turnCredentialsPayload.password !=
                                "none") {
                                if (auto turnServer = common::Utils::toTurnServer(
                                    turnCredentialsPayload.turnUrl, turnCredentialsPayload.username,
                                    turnCredentialsPayload.password); turnServer.has_value()) {
                                    common::IceHandler::addTurnServer(turnServer.value());
                                }
                            }


                            common::IceHandler::gatherLocalCandidates(false, "",1,
                                                                      [&socket](common::CandidatesResult result) {

                                                                          if (result.serializedCandidates.empty()) {
                                                                            spdlog::error("P2P Negotiation failed: Route unavailable.");
                                                                              common::ThreadManager::terminate();
                                                                              return;
                                                                          }

                                                                          spdlog::info("Joining session : {}", ReceiverConfig::joinCode);
                                                                          socket.send(nlohmann::json(
                                                                              common::JoinTransferSessionPayload{
                                                                                  .candidatesResult = std::move(result),
                                                                                  .joinCode = ReceiverConfig::joinCode,
                                                                              }).dump());
                                                                      });
                        });
                } else if (type == "accept_transfer_session_payload") {
                    const auto acceptedTransferSessionPayload = j.get<common::AcceptTransferSessionPayload>();
                    spdlog::info("Access verified. Starting P2P negotiation...");
                    common::ThreadManager::postTask([payload = std::move(acceptedTransferSessionPayload), &socket]() {
                        common::IceHandler::establishConnection(
                            false, "",
                            payload.candidatesResult,
                            [&socket](NiceAgent *agent, const bool success, const guint streamId,
                                      const int n) {
                                if (success) {
                                    spdlog::info(
                                        "P2P Route Established.");
                                    ReceiverStream::receiveTransfer(
                                        agent, streamId);
                                    socket.send(nlohmann::json(common::AcknowledgeTransferSessionPayload{
                                        .receiverId = "to_be_provided_by_server"
                                    }).dump());
                                } else {
                                    spdlog::error("P2P Negotiation failed: Route unavailable.");
                                    common::ThreadManager::terminate();
                                }
                            });
                    });
                }
            } catch (const std::exception &e) {
                socket.close();
            }
        }
    };
}
