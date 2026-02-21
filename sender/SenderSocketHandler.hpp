#pragma once
#include <IXWebSocket.h>
#include <memory>
#include <nlohmann/json.hpp>
#include <nlohmann/json_fwd.hpp>
#include <spdlog/spdlog.h>
#include "SenderConfig.hpp"
#include "../common/Payloads.hpp"
#include "../common/Utils.hpp"
#include "SenderContexts.hpp"
#include "SenderStream.hpp"
#include "../common/ThreadManager.hpp"

namespace sender {
    class SenderSocketHandler {
    public:
        static void onConnect(ix::WebSocket &socket) {
            spdlog::info("Signaling Server Cnnected: {}", SenderConfig::serverUrl);
        }

        static void onClose(ix::WebSocket &socket, std::string_view reason) {
            spdlog::info("Signaling Server Disconnected: {} Reason: {}", SenderConfig::serverUrl, reason);
            common::ThreadManager::terminate();
        }

        static void onMessage(ix::WebSocket &socket, std::string_view message) {
            try {
                nlohmann::json j = nlohmann::json::parse(message);
                const std::string type = j.value("type", "");
                if (type == "turn_credentials_payload") {
                    const auto turnCredentialsPayload = j.get<common::TurnCredentialsPayload>();

                    common::ThreadManager::postTask(
                        [&socket,turnCredentialsPayload = std::move(turnCredentialsPayload)]() {
                            if (turnCredentialsPayload.username != "none" || turnCredentialsPayload.password !=
                                "none") {
                                if (auto turnServer = common::Utils::toTurnServer(
                                    turnCredentialsPayload.turnUrl, turnCredentialsPayload.username,
                                    turnCredentialsPayload.password); turnServer.has_value()) {
                                    common::IceHandler::addTurnServer(turnServer.value());
                                }
                            }

                            senderPersistentContext.buildManifest(SenderConfig::paths);

                            const auto createTransferSessionPayload =
                                    common::CreateTransferSessionPayload{
                                        .maxReceivers = SenderConfig::maxReceivers,
                                        .totalSize = senderPersistentContext.totalExpectedBytes,
                                        .filesCount = senderPersistentContext.totalExpectedFilesCount,
                                    };

                            socket.send(nlohmann::json(createTransferSessionPayload).dump());
                        });
                } else if (type == "created_transfer_session_payload") {
                    const auto createdTransferPayload = j.get<common::CreatedTransferSessionPayload>();
                    spdlog::info("Secure Join Code Generated : \033[1;36m{}\033[0m", createdTransferPayload.joinCode);
                    spdlog::info("Run on receiver : thru join {}", createdTransferPayload.joinCode);
                    common::ThreadManager::postTask([createdTransferPayload = std::move(createdTransferPayload)]() {
                        senderPersistentContext.joinCode = createdTransferPayload.joinCode;
                    });
                } else if (type == "join_transfer_session_payload") {
                    const auto joinTransferSessionPayload = j.get<common::JoinTransferSessionPayload>();
                    if (senderPersistentContext.receiversCount >= SenderConfig::maxReceivers) {
                        socket.send(nlohmann::json(common::RejectTransferSessionPayload{
                            .receiverId = joinTransferSessionPayload.receiverId,
                            .reason = "Sender does not accept any more receivers",
                        }).dump());
                        return;
                    }

                    common::ThreadManager::postTask(
                        [&socket,joinTransferSessionPayload = std::move(joinTransferSessionPayload)]() {
                            auto &receiverId = joinTransferSessionPayload.receiverId;
                            common::IceHandler::gatherLocalCandidates(true, receiverId, 1,
                                                                      [&socket, receiverId = std::move(receiverId),
                                                                          payload = std::move(
                                                                              joinTransferSessionPayload)](
                                                                  common::CandidatesResult result) {
                                                                          if (result.serializedCandidates.empty()) {
                                                                              socket.send(nlohmann::json(
                                                                                  common::RejectTransferSessionPayload
                                                                                  {
                                                                                      .receiverId = receiverId,
                                                                                      .reason =
                                                                                      "P2P Negotiation failed: Route unavailable."
                                                                                  }).dump());
                                                                              return;
                                                                          }

                                                                          senderPersistentContext.receiversCount.
                                                                                  fetch_add(1);
                                                                          socket.send(nlohmann::json(
                                                                              common::AcceptTransferSessionPayload
                                                                              {
                                                                                  .candidatesResult =
                                                                                  std::move(result),
                                                                                  .receiverId = receiverId,
                                                                              }).dump());

                                                                          common::IceHandler::establishConnection(
                                                                              true,
                                                                              receiverId,
                                                                              payload.candidatesResult,
                                                                              [receiverId = std::move(receiverId), &
                                                                                  socket, result = std::move(result)](
                                                                          NiceAgent *agent, const bool success,
                                                                          guint streamId,
                                                                          const int n) {
                                                                                  if (!success) {
                                                                                      socket.send(nlohmann::json(
                                                                                          common::RejectTransferSessionPayload
                                                                                          {
                                                                                              .receiverId = receiverId,
                                                                                              .reason =
                                                                                              "P2P Negotiation failed: Route unavailable."
                                                                                          }).dump());
                                                                                  }
                                                                              });
                                                                      });
                        });
                } else if (type == "quit_transfer_session_payload") {
                    const auto quitTransferSessionPayload = j.get<common::QuitTransferSessionPayload>();
                    common::ThreadManager::postTask(
                        [quitTransferSessionPayload = std::move(quitTransferSessionPayload)]() {
                            senderPersistentContext.receiversCount.fetch_sub(1);
                            auto &receiverId = quitTransferSessionPayload.receiverId;
                            SenderStream::disposeReceiverConnection(receiverId);
                        });
                } else if (type == "acknowledge_transfer_session_payload") {
                    const auto acknowledgeTransferSessionPayload = j.get<common::AcknowledgeTransferSessionPayload>();

                    common::ThreadManager::postTask(
                        [acknowledgeTransferSessionPayload = std::move(acknowledgeTransferSessionPayload)]() {
                            const auto &iceContext = common::IceHandler::getAgentsMap()[
                                acknowledgeTransferSessionPayload.receiverId];

                            SenderStream::startTransfer(iceContext.agent, iceContext.streamId,
                                                        acknowledgeTransferSessionPayload.receiverId);
                        });
                }
            } catch (const std::exception &e) {
                socket.close();
            }
        }
    };
}
