#pragma once
#include <spdlog/spdlog.h>

#include "SessionTracker.hpp"
#include "TransferSessionStore.hpp"
#include "../common/Utils.hpp"
#include "../common/Types.hpp"

namespace server {
    class ServerSocketHandler {
    public:
        static void onConnect(common::Session *session) {
            sessionTracker[session->getUserData()->id] = session;

            auto role = session->getUserData()->role;
            auto id = session->getUserData()->id;

            spdlog::info("New {} with id {} has joined!", role, id);
            if (!ServerConfig::turnServer.empty() && !ServerConfig::turnStaticAuthSecret.empty()) {
                spdlog::info("Issuing a new TURN credentials for user {}...", id);
                auto turnCredentialsPayload = common::Utils::generateTurnCredentials(ServerConfig::turnServer,
                    ServerConfig::turnStaticAuthSecret, id, ServerConfig::turnStaticCredTtl);
                session->send(nlohmann::json(turnCredentialsPayload).dump());
            } else {
                spdlog::info("No TURN configuration detected. Skipping issuing new TURN credentials for {}", id);
                auto dummyTurnCredentialsPayload = common::TurnCredentialsPayload{
                    .username = "none", .password = "none", .turnUrl = "none"
                };
                session->send(nlohmann::json(dummyTurnCredentialsPayload).dump());
            }
        }

        static void onClose(common::Session *session, std::string_view message) {
            const auto &id = session->getUserData()->id;
            const auto &role = session->getUserData()->role;
            sessionTracker.erase(id);
            spdlog::info("A {} with id {} has left. Reason: {}", session->getUserData()->role,
                         session->getUserData()->id.c_str(), message);
            if (role == "sender") {
                const auto removed = TransferSessionStore::instance().removeTransferSession(id);
                if (removed) {
                    removed->destroy();
                }
            } else {
                if (const auto receiverTransferSession = TransferSessionStore::instance().
                            getTransferSessionByReceiverId(id);
                    receiverTransferSession.has_value()) {
                    receiverTransferSession.value()->removeReceiver(id);
                    receiverTransferSession.value()->senderSession()->send(nlohmann::json(
                        common::QuitTransferSessionPayload{
                            .receiverId = id
                        }).dump());
                }
            }
        }

        static void onMessage(common::Session *session, std::string_view message) {
            const bool isSender = session->getUserData()->role == "sender";

            try {
                nlohmann::json j = nlohmann::json::parse(message);
                const std::string type = j.value("type", "");
                if (isSender && type == "create_transfer_session_payload") {
                    if (session->getUserData()->sessionCreationAttempted) {
                        session->end(4000, "Only one session creation attempt is allowed per connection");
                        return;
                    }
                    session->getUserData()->sessionCreationAttempted = true;
                    const auto payload = j.get<common::CreateTransferSessionPayload>();
                    if (payload.maxReceivers > ServerConfig::maxReceiversPerSender) {
                        session->end(4000, "Server forbids sender to have max receivers of more than " + std::to_string(ServerConfig::maxReceiversPerSender));
                        return;
                    }
                    if (const auto currentTransfer = TransferSessionStore::instance().getTransferSession(
                        session->getUserData()->id); currentTransfer.has_value()) {
                        session->end(4000, "Duplicate Session");
                        return;
                    }
                    const auto transferSession = TransferSessionStore::instance().createSessionFrom(session, payload);
                    if (!transferSession) {
                        session->end(4000, "Server has reached max number of sessions");
                        return;
                    }


                    session->send(nlohmann::json(common::CreatedTransferSessionPayload{
                        .joinCode = transferSession->joinCode()
                    }).dump());
                } else if (!isSender && type == "join_transfer_session_payload") {
                    if (const auto currentSession = TransferSessionStore::instance().getTransferSessionByReceiverId(
                        session->getUserData()->id)) {
                        session->end(4000, "Duplicate Session");
                        return;
                    }
                    auto payload = j.get<common::JoinTransferSessionPayload>();
                    const auto transferSession = TransferSessionStore::instance().
                            getTransferSessionByJoinCode(payload.joinCode);

                    if (transferSession.has_value()) {
                        transferSession.value()->addReceiver(session->getUserData()->id);
                        payload.receiverId = session->getUserData()->id;
                        transferSession.value()->senderSession()->send(nlohmann::json(payload).dump());
                    } else {
                        session->end(4004, "No Session Found");
                    }
                } else if (isSender && type == "accept_transfer_session_payload") {
                    const auto payload = j.get<common::AcceptTransferSessionPayload>();
                    if (const auto transferSession = TransferSessionStore::instance().getTransferSession(
                        session->getUserData()->id); transferSession.has_value()) {
                        if (const auto receiverSession = transferSession.value()->getReceiver(payload.receiverId)) {
                            receiverSession->send(j.dump());
                        }
                    }
                } else if (!isSender && type == "acknowledge_transfer_session_payload") {
                    auto payload = j.get<common::AcknowledgeTransferSessionPayload>();
                    const auto transferSession = TransferSessionStore::instance().getTransferSessionByReceiverId(
                        session->getUserData()->id);
                    if (transferSession.has_value()) {
                        payload.receiverId = session->getUserData()->id;
                        transferSession.value()->senderSession()->send(nlohmann::json(payload).dump());
                    } else {
                        session->end(4004, "No Session Found While Acknowledging");
                    }
                }
                else if (isSender && type == "reject_transfer_session_payload") {
                    auto payload = j.get<common::RejectTransferSessionPayload>();
                    const auto receiverSession = sessionTracker[payload.receiverId];
                    if (receiverSession) {
                        receiverSession->end(4000, payload.reason);
                    }
                }
            } catch (const std::exception &e) {
                spdlog::error("Error occurred while handling socket message: {}", e.what());
                session->close();
            }
        }
    };
}
