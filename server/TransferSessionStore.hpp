#pragma once
#include "ServerConfig.hpp"
#include "TransferSession.hpp"
#include "../common/Payloads.hpp"
#include "../common/TTLCache.hpp"
#include "../common/Types.hpp"

namespace server {

    class TransferSessionStore {
    public:
        static TransferSessionStore& instance() {
            static TransferSessionStore inst;
            return inst;
        }



        TransferSessionStore(const TransferSessionStore&) = delete;
        TransferSessionStore& operator=(const TransferSessionStore&) = delete;


        std::optional<std::shared_ptr<TransferSession>> getTransferSession(const std::string& senderId) {
            return cache_.get(senderId);
        }

        std::optional<std::shared_ptr<TransferSession>> getTransferSessionByJoinCode(const std::string& joinCode) {
            for (const auto &session: cache_ | std::views::values) {
                if (session->value->joinCode() == joinCode) return session->value;
            }
            return std::nullopt;
        }

        std::optional<std::shared_ptr<TransferSession>> getTransferSessionByReceiverId(const std::string& receiverId) {
            for (const auto& session: cache_ | std::views::values) {
                if (session->value->hasReceiver(receiverId)) {
                    return session->value;
                }
            }
            return std::nullopt;
        }

        std::shared_ptr<TransferSession> removeTransferSession(const std::string& senderId) {
            return cache_.erase(senderId);
        }

        std::shared_ptr<TransferSession> createSessionFrom(common::Session* senderSession, const common::CreateTransferSessionPayload& payload) {
            auto& id = senderSession->getUserData()->id;
            auto transferSession = std::make_shared<TransferSession>(id, payload);
            try {
                cache_.put(id, transferSession);
            }
            catch (std::exception& e) {
                spdlog::warn("A session could not be created due to max sessions limit: {}", ServerConfig::maxSessions);
                return nullptr;
            }
            spdlog::info("New session with join code {} has been created", transferSession->joinCode());
            return transferSession;
        }



        void cleanExpiredSessions() {
            cache_.cleanExpired();
        }


    private:
        //maps sender id => TransferSession
        TTLCache<std::string, std::shared_ptr<TransferSession>> cache_;
        TransferSessionStore()
           : cache_(
               ServerConfig::sessionTimeout,
               static_cast<std::size_t>(ServerConfig::maxSessions),
               [](const std::shared_ptr<TransferSession>& transferSession) {
                   if (!transferSession) return;
                   spdlog::info("A session with join code {} has expired, destroying the session.", transferSession->joinCode());
                   transferSession->destroy("Session expired");
               }
           ) {}
    };
}
