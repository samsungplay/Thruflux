#pragma once
#include <ranges>
#include <utility>
#include "../common/Payloads.hpp"
#include "../common/Types.hpp"
#include "../common/Utils.hpp"

namespace server {
    class ServerSocketHandler;

    class TransferSession {
        std::string senderSessionId_;
        std::string joinCode_;
        int maxReceivers_;
        long totalSize_;
        int filesCount_;
        std::unordered_set<std::string> receiverIds_;

    public:
        TransferSession(std::string senderSessionId,
                        const common::CreateTransferSessionPayload &payload) : senderSessionId_(
                                                                                   std::move(senderSessionId)),
                                                                               joinCode_(
                                                                                   common::Utils::generateJoinCode()),
                                                                               maxReceivers_(payload.maxReceivers),
                                                                               totalSize_(payload.totalSize),
                                                                               filesCount_(payload.filesCount) {
        }

        [[nodiscard]] common::Session *senderSession() const {
            const auto it = sessionTracker.find(senderSessionId_);
            if (it == sessionTracker.end()) {
                return nullptr;
            }
            return it->second;
        }


        [[nodiscard]] const std::string &joinCode() const {
            return joinCode_;
        }

        void addReceiver(std::string receiverId) {
            receiverIds_.insert(std::move(receiverId));
        }


        common::Session *getReceiver(const std::string &receiverId) const {
            if (receiverIds_.contains(receiverId)) {
                const auto it = sessionTracker.find(receiverId);
                if (it == sessionTracker.end()) {
                    return nullptr;
                }
                return it->second;
            }
            return nullptr;
        }

        bool hasReceiver(const std::string &receiverId) const {
            return receiverIds_.contains(receiverId);
        }

        void removeReceiver(const std::string &receiverId) {
            receiverIds_.erase(receiverId);
        }

        void destroy(std::string customErrorMessage = "Session destroyed") {
            spdlog::info("A session with join code {} has been destroyed.", joinCode_);
            if (const auto it = sessionTracker.find(senderSessionId_); it != sessionTracker.end()) {
                it->second->end(4000, std::move(customErrorMessage));
            }

            for (const auto &receiverId: receiverIds_) {
                if (const auto pair = sessionTracker.find(receiverId); pair != sessionTracker.end()) {
                    pair->second->end(4000, std::move(customErrorMessage));
                }
            }
            receiverIds_.clear();
        }
    };
}
