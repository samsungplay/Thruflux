#pragma once
#include <latch>
#include <queue>
#include <nlohmann/json.hpp>

namespace ui {
    inline std::atomic isEnabled{false};

    struct EventStream {
        std::mutex mutex;
        std::condition_variable cv;
        std::queue<std::string> messageQueue;

        void sendMessage(const std::string type, const nlohmann::json jsonMessage = nullptr) {
            if (!isEnabled.load()) {
                return;
            }
            nlohmann::json j = nlohmann::json::object();
            j["type"] = type;
            if (!jsonMessage.is_null()) {
                j["message"] = jsonMessage;
            } else {
                j["message"] = "";
            }
            const auto msg = j.dump();
            {
                std::lock_guard lock(mutex);
                messageQueue.push(std::move(msg));
            }
            cv.notify_all();
        }
    };

    inline EventStream eventStream;

    struct UIProgressSnapshot {
        std::string receiverId = "";
        uint64_t totalExpectedBytes;
        double ewmaThroughput;
        uint64_t bytesMoved;
        uint64_t skippedBytes;
        int percent = 0;
        int filesMoved;
        int totalExpectedFilesCount;
        bool isRelayed;
        bool hasError = false;
    };

    NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(UIProgressSnapshot, totalExpectedBytes, ewmaThroughput, bytesMoved, skippedBytes,
                                       percent, filesMoved, totalExpectedFilesCount, isRelayed, receiverId, hasError);
}
