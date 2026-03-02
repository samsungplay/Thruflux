#pragma once
#include <latch>
#include <queue>
#include <nlohmann/json.hpp>

namespace ui {

    struct EventStream {
        std::mutex mutex;
        std::condition_variable cv;
        std::queue<std::string> messageQueue;

        void sendMessage(const std::string type, const std::string message) {
            const auto jsonMessage = nlohmann::json{{"type",std::move(type)},{"message",std::move(message)}}.dump() + "\n\n";
            {
                std::lock_guard lock(mutex);
                messageQueue.push(jsonMessage);
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
                                       percent, filesMoved, totalExpectedFilesCount, isRelayed);

}