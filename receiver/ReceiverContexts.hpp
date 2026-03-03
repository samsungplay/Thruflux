#pragma once
#include "ReceiverConfig.hpp"
#include "../common/Contexts.hpp"
#ifdef _MSC_VER
#include <intrin.h>
#pragma intrinsic(__popcnt)
static inline int popcount32(unsigned int x) {
    return (int) __popcnt(x);
}
#else
static int popcount32(unsigned int x) {
    return __builtin_popcount(x);
}
#endif


namespace receiver {
    inline static constexpr size_t STAGE_LIMIT = 16 * 1024 * 1024;
    inline static constexpr size_t FLUSH_AT = 8 * 1024 * 1024;

    struct ReceiverConnectionContext : common::ConnectionContext {
        common::FileHandleCache cache;
        std::vector<uint8_t> manifestBuf;
        bool manifestParsed = false;
        uint64_t totalExpectedBytes = 0;
        int totalExpectedFilesCount = 0;
        std::vector<uint64_t> fileSizes;
        bool pendingManifestAck = false;
        bool pendingCompleteAck = false;
        std::unique_ptr<indicators::ProgressBar> progressBar;
        uint32_t resumeFileId = 0;
        uint64_t resumeOffset = 0;
        std::string resumeStatePath;
        int manifestAckSent = 0;

        indicators::ProgressBar manifestProgressBar{
            indicators::option::BarWidth{0},
            indicators::option::Start{""},
            indicators::option::End{""},
            indicators::option::ShowPercentage{false},
            indicators::option::PrefixText{"Fetching catalogue.. "},
            indicators::option::PostfixText{" received 0B"},
            indicators::option::ForegroundColor{indicators::Color::white}
        };


        std::chrono::steady_clock::time_point lastManifestProgressPrint{};

        void createProgressBar(std::string prefix) {
            progressBar = common::Utils::createProgressBarUniquePtr(prefix);
        };


        void parseManifest() {
            ui::eventStream.sendMessage("manifest_parsing");

            uint8_t *p = manifestBuf.data();
            uint32_t count;
            memcpy(&count, p, 4);
            cache.reset(count);
            p += 4;
            fileSizes.resize(count);
            auto outPath = std::filesystem::u8path(ReceiverConfig::out);

            for (int i = 0; i < count; i++) {
                uint32_t id;
                memcpy(&id, p, 4);
                p += 4;
                uint64_t sz;
                memcpy(&sz, p, 8);
                fileSizes[id] = sz;
                totalExpectedBytes += sz;
                totalExpectedFilesCount++;
                p += 8;
                uint16_t l;
                memcpy(&l, p, 2);
                p += 2;
                std::string relativePathU8(reinterpret_cast<char *>(p), l);
                p += l;

                auto relativePath = std::filesystem::u8path(relativePathU8);

                std::filesystem::path fullPath = outPath / relativePath;
                std::error_code ec;
                std::filesystem::create_directories(fullPath.parent_path(), ec);
                cache.registerPath(id, fullPath);
            }


            const auto manifestHash = common::Utils::fnv1a64(manifestBuf.data(), manifestBuf.size());
            auto statePath = outPath /
                             (".thruflux_resume_" + std::to_string(manifestHash) + ".state");
            resumeStatePath = statePath.string();

            resumeFileId = 0;
            resumeOffset = 0;

            if (!ReceiverConfig::overwrite) {
                //calculate resume state from disk instead of manual resume sidecar files

                uint64_t resumedBytes = 0;
                for (uint32_t id = 0; id < count; id++) {
                    const auto &fullPath = cache.paths[id];
                    std::error_code ec;
                    if (!std::filesystem::exists(fullPath, ec)) {
                        resumeFileId = id;
                        resumeOffset = 0;
                        break;
                    }

                    auto actualSize = std::filesystem::file_size(fullPath, ec);
                    if (ec) {
                        resumeFileId = id;
                        resumeOffset = 0;
                        break;
                    }

                    const auto expectedSize = fileSizes[id];

                    if (actualSize < expectedSize) {
                        resumeFileId = id;
                        resumeOffset = actualSize;
                        break;
                    }

                    if (actualSize > expectedSize) {
                        resumeFileId = id;
                        resumeOffset = 0;
                        break;
                    }

                    resumedBytes += fileSizes[id];
                }

                resumedBytes += resumeOffset;

                bytesMoved = resumedBytes;
                lastBytesMoved = resumedBytes;
                skippedBytes = resumedBytes;
                filesMoved = resumeFileId;

                const auto resumePercent = bytesMoved / static_cast<double>(totalExpectedBytes) * 100;

                spdlog::info("Automatically resuming from around {}%. Pass --overwrite flag to disable.",
                             resumePercent);
                ui::eventStream.sendMessage("resume_notice", nlohmann::json{{"percent", resumePercent}});
            }

            ui::eventStream.sendMessage("manifest_unsealed",
                                        nlohmann::json{{"files_count", count}, {"total_size", totalExpectedBytes}});
            spdlog::info("Manifest unsealed: {} file(s) , Total size: {}", count,
                         common::Utils::sizeToReadableFormat(totalExpectedBytes));
        }

    };

    struct ReceiverStreamContext {
        enum StreamType { UNKNOWN, MANIFEST, DATA } type = UNKNOWN;

        std::vector<uint8_t> stage;
        size_t stageLen = 0;

        uint32_t curFileId = 0;
        uint64_t curSize = 0;
        uint32_t pinnedFileId = UINT32_MAX;
        llfio::file_handle *pinnedHandle = nullptr;
        uint8_t writeBuffer[256 * 1024];
        uint64_t flushOff = 0;
        uint64_t recvOff = 0;

        ReceiverStreamContext() {
            stage.resize(STAGE_LIMIT);
        }

        bool openFile(ReceiverConnectionContext *connCtx, uint32_t fileId, uint64_t startOff = 0) {
            if (fileId >= connCtx->fileSizes.size()) return false;

            curFileId = fileId;
            curSize = connCtx->fileSizes[fileId];

            flushOff = startOff;
            recvOff = startOff;
            stageLen = 0;

            if (pinnedFileId != fileId) {
                if (pinnedFileId != UINT32_MAX) connCtx->cache.release(pinnedFileId);
                pinnedFileId = fileId;
                pinnedHandle = connCtx->cache.acquire(fileId, true);
                if (!pinnedHandle) return false;
            }
            return true;
        }

        bool flushStage(ReceiverConnectionContext *connCtx) {
            if (stageLen == 0) return true;
            if (!pinnedHandle) return false;


            llfio::byte_io_handle::const_buffer_type reqBuf({
                reinterpret_cast<const llfio::byte *>(stage.data()),
                stageLen
            });

            llfio::file_handle::io_request<llfio::file_handle::const_buffers_type> req(
                llfio::file_handle::const_buffers_type{&reqBuf, 1},
                flushOff
            );

            auto result = pinnedHandle->write(req);
            if (!result) return false;

            const size_t nw = result.bytes_transferred();
            if (nw != stageLen) return false;

            connCtx->bytesMoved += nw;

            flushOff += nw;
            stageLen = 0;

            connCtx->resumeFileId = curFileId;
            connCtx->resumeOffset = flushOff;

            return true;
        }
    };
}
