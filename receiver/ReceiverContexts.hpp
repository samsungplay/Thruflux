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
        std::chrono::steady_clock::time_point lastResumeFlush{};
        bool resumeDirty = false;
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
            uint8_t *p = manifestBuf.data();
            uint32_t count;
            memcpy(&count, p, 4);
            cache.reset(count);
            p += 4;
            fileSizes.resize(count);

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
                std::string relativePath(reinterpret_cast<char *>(p), l);
                p += l;

                std::filesystem::path full = std::filesystem::path(ReceiverConfig::out) / relativePath;
                std::filesystem::create_directories(full.parent_path());
                cache.registerPath(id, full.string());
            }


            const auto manifestHash = common::Utils::fnv1a64(manifestBuf.data(), manifestBuf.size());
            auto statePath = std::filesystem::path(ReceiverConfig::out) /
                             (".thruflux_resume_" + std::to_string(manifestHash) + ".state");
            resumeStatePath = statePath.string();

            if (ReceiverConfig::overwrite) {
                std::error_code ec;
                std::filesystem::remove(statePath, ec);
                resumeFileId = 0;
                resumeOffset = 0;
            } else {
                if (std::filesystem::exists(statePath)) {
                    resumeFileId = 0;
                    resumeOffset = 0;
                    {
                        std::ifstream in(statePath, std::ios::binary);
                        if (in) {
                            uint32_t fid = 0;
                            uint64_t off = 0;
                            in.read(reinterpret_cast<char *>(&fid), sizeof(fid));
                            in.read(reinterpret_cast<char *>(&off), sizeof(off));

                            if (in.good() && in.gcount() == sizeof(off) && fid < fileSizes.size()) {
                                resumeFileId = fid;
                                resumeOffset = std::min(off, fileSizes[fid]);
                            }
                        }
                    }

                    while (resumeFileId < fileSizes.size() && resumeOffset >= fileSizes[resumeFileId]) {
                        resumeOffset = 0;
                        resumeFileId++;
                    }

                    if (resumeFileId >= fileSizes.size()) {
                        resumeFileId = fileSizes.size();
                        resumeOffset = 0;
                    }

                    uint64_t resumedBytes = 0;
                    for (uint32_t id = 0; id < resumeFileId && id < fileSizes.size(); ++id) {
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
                }
            }

            spdlog::info("Manifest unsealed: {} file(s) , Total size: {}", count,
                         common::Utils::sizeToReadableFormat(totalExpectedBytes));
        }

        void maybeSaveResumeState(bool force = false) {
            if (!resumeDirty) return;

            const auto now = std::chrono::steady_clock::now();
            const bool timeOk = lastResumeFlush.time_since_epoch().count() == 0 ||
                                std::chrono::duration<double>(now - lastResumeFlush).count() >= 1.0;

            if (!force && !timeOk) {
                return;
            }

            const std::string tmp = resumeStatePath + ".tmp";
            std::ofstream out(tmp, std::ios::binary | std::ios::trunc);
            if (!out) return;

            out.write(reinterpret_cast<const char *>(&resumeFileId), sizeof(resumeFileId));
            out.write(reinterpret_cast<const char *>(&resumeOffset), sizeof(resumeOffset));

            out.flush();

            std::error_code ec;
            std::filesystem::rename(tmp, resumeStatePath, ec);
            if (ec) {
                std::filesystem::remove(resumeStatePath, ec);
                ec.clear();
                std::filesystem::rename(tmp, resumeStatePath, ec);
            }

            resumeDirty = false;
            lastResumeFlush = now;
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
            connCtx->resumeDirty = true;

            return true;
        }
    };
}
