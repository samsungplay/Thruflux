#pragma once
#include <indicators/dynamic_progress.hpp>
#include "../common/Contexts.hpp"
#include "../common/Stream.hpp"
#include <llfio/llfio.hpp>

namespace sender {
    struct FileInfo {
        uint32_t id;
        uint64_t size;
        std::string path;
        std::string relativePath;
    };


    struct SenderPersistentContext {
        std::string joinCode;
        std::uint64_t totalExpectedBytes;
        int totalExpectedFilesCount;
        std::vector<FileInfo> files;
        std::vector<uint8_t> manifestBlob;
        std::list<std::unique_ptr<indicators::ProgressBar> > progressBarsStorage;
        indicators::DynamicProgress<indicators::ProgressBar> progressBars;
        common::FileHandleCache cache;


        SenderPersistentContext() {
            progressBars.set_option(indicators::option::HideBarWhenComplete(false));
        }

        std::vector<uint64_t> fileChunkBase;
        uint64_t totalChunks = 0;
        std::atomic<int> receiversCount{0};


        void buildManifest(const std::vector<std::string> &paths) {
            files.clear();

            std::uint64_t totalSize = 0;
            int filesCount = 0;

            indicators::ProgressBar scannerBar{
                indicators::option::BarWidth{0},
                indicators::option::Start{""},
                indicators::option::End{""},
                indicators::option::ShowPercentage{false},
                indicators::option::PrefixText{"Cataloging... "},
                indicators::option::PostfixText{"0 file(s), 0 B"},
                indicators::option::ForegroundColor{indicators::Color::white}
            };

            for (auto &path: paths) {
                std::filesystem::path root(path);
                if (!std::filesystem::exists(root)) {
                    continue;
                }
                if (std::filesystem::is_regular_file(root)) {
                    auto size = std::filesystem::file_size(root);
                    totalSize += size;
                    filesCount++;
                    auto u8 = root.filename().generic_u8string();
                    const auto relativePath = std::string(u8.begin(), u8.end());
                    u8 = root.generic_u8string();
                    const auto absolutePath = std::string(u8.begin(), u8.end());
                    files.push_back({
                        0,
                        size,
                        absolutePath,
                        relativePath
                    });

                    if (filesCount % 1000 == 0) {
                        std::string stats = std::to_string(filesCount) + " file(s), " +
                                            common::Utils::sizeToReadableFormat(totalSize);
                        scannerBar.set_option(indicators::option::PostfixText{stats});
                        scannerBar.print_progress();
                    }
                } else {
                    for (const auto &entry: std::filesystem::recursive_directory_iterator(root)) {
                        if (entry.is_regular_file()) {
                            auto size = entry.file_size();
                            totalSize += size;
                            filesCount++;

                            auto relative = entry.path().lexically_relative(root.parent_path());
                            auto u8 = relative.generic_u8string();
                            auto relativePath = std::string(u8.begin(), u8.end());
                            u8 = entry.path().generic_u8string();
                            auto absolutePath = std::string(u8.begin(), u8.end());
                            files.push_back({
                                0,
                                size,
                                absolutePath,
                                relativePath
                            });
                            if (filesCount % 1000 == 0) {
                                std::string stats =
                                        std::to_string(filesCount) + " file(s), " + common::Utils::sizeToReadableFormat(
                                            totalSize);
                                scannerBar.set_option(indicators::option::PostfixText{stats});
                                scannerBar.print_progress();
                            }
                        }
                    }
                }
            }

            //sort for stable file ids (for resuming)
            std::sort(files.begin(), files.end(),
                      [](const FileInfo &a, const FileInfo &b) {
                          return a.relativePath < b.relativePath;
                      });
            for (uint32_t i = 0; i < files.size(); ++i) {
                files[i].id = i;
            }

            cache.reset(files.size());
            for (auto &f: files) cache.registerPath(f.id, f.path);


            std::string stats = std::to_string(filesCount) + " file(s), " + common::Utils::sizeToReadableFormat(
                                    totalSize);
            scannerBar.set_option(indicators::option::PrefixText{"Encoding Manifest... "});
            scannerBar.set_option(indicators::option::PostfixText{stats});
            scannerBar.print_progress();


            totalExpectedBytes = totalSize;
            totalExpectedFilesCount = filesCount;

            size_t estimatedSize = 4;
            for (const auto &f: files) estimatedSize += (14 + f.relativePath.size());
            manifestBlob.clear();
            manifestBlob.resize(estimatedSize);
            uint8_t *p = manifestBlob.data();
            const uint32_t count = static_cast<uint32_t>(files.size());
            memcpy(p, &count, 4);
            p += 4;

            for (const auto &f: files) {
                uint16_t nl = static_cast<uint16_t>(f.relativePath.size());
                memcpy(p, &f.id, 4);
                p += 4;
                memcpy(p, &f.size, 8);
                p += 8;
                memcpy(p, &nl, 2);
                p += 2;
                memcpy(p, f.relativePath.data(), nl);
                p += nl;
            }

            fileChunkBase.resize(files.size());
            totalChunks = 0;
            for (const auto &f: files) {
                fileChunkBase[f.id] = totalChunks;
                totalChunks += common::Utils::ceilDiv(f.size, common::CHUNK_SIZE);
            }

            scannerBar.set_option(indicators::option::PrefixText{"Manifest Sealed. "});
            scannerBar.mark_as_completed();
        }


        int addNewProgressBar(std::string prefix) {
            progressBarsStorage.push_back(common::Utils::createProgressBarUniquePtr(std::move(prefix)));
            const size_t id = progressBars.push_back(*progressBarsStorage.back());
            return id;
        }
    };

    inline SenderPersistentContext senderPersistentContext;


    //1 connection = 1 transfer = 1 receiver
    struct SenderConnectionContext : common::ConnectionContext {
        std::string receiverId;
        bool manifestStreamCreated = false;
        bool dataStreamCreated = false;
        size_t currentFileIndex = 0;
        uint64_t currentFileOffset = 0;
        bool manifestCreated = false;
        size_t manifestSent = 0;
        size_t progressBarIndex = 0;
        std::vector<uint8_t> ackBuf;
        uint64_t logicalBytesMoved = 0;
        uint64_t lastLogicalBytesMoved = 0;
        uint32_t resumeFileId = 0;
        uint64_t resumeOffset = 0;

    };

    struct SenderStreamContext {
        SenderConnectionContext *connectionContext = nullptr;
        bool typeByteSent = false;
        bool isManifestStream = false;
        std::vector<uint8_t> readBuf;
        int id = 0;
        uint32_t pinnedFileId = UINT32_MAX;
        llfio::file_handle *pinnedHandle = nullptr;
        uint64_t fileSize = 0;
        uint64_t fileOffset = 0;
        size_t bufReady = 0;
        size_t bufSent = 0;
        bool eofAll = false;

        void initialize() {
            if (readBuf.empty()) readBuf.resize(common::CHUNK_SIZE);

            if (!openCurrentFile()) {
                eofAll = true;
                return;
            }
            if (!fillBuf()) {
                while (fileOffset >= fileSize) {
                    if (!advanceFile()) { eofAll = true; return; }
                }
                if (!fillBuf()) { eofAll = true; }
            }
        }

        bool openCurrentFile() {
            if (connectionContext->currentFileIndex >= senderPersistentContext.files.size())
                return false;

            auto &f = senderPersistentContext.files[connectionContext->currentFileIndex];

            fileSize = f.size;
            fileOffset = connectionContext->currentFileOffset;

            if (fileOffset > fileSize) fileOffset = fileSize;

            if (pinnedFileId != f.id) {
                if (pinnedFileId != UINT32_MAX) senderPersistentContext.cache.release(pinnedFileId);
                pinnedFileId = f.id;
                pinnedHandle = senderPersistentContext.cache.acquire(f.id);
                if (!pinnedHandle) return false;
            }

            return true;
        }

        bool advanceFile() {
            connectionContext->currentFileIndex++;
            connectionContext->filesMoved++;
            connectionContext->currentFileOffset = 0;

            if (pinnedFileId != UINT32_MAX) {
                senderPersistentContext.cache.release(pinnedFileId);
                pinnedFileId = UINT32_MAX;
                pinnedHandle = nullptr;
            }
            return openCurrentFile();
        }

        bool fillBuf() {
            if (!pinnedHandle) return false;
            if (fileOffset >= fileSize) return true;

            const size_t len = std::min<uint64_t>(readBuf.size(), fileSize - fileOffset);

            llfio::byte_io_handle::buffer_type reqBuf({
                reinterpret_cast<llfio::byte *>(readBuf.data()),
                len
            });
            llfio::file_handle::io_request<llfio::file_handle::buffers_type> req(
                llfio::file_handle::buffers_type{&reqBuf, 1},
                fileOffset
            );

            auto result = pinnedHandle->read(req);
            if (!result) return false;

            const auto got = result.bytes_transferred();

            if (got == 0) return false;

            bufReady = got;
            bufSent = 0;
            return true;
        }
    };
}
