#pragma once
#include <agent.h>
#include <cstdint>
#include <lsquic_types.h>
#include <ranges>
#include <unordered_map>
#include <string>
#include <indicators/progress_bar.hpp>
#include <spdlog/spdlog.h>

#include <llfio/llfio.hpp>

#ifdef _WIN32
#include <io.h>
#include <fcntl.h>
typedef int mode_t;
#define O_RDONLY _O_RDONLY
#define O_WRONLY _O_WRONLY
#define O_RDWR _O_RDWR
#define S_IRUSR _S_IREAD
#define S_IWUSR _S_IWRITE
#endif

namespace llfio = LLFIO_V2_NAMESPACE;

namespace common {
    inline constexpr char RECEIVER_MANIFEST_RECEIVED_ACK = 0x06;
    inline constexpr char RECEIVER_TRANSFER_COMPLETE_ACK = 0x07;
    inline static constexpr uint64_t CHUNK_SIZE = 2 * 1024 * 1024; //controls disk io buffer size

    struct FileHandleCache {
        struct Entry {
            llfio::file_handle fh{};
            bool open = false;
            uint32_t pinCount = 0;
            int prev = -1;
            int next = -1;
        };

        size_t maxFds = 128;
        std::vector<std::string> paths;
        std::vector<Entry> entries;

        int head = -1;
        int tail = -1;
        size_t openCount = 0;

        FileHandleCache() = default;

        explicit FileHandleCache(size_t fileCount, size_t maxFds_ = 128) {
            reset(fileCount, maxFds_);
        }

        void reset(size_t fileCount, size_t maxFds_ = 128) {
            closeAll();
            maxFds = maxFds_;
            paths.clear();
            paths.resize(fileCount);
            entries.clear();
            entries.resize(fileCount);
            head = tail = -1;
            openCount = 0;
        }

        void registerPath(uint32_t id, std::string p) {
            if (id >= paths.size()) {
                paths.resize(id + 1);
                entries.resize(id + 1);
            }
            paths[id] = std::move(p);
        }

        llfio::file_handle* acquire(uint32_t id, bool write = false) {
            if (id >= entries.size() || id >= paths.size() || paths[id].empty()) return nullptr;

            Entry &e = entries[id];

            if (e.open && e.fh.is_valid()) {
                ++e.pinCount;
                touch((int)id);
                return &e.fh;
            }

            while (openCount >= maxFds) {
                if (!evictOne()) break;
            }

            if (openCount >= maxFds) {
                spdlog::error("FileHandleCache: cannot evict maxFds={}", maxFds);
                return nullptr;
            }

            auto opened = write ? llfio::file({},paths[id], llfio::file_handle::mode::write, llfio::file_handle::creation::if_needed) : llfio::file({}, paths[id]);
            if (!opened) {
                spdlog::error("Failed to open file id {} path='{}' err={}", id, paths[id], opened.error().message());
                return nullptr;
            }

            if (e.prev != -1 || e.next != -1 || head == (int)id || tail == (int)id)
                removeFromList((int)id);

            e.fh = std::move(opened).value();
            e.open = true;
            e.pinCount = 1;
            pushFront((int)id);
            ++openCount;
            return &e.fh;
        }

        void release(uint32_t id) {
            if (id >= entries.size()) return;
            Entry &e = entries[id];
            if (e.pinCount > 0) --e.pinCount;
        }

        bool evictOne() {
            int cur = tail;
            while (cur != -1) {
                Entry &e = entries[static_cast<size_t>(cur)];
                if (e.open && e.fh.is_valid() && e.pinCount == 0) {
                    auto r = e.fh.close();
                    if (!r) spdlog::warn("failed to close {} : {}", paths[cur], r.error().message());

                    e.fh = llfio::file_handle{};
                    e.open = false;

                    removeFromList(cur);
                    --openCount;

                    return true;
                }
                cur = e.prev;
            }
            return false;
        }


        void closeAll() {
            for (size_t i = 0; i < entries.size(); ++i) {
                auto &e = entries[i];
                if (e.open && e.fh.is_valid()) {
                    auto r = e.fh.close();
                    if (!r) spdlog::warn("close('{}') failed: {}", paths[i], r.error().message());
                }
                e.fh = llfio::file_handle{};
                e.open = false;
                e.pinCount = 0;
                e.prev = e.next = -1;
            }
            head = tail = -1;
            openCount = 0;
        }

        ~FileHandleCache() { closeAll(); }

    private:
        void removeFromList(int id) {
            if (id == -1) return;
            Entry &e = entries[id];

            if (e.prev != -1) entries[e.prev].next = e.next;
            if (e.next != -1) entries[e.next].prev = e.prev;

            if (head == id) head = e.next;
            if (tail == id) tail = e.prev;

            e.prev = e.next = -1;
        }


        void pushFront(int id) {
            Entry &e = entries[id];
            e.prev = -1;
            e.next = head;
            if (head != -1) entries[head].prev = id;
            head = id;
            if (tail == -1) tail = id;
        }

        void touch(int id) {
            if (head == id) return;
            removeFromList(id);
            pushFront(id);
        }

    };


    struct ConnectionContext {
        NiceAgent *agent;
        guint streamId;
        mutable lsquic_conn_t *connection;
        sockaddr_storage localAddr;
        sockaddr_storage remoteAddr;
        std::chrono::steady_clock::time_point startTime;
        std::chrono::steady_clock::time_point lastTime;
        uint64_t bytesMoved = 0;
        uint64_t lastBytesMoved = 0;
        int filesMoved = 0;
        double ewmaThroughput;
        bool started = false;
        bool complete = false;
        lsquic_stream_t *manifestStream = nullptr;
        uint64_t skippedBytes = 0;
        enum ConnectionType { DIRECT, RELAYED };
        ConnectionType connectionType = DIRECT;
    };
}
