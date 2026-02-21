#pragma once
#include <lsquic.h>

#include "SenderConfig.hpp"
#include "../common/Stream.hpp"
#include "SenderContexts.hpp"
#include <indicators/dynamic_progress.hpp>
#include <indicators/multi_progress.hpp>
#include <indicators/progress_bar.hpp>
#include "../common/IceHandler.hpp"

namespace sender {
    class SenderStream : public common::Stream {
        //unlike receiver, sender's progress reporter should run persistently
        static void watchProgress() {
            g_timeout_add_full(
                G_PRIORITY_HIGH,
                1000,
                [](gpointer) -> gboolean {
                    const auto now = std::chrono::steady_clock::now();
                    const double totalBytes = static_cast<double>(senderPersistentContext.totalExpectedBytes);

                    for (const auto &context: connectionContexts_) {
                        if (!context || !context->started || context->complete) continue;

                        auto &progressBar = senderPersistentContext.progressBars[static_cast<SenderConnectionContext *>(
                            context)->progressBarIndex];

                        if (context->lastTime.time_since_epoch().count() == 0) {
                            context->lastTime = now;
                            context->lastBytesMoved = context->bytesMoved;
                            progressBar.set_option(
                                indicators::option::PostfixText{"starting..."});
                            progressBar.set_progress(0);
                            continue;
                        }

                        const double elapsedSeconds =
                                std::chrono::duration<double>(now - context->startTime).count();
                        const double deltaSeconds =
                                std::chrono::duration<double>(now - context->lastTime).count();

                        const double safeDelta = (deltaSeconds > 1e-6) ? deltaSeconds : 1e-6;
                        const double safeElapsed = (elapsedSeconds > 1e-6) ? elapsedSeconds : 1e-6;

                        const double bytesMoved = static_cast<double>(context->bytesMoved);
                        const double lastBytesMoved = static_cast<double>(context->lastBytesMoved);

                        const double instantThroughput = (bytesMoved - lastBytesMoved) / safeDelta;
                        const double averageThroughput = bytesMoved / safeElapsed;

                        const double ewmaThroughput =
                                (context->ewmaThroughput == 0.0)
                                    ? instantThroughput
                                    : 0.2 * instantThroughput + 0.8 * context->ewmaThroughput;

                        context->ewmaThroughput = ewmaThroughput;

                        const double percent = (totalBytes <= 0.0)
                                                   ? 0.0
                                                   : (static_cast<SenderConnectionContext *>(
                                                          context)->logicalBytesMoved / totalBytes) * 100.0;
                        int p = static_cast<int>(std::lround(percent));
                        if (p < 0) p = 0;
                        if (p > 100) p = 100;

                        std::string postfix;
                        postfix.reserve(256);
                        postfix += common::Utils::sizeToReadableFormat(ewmaThroughput);
                        postfix += "/s sent ";
                        postfix += common::Utils::sizeToReadableFormat(context->bytesMoved);
                        postfix += " resumed ";
                        postfix += common::Utils::sizeToReadableFormat(context->skippedBytes);
                        postfix += " files ";
                        postfix += std::to_string(context->filesMoved);
                        postfix += "/";
                        postfix += std::to_string(senderPersistentContext.totalExpectedFilesCount);
                        postfix += " ";
                        postfix += context->connectionType == common::ConnectionContext::RELAYED ? "relayed" : "direct";

                        progressBar.set_option(indicators::option::PostfixText{postfix});
                        progressBar.set_progress(p);

                        context->lastTime = now;
                        context->lastBytesMoved = context->bytesMoved;
                    }

                    return G_SOURCE_CONTINUE;
                },
                nullptr,
                nullptr
            );
        }

        inline static lsquic_stream_if streamCallbacks = {

            .on_new_conn = [](void *streamIfCtx, lsquic_conn_t *connection) -> lsquic_conn_ctx * {
                auto *ctx = static_cast<SenderConnectionContext *>(lsquic_conn_get_peer_ctx(connection, nullptr));
                ctx->connection = connection;
                //open manifest stream
                lsquic_conn_make_stream(connection);
                return reinterpret_cast<lsquic_conn_ctx *>(ctx);
            },

            .on_conn_closed = [](lsquic_conn_t *connection) {
                auto *ctx = reinterpret_cast<SenderConnectionContext *>(lsquic_conn_get_ctx(connection));
                lsquic_conn_set_ctx(connection, nullptr);
                if (ctx) {
                    if (ctx->complete) {
                        auto &progressBar = senderPersistentContext.progressBars[ctx->progressBarIndex];
                        progressBar.set_option(
                            indicators::option::ForegroundColor{indicators::Color::green});
                        std::string postfix;
                        postfix.reserve(256);
                        postfix += " sent ";
                        postfix += common::Utils::sizeToReadableFormat(ctx->bytesMoved);
                        postfix += " resumed ";
                        postfix += common::Utils::sizeToReadableFormat(ctx->skippedBytes);
                        postfix += " files ";
                        postfix += std::to_string(ctx->filesMoved);
                        postfix += "/";
                        postfix += std::to_string(senderPersistentContext.totalExpectedFilesCount);
                        postfix += " ";
                        postfix += ctx->connectionType == common::ConnectionContext::RELAYED ? "relayed" : "direct";
                        postfix += " [DONE]";

                        progressBar.set_option(indicators::option::PostfixText{postfix});
                        progressBar.set_progress(100);
                        senderPersistentContext.progressBars.print_progress();
                    } else {
                        auto &progressBar = senderPersistentContext.progressBars[ctx->progressBarIndex];
                        progressBar.set_option(
                            indicators::option::ForegroundColor{indicators::Color::red});
                        std::string postfix;
                        postfix.reserve(256);
                        postfix += " sent ";
                        postfix += common::Utils::sizeToReadableFormat(ctx->bytesMoved);
                        postfix += " resumed ";
                        postfix += common::Utils::sizeToReadableFormat(ctx->skippedBytes);
                        postfix += " files ";
                        postfix += std::to_string(ctx->filesMoved);
                        postfix += "/";
                        postfix += std::to_string(senderPersistentContext.totalExpectedFilesCount);
                        postfix += " ";
                        postfix += ctx->connectionType == common::ConnectionContext::RELAYED ? "relayed" : "direct";
                        postfix += " [FAILED]";
                        progressBar.set_option(indicators::option::PostfixText{postfix});
                        progressBar.mark_as_completed();
                        senderPersistentContext.progressBars.print_progress();
                    }
                    std::erase(connectionContexts_, ctx);
                    ctx->connection = nullptr;

                    common::IceHandler::dispose(ctx->receiverId);

                    delete ctx;
                }
            },


            .on_new_stream = [](void *stream_if_ctx, lsquic_stream_t *stream) -> lsquic_stream_ctx * {
                auto *connCtx = reinterpret_cast<SenderConnectionContext *>(lsquic_conn_get_ctx(
                    lsquic_stream_conn(stream)));

                auto *ctx = new SenderStreamContext();

                ctx->connectionContext = connCtx;
                ctx->readBuf.resize(common::CHUNK_SIZE);


                if (!connCtx->manifestStreamCreated) {
                    ctx->isManifestStream = true;
                    connCtx->manifestStreamCreated = true;
                } else if (!connCtx->dataStreamCreated) {
                    ctx->isManifestStream = false;
                    connCtx->dataStreamCreated = true;
                    ctx->initialize();
                } else {
                    lsquic_stream_shutdown(stream, 1);
                    delete ctx;
                    return nullptr;
                }

                lsquic_stream_wantwrite(stream, 1);

                return reinterpret_cast<lsquic_stream_ctx *>(ctx);
            },
            .on_read = [](lsquic_stream_t *stream, lsquic_stream_ctx_t *h) {
                auto *ctx = reinterpret_cast<SenderStreamContext *>(h);
                auto *connCtx = reinterpret_cast<SenderConnectionContext *>(lsquic_conn_get_ctx(
                    lsquic_stream_conn(stream)));

                if (ctx->isManifestStream) {
                    uint8_t tmp[4096];
                    const ssize_t nr = lsquic_stream_read(stream, tmp, sizeof(tmp));
                    if (nr > 0) {
                        connCtx->ackBuf.insert(connCtx->ackBuf.end(), tmp, tmp + nr);
                    }

                    if (connCtx->ackBuf.empty()) return;

                    const uint8_t code = connCtx->ackBuf[0];

                    if (code == common::RECEIVER_MANIFEST_RECEIVED_ACK) {
                        const size_t need = 1 + 4 + 8;
                        if (connCtx->ackBuf.size() < need) return;
                        uint32_t fid = 0;
                        uint64_t off = 0;
                        memcpy(&fid, connCtx->ackBuf.data() + 1, 4);
                        memcpy(&off, connCtx->ackBuf.data() + 5, 8);
                        connCtx->ackBuf.erase(connCtx->ackBuf.begin(), connCtx->ackBuf.begin() + need);
                        connCtx->resumeFileId = fid;
                        connCtx->resumeOffset = off;

                        if (connCtx->resumeFileId >= senderPersistentContext.files.size()) {
                            connCtx->resumeFileId = senderPersistentContext.files.size();
                            connCtx->resumeOffset = 0;
                        } else {
                            auto sz = senderPersistentContext.files[connCtx->resumeFileId].size;
                            if (connCtx->resumeOffset > sz) connCtx->resumeOffset = sz;
                            while (connCtx->resumeFileId < senderPersistentContext.files.size() &&
                                   connCtx->resumeOffset >= senderPersistentContext.files[connCtx->resumeFileId].size) {
                                connCtx->resumeOffset = 0;
                                connCtx->resumeFileId++;
                            }
                        }

                        connCtx->currentFileIndex = connCtx->resumeFileId;
                        connCtx->currentFileOffset = connCtx->resumeOffset;

                        uint64_t resumedBytes = 0;
                        for (uint32_t i = 0; i < connCtx->resumeFileId && i < senderPersistentContext.files.size(); ++i)
                            resumedBytes += senderPersistentContext.files[i].size;
                        resumedBytes += connCtx->resumeOffset;

                        connCtx->logicalBytesMoved = resumedBytes;
                        connCtx->skippedBytes = resumedBytes;

                        //Time to blast data!
                        if (!connCtx->started) {
                            auto &progressBar = senderPersistentContext.progressBars[connCtx->progressBarIndex];
                            progressBar.set_option(indicators::option::PostfixText{"starting..."});
                            progressBar.set_progress(0);
                            connCtx->started = true;
                            connCtx->startTime = std::chrono::steady_clock::now();
                        }

                        //save the manifest stream for reading future ack
                        connCtx->manifestStream = stream;
                        lsquic_stream_wantread(stream, 0);
                        //Open data stream
                        lsquic_conn_make_stream(connCtx->connection);
                    } else if (code == common::RECEIVER_TRANSFER_COMPLETE_ACK) {
                        connCtx->ackBuf.erase(connCtx->ackBuf.begin());
                        connCtx->complete = true;
                        lsquic_stream_shutdown(stream, 0);
                        if (connCtx->connection) {
                            lsquic_conn_close(connCtx->connection);
                            connCtx->connection = nullptr;
                        }
                    }
                }
            },
            .on_write = [](lsquic_stream_t *stream, lsquic_stream_ctx_t *h) {
                auto *ctx = reinterpret_cast<SenderStreamContext *>(h);
                auto *connCtx = reinterpret_cast<SenderConnectionContext *>(lsquic_conn_get_ctx(
                    lsquic_stream_conn(stream)));

                if (ctx->eofAll) {
                    lsquic_stream_shutdown(stream, 1);
                    lsquic_stream_wantread(connCtx->manifestStream, 1);
                    return;
                }

                if (!ctx->typeByteSent) {
                    uint8_t tag = ctx->isManifestStream ? 0x00 : 0x01;
                    ssize_t nw = lsquic_stream_write(stream, &tag, 1);
                    if (nw > 0) {
                        ctx->typeByteSent = true;
                    } else {
                        return;
                    }
                }


                if (ctx->isManifestStream) {
                    size_t total = senderPersistentContext.manifestBlob.size();
                    size_t sent = connCtx->manifestSent;
                    if (sent < total) {
                        ssize_t nw = lsquic_stream_write(stream, senderPersistentContext.manifestBlob.data() + sent,
                                                         total - sent);
                        if (nw > 0) connCtx->manifestSent += nw;
                    }
                    if (connCtx->manifestSent == total) {
                        lsquic_stream_flush(stream);
                        //send FIN bit
                        lsquic_stream_shutdown(stream, 1);
                        //wait for manifest ack
                        lsquic_stream_wantread(stream, 1);
                    }
                    return;
                }

                while (true) {
                    if (ctx->bufSent >= ctx->bufReady) {
                        if (ctx->fileOffset >= ctx->fileSize) {
                            if (!ctx->advanceFile()) {
                                //wait for receiver ACK
                                lsquic_stream_shutdown(stream, 1);
                                lsquic_stream_wantread(connCtx->manifestStream, 1);
                                return;
                            }
                        }
                        if (!ctx->fillBuf()) {
                            lsquic_stream_close(stream);
                            return;
                        }
                    }

                    const uint8_t *ptr = ctx->readBuf.data() + ctx->bufSent;
                    size_t remaining = ctx->bufReady - ctx->bufSent;

                    ssize_t nw = lsquic_stream_write(stream, ptr, remaining);
                    if (nw <= 0) return;

                    ctx->bufSent += static_cast<size_t>(nw);
                    ctx->fileOffset += static_cast<uint64_t>(nw);

                    connCtx->currentFileOffset = ctx->fileOffset;

                    connCtx->bytesMoved += nw;
                    connCtx->logicalBytesMoved += nw;
                }
            },

            .on_close = [](lsquic_stream_t *stream, lsquic_stream_ctx_t *h) {
                const auto *ctx = reinterpret_cast<SenderStreamContext *>(h);
                if (ctx->pinnedFileId != UINT32_MAX) {
                    senderPersistentContext.cache.release(ctx->pinnedFileId);
                }
                delete ctx;
            },

            .on_hsk_done = [](lsquic_conn_t *connection, enum lsquic_hsk_status status) {
            },

            .on_conncloseframe_received = [](lsquic_conn_t *c, int app_error, uint64_t error_code, const char *reason,
                                             int reason_len) {
                spdlog::error("PEER REJECTED CONNECTION! Code: {}, Reason: {:.{}s}",
                              error_code, reason, reason_len);
            },


        };

    public:
        static void initialize() {
            // common::init_lsquic_logging();
            sslCtx_ = createSslCtx();
            lsquic_global_init(LSQUIC_GLOBAL_CLIENT);
            lsquic_engine_settings settings;
            lsquic_engine_init_settings(&settings, 0);
            settings.es_versions = (1 << LSQVER_I001);
            settings.es_cc_algo = 2;
            settings.es_init_max_data = SenderConfig::quicConnWindowBytes;
            settings.es_init_max_streams_uni = 0;
            settings.es_init_max_streams_bidi = 2;
            settings.es_idle_conn_to = 15000000;
            settings.es_init_max_stream_data_uni = SenderConfig::quicStreamWindowBytes;
            settings.es_init_max_stream_data_bidi_local = SenderConfig::quicStreamWindowBytes;
            settings.es_init_max_stream_data_bidi_remote = SenderConfig::quicStreamWindowBytes;
            settings.es_handshake_to = 30000000;
            settings.es_allow_migration = 0;
            settings.es_pace_packets = 1;
            settings.es_delayed_acks = 0;
            settings.es_max_batch_size = 64;
            settings.es_scid_len = 8;
            settings.es_max_cfcw = SenderConfig::quicConnWindowBytes * 2;
            settings.es_max_sfcw = SenderConfig::quicStreamWindowBytes * 2;
            settings.es_progress_check = 10000;


            char err_buf[256];
            if (0 != lsquic_engine_check_settings(&settings, 0, err_buf, sizeof(err_buf))) {
                spdlog::error("Invalid lsquic engine settings: {}", err_buf);
                return;
            }
            lsquic_engine_api api = {};
            api.ea_settings = &settings;
            api.ea_stream_if = &streamCallbacks;
            api.ea_packets_out = sendPackets;
            api.ea_get_ssl_ctx = getSslCtx;
            engine = lsquic_engine_new(0, &api);

            watchProgress();
            g_timeout_add(0, engineTick, nullptr);
        }


        static void startTransfer(NiceAgent *agent, const guint streamId,
                                  std::string receiverId) {
            setAndVerifySocketBuffers(agent, streamId, 1, SenderConfig::udpBufferBytes);

            NiceCandidate *local = nullptr, *remote = nullptr;
            if (!nice_agent_get_selected_pair(agent, streamId, 1, &local, &remote)) {
                spdlog::error("QUIC connection not ready");
                return;
            }


            auto *ctx = new SenderConnectionContext();
            ctx->agent = agent;
            ctx->streamId = streamId;
            ctx->receiverId = receiverId;
            ctx->progressBarIndex = senderPersistentContext.addNewProgressBar("Receiver ID: " + ctx->receiverId);
            ctx->connectionType = (local->type == NICE_CANDIDATE_TYPE_RELAYED || remote->type ==
                                   NICE_CANDIDATE_TYPE_RELAYED)
                                      ? common::ConnectionContext::RELAYED
                                      : common::ConnectionContext::DIRECT;
            if (ctx->connectionType == common::ConnectionContext::RELAYED) {
                senderPersistentContext.progressBars[ctx->progressBarIndex].set_option(
                    indicators::option::ForegroundColor{indicators::Color::yellow});
            }

            nice_address_copy_to_sockaddr(&local->addr, reinterpret_cast<sockaddr *>(&ctx->localAddr));
            nice_address_copy_to_sockaddr(&remote->addr, reinterpret_cast<sockaddr *>(&ctx->remoteAddr));

            connectionContexts_.push_back(ctx);


            nice_agent_attach_recv(agent, streamId, 1, common::ThreadManager::getContext(),
                                   [](NiceAgent *agent, guint stream_id, guint component_id,
                                      guint len, gchar *buf, gpointer user_data) {
                                       auto *c = static_cast<common::ConnectionContext *>(user_data);

                                       lsquic_engine_packet_in(engine, (unsigned char *) buf, len,
                                                               (sockaddr *) &c->localAddr,
                                                               (sockaddr *) &c->remoteAddr,
                                                               c, 0);

                                       process();
                                   },
                                   ctx
            );


            lsquic_engine_connect(
                engine,
                LSQVER_I001,
                reinterpret_cast<const sockaddr *>(&ctx->localAddr),
                reinterpret_cast<const sockaddr *>(&ctx->remoteAddr),
                ctx,
                nullptr,
                "thruflux.local", 0, nullptr, 0, nullptr, 0
            );
        }


        static void disposeReceiverConnection(std::string_view receiverId) {
            for (const auto *ctx: connectionContexts_) {
                if (((SenderConnectionContext *) ctx)->receiverId == receiverId) {
                    if (ctx->connection) {
                        lsquic_conn_close(ctx->connection);
                        ctx->connection = nullptr;
                    }
                    break;
                }
            }

            process();
        }
    };
}
