#pragma once
#include <lsquic.h>
#include <openssl/base.h>
#include <openssl/ssl.h>
#include <spdlog/spdlog.h>
#include <openssl/x509.h>
#include <openssl/pem.h>

#include "ReceiverConfig.hpp"
#include "ReceiverContexts.hpp"
#include "../common/Contexts.hpp"
#include "../common/Stream.hpp"
#include <llfio/llfio.hpp>

namespace receiver {
    class ReceiverStream : public common::Stream {
        static void watchProgress() {
            g_timeout_add_full(G_PRIORITY_HIGH, 1000, [](gpointer data)-> gboolean {
                if (connectionContexts_.empty()) {
                    return G_SOURCE_CONTINUE;
                }
                auto *receiverConnectionContext = static_cast<ReceiverConnectionContext *>(
                    connectionContexts_[0]);
                if (!receiverConnectionContext || !receiverConnectionContext->started) {
                    return G_SOURCE_CONTINUE;
                }
                if (receiverConnectionContext->complete) {
                    return G_SOURCE_REMOVE;
                }
                const auto now = std::chrono::steady_clock::now();
                if (receiverConnectionContext->lastTime.time_since_epoch().count() == 0) {
                    receiverConnectionContext->lastTime = now;
                    receiverConnectionContext->progressBar->set_option(
                        indicators::option::PostfixText{"starting..."});
                    receiverConnectionContext->progressBar->set_progress(0);
                    return G_SOURCE_CONTINUE;
                }

                std::chrono::duration<double> elapsed = now - receiverConnectionContext->startTime;
                std::chrono::duration<double> delta = now - receiverConnectionContext->lastTime;

                const double elapsedSeconds = elapsed.count();
                const double deltaSeconds = delta.count();

                const double safeDelta = (deltaSeconds > 1e-6) ? deltaSeconds : 1e-6;
                const double safeElapsed = (elapsedSeconds > 1e-6) ? elapsedSeconds : 1e-6;

                const double instantThroughput =
                        (receiverConnectionContext->bytesMoved - receiverConnectionContext->lastBytesMoved) /
                        safeDelta;
                const double averageThroughput = receiverConnectionContext->bytesMoved / safeElapsed;
                const double ewmaThroughput = receiverConnectionContext->ewmaThroughput == 0
                                                  ? instantThroughput
                                                  : 0.2 * instantThroughput + 0.8 * receiverConnectionContext->
                                                    ewmaThroughput;
                receiverConnectionContext->ewmaThroughput = ewmaThroughput;

                const double totalBytes = receiverConnectionContext->totalExpectedBytes;

                const double percent = (totalBytes <= 0.0)
                                           ? 0.0
                                           : (receiverConnectionContext->bytesMoved / totalBytes) * 100.0;
                int p = static_cast<int>(std::lround(percent));
                if (p < 0) p = 0;
                if (p > 100) p = 100;

                std::string postfix;
                postfix.reserve(256);
                postfix += common::Utils::sizeToReadableFormat(ewmaThroughput);
                postfix += "/s received ";
                postfix += common::Utils::sizeToReadableFormat(receiverConnectionContext->bytesMoved);
                postfix += " resumed ";
                postfix += common::Utils::sizeToReadableFormat(receiverConnectionContext->skippedBytes);

                postfix += " files ";

                postfix += std::to_string(receiverConnectionContext->filesMoved);
                postfix += "/";
                postfix += std::to_string(receiverConnectionContext->totalExpectedFilesCount);
                postfix += " ";
                postfix += receiverConnectionContext->connectionType == common::ConnectionContext::RELAYED
                               ? "relayed"
                               : "direct";
                receiverConnectionContext->progressBar->set_option(indicators::option::PostfixText{postfix});
                receiverConnectionContext->progressBar->set_progress(p);;
                receiverConnectionContext->lastTime = now;
                receiverConnectionContext->lastBytesMoved = receiverConnectionContext->bytesMoved;

                receiverConnectionContext->maybeSaveResumeState();
                return G_SOURCE_CONTINUE;
            }, nullptr, nullptr);
        }

        static int alpnSelectCallback(SSL *ssl, const unsigned char **out, unsigned char *outlen,
                                      const unsigned char *in, unsigned int inlen, void *arg) {
            *out = reinterpret_cast<const unsigned char *>("thruflux");
            *outlen = 8;

            return SSL_TLSEXT_ERR_OK;
        }


        static void loadInMemoryCertificate(SSL_CTX *ctx) {
            EVP_PKEY_CTX *pkt = EVP_PKEY_CTX_new_id(EVP_PKEY_RSA, nullptr);
            if (!pkt) throw std::runtime_error("Failed to create keygen ctx");

            if (EVP_PKEY_keygen_init(pkt) <= 0) throw std::runtime_error("Keygen init failed");

            if (EVP_PKEY_CTX_set_rsa_keygen_bits(pkt, 2048) <= 0) throw std::runtime_error("RSA bit set failed");

            EVP_PKEY *pkey = nullptr;
            if (EVP_PKEY_keygen(pkt, &pkey) <= 0) throw std::runtime_error("Key generation failed");

            X509 *x509 = X509_new();
            X509_set_version(x509, 2);
            ASN1_INTEGER_set(X509_get_serialNumber(x509), 1);
            X509_gmtime_adj(X509_get_notBefore(x509), 0);
            X509_gmtime_adj(X509_get_notAfter(x509), 31536000L); // 1 year

            X509_set_pubkey(x509, pkey);
            X509_NAME *name = X509_get_subject_name(x509);
            X509_NAME_add_entry_by_txt(name, "CN", MBSTRING_ASC, (unsigned char *) "thruflux.local", -1, -1, 0);
            X509_set_issuer_name(x509, name);

            if (!X509_sign(x509, pkey, EVP_sha256())) throw std::runtime_error("Signing failed");

            if (SSL_CTX_use_certificate(ctx, x509) <= 0) throw std::runtime_error("Cert use failed");
            if (SSL_CTX_use_PrivateKey(ctx, pkey) <= 0) throw std::runtime_error("Key use failed");

            EVP_PKEY_CTX_free(pkt);
            X509_free(x509);
            EVP_PKEY_free(pkey);
        }


        inline static lsquic_stream_if streamCallbacks = {
            .on_new_conn = [](void *streamIfCtx, lsquic_conn_t *c) -> lsquic_conn_ctx * {
                auto *ctx = static_cast<ReceiverConnectionContext *>(lsquic_conn_get_peer_ctx(c, nullptr));
                ctx->connection = c;
                return reinterpret_cast<lsquic_conn_ctx *>(ctx);
            },
            .on_conn_closed = [](lsquic_conn_t *c) {
                auto *ctx = reinterpret_cast<ReceiverConnectionContext *>(lsquic_conn_get_ctx(c));
                lsquic_conn_set_ctx(c, nullptr);
                if (ctx) {
                    if (ctx->complete) {
                        const auto &progressBar = ctx->progressBar;
                        progressBar->set_option(
                            indicators::option::ForegroundColor{indicators::Color::green});

                        std::string postfix;
                        postfix.reserve(256);
                        postfix += " received ";
                        postfix += common::Utils::sizeToReadableFormat(ctx->bytesMoved);
                        postfix += " resumed ";
                        postfix += common::Utils::sizeToReadableFormat(ctx->skippedBytes);
                        postfix += " files ";
                        postfix += std::to_string(ctx->filesMoved);
                        postfix += "/";
                        postfix += std::to_string(ctx->totalExpectedFilesCount);
                        postfix += " ";
                        postfix += ctx->connectionType == common::ConnectionContext::RELAYED ? "relayed" : "direct";
                        postfix += " [DONE]";
                        progressBar->set_option(indicators::option::PostfixText{postfix});
                        progressBar->set_progress(100);
                        //delete resume state
                        std::error_code ec;
                        std::filesystem::remove(ctx->resumeStatePath, ec);
                    } else {
                        const auto &progressBar = ctx->progressBar;
                        std::string postfix;
                        postfix.reserve(256);
                        postfix += " received ";
                        postfix += common::Utils::sizeToReadableFormat(ctx->bytesMoved);
                        postfix += " resumed ";
                        postfix += common::Utils::sizeToReadableFormat(ctx->skippedBytes);
                        postfix += " files ";
                        postfix += std::to_string(ctx->filesMoved);
                        postfix += "/";
                        postfix += std::to_string(ctx->totalExpectedFilesCount);
                        postfix += " ";
                        postfix += ctx->connectionType == common::ConnectionContext::RELAYED ? "relayed" : "direct";
                        postfix += " [FAILED]";
                        progressBar->set_option(indicators::option::PostfixText(postfix));
                        progressBar->set_option(
                            indicators::option::ForegroundColor{indicators::Color::red});
                        progressBar->mark_as_completed();
                        ctx->maybeSaveResumeState(true);
                    }
                    ctx->connection = nullptr;
                }
                //no need to delete connection context pointer for receiver; to be handled by dispose() function anyways
                common::ThreadManager::terminate();
            },
            .on_new_stream = [](void *stream_if_ctx, lsquic_stream_t *stream) -> lsquic_stream_ctx_t * {
                lsquic_stream_wantread(stream, 1);
                return reinterpret_cast<lsquic_stream_ctx_t *>(new ReceiverStreamContext());
            },
            .on_read = [](lsquic_stream_t *stream, lsquic_stream_ctx_t *h) {
                auto *connCtx = reinterpret_cast<ReceiverConnectionContext *>(lsquic_conn_get_ctx(
                    lsquic_stream_conn(stream)));
                auto *ctx = reinterpret_cast<ReceiverStreamContext *>(h);

                if (ctx->type == ReceiverStreamContext::UNKNOWN) {
                    uint8_t tag;
                    if (lsquic_stream_read(stream, &tag, 1) == 1) {
                        ctx->type = (tag == 0x00)
                                        ? ReceiverStreamContext::MANIFEST
                                        : ReceiverStreamContext::DATA;
                        if (ctx->type == ReceiverStreamContext::DATA && !connCtx->started) {
                            if (!ctx->openFile(connCtx, connCtx->resumeFileId, connCtx->resumeOffset)) {
                                lsquic_stream_close(stream);
                                return;
                            }

                            connCtx->startTime = std::chrono::steady_clock::now();
                            connCtx->progressBar->set_option(
                                indicators::option::PostfixText{"starting..."});
                            connCtx->progressBar->set_progress(0);
                            connCtx->started = true;
                        }
                    } else {
                        return;
                    }
                }

                if (ctx->type == ReceiverStreamContext::MANIFEST) {
                    uint8_t tmp[4096];
                    while (!connCtx->manifestParsed) {
                        const auto nr = lsquic_stream_read(stream, tmp, sizeof(tmp));
                        if (nr > 0) {
                            connCtx->manifestBuf.insert(connCtx->manifestBuf.end(), tmp, tmp + nr);
                            std::string postfix;
                            postfix.reserve(64);
                            postfix += common::Utils::sizeToReadableFormat(
                                static_cast<double>(connCtx->manifestBuf.size()));
                            postfix += " received";
                            connCtx->manifestProgressBar.set_option(indicators::option::PostfixText(postfix));
                            const auto now = std::chrono::steady_clock::now();
                            if (now - connCtx->lastManifestProgressPrint >= std::chrono::milliseconds(250)) {
                                connCtx->manifestProgressBar.print_progress();
                                connCtx->lastManifestProgressPrint = now;
                            }
                        } else if (nr == 0) {
                            std::string postfix;
                            postfix.reserve(64);
                            postfix += common::Utils::sizeToReadableFormat((double) connCtx->manifestBuf.size());
                            postfix += " received";
                            connCtx->manifestProgressBar.set_option(indicators::option::PostfixText(postfix));
                            connCtx->manifestProgressBar.mark_as_completed();

                            connCtx->parseManifest();
                            connCtx->manifestParsed = true;
                            connCtx->pendingManifestAck = true;
                            //must write ACK
                            lsquic_stream_wantwrite(stream, 1);
                            //no reading
                            lsquic_stream_wantread(stream, 0);
                            break;
                        } else {
                            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                                break;
                            }
                            spdlog::error("Unexpected error while reading manifest stream: error code={}", errno);
                            break;
                        }
                    }
                    return;
                }

                while (true) {
                    if (!ctx->pinnedHandle) {
                        lsquic_stream_close(stream);
                        return;
                    }

                    while (ctx->flushOff >= ctx->curSize) {
                        if (!ctx->flushStage(connCtx)) {
                            lsquic_stream_close(stream);
                            return;
                        }

                        connCtx->filesMoved++;
                        ctx->curFileId++;

                        if (connCtx->filesMoved >= connCtx->totalExpectedFilesCount) {
                            connCtx->complete = true;
                            connCtx->pendingCompleteAck = true;
                            lsquic_stream_wantwrite(connCtx->manifestStream, 1);
                            lsquic_stream_wantread(stream, 0);
                            return;
                        }

                        if (!ctx->openFile(connCtx, ctx->curFileId, 0)) {
                            lsquic_stream_close(stream);
                            return;
                        }
                    }

                    const size_t stageRoom = ctx->stage.size() - ctx->stageLen;
                    if (stageRoom == 0) {
                        if (!ctx->flushStage(connCtx)) {
                            lsquic_stream_close(stream);
                            return;
                        }
                        continue;
                    }

                    const uint64_t remaining = (ctx->recvOff < ctx->curSize) ? (ctx->curSize - ctx->recvOff) : 0;
                    if (remaining == 0) {
                        if (!ctx->flushStage(connCtx)) {
                            lsquic_stream_close(stream);
                            return;
                        }
                        continue;
                    }

                    const size_t maxRead = std::min<uint64_t>(stageRoom, remaining);
                    const ssize_t nr = lsquic_stream_read(stream, ctx->stage.data() + ctx->stageLen, maxRead);
                    if (nr <= 0) break;

                    ctx->stageLen += nr;
                    ctx->recvOff += nr;

                    if (ctx->stageLen >= FLUSH_AT || ctx->recvOff >= ctx->curSize) {
                        if (!ctx->flushStage(connCtx)) {
                            lsquic_stream_close(stream);
                            return;
                        }
                    }
                }
            },
            .on_write = [](lsquic_stream_t *stream, lsquic_stream_ctx_t *h) {
                auto *connCtx = reinterpret_cast<ReceiverConnectionContext *>(lsquic_conn_get_ctx(
                    lsquic_stream_conn(stream)));
                auto *ctx = reinterpret_cast<ReceiverStreamContext *>(h);

                if (ctx->type == ReceiverStreamContext::MANIFEST) {
                    if (connCtx->pendingManifestAck) {
                        uint8_t ackbuf[1 + 4 + 8];
                        ackbuf[0] = common::RECEIVER_MANIFEST_RECEIVED_ACK;
                        memcpy(ackbuf + 1, &connCtx->resumeFileId, 4);
                        memcpy(ackbuf + 5, &connCtx->resumeOffset, 8);

                        const size_t total = sizeof(ackbuf);
                        const size_t sent = connCtx->manifestAckSent;
                        const ssize_t nw = lsquic_stream_write(stream, ackbuf + sent, total - sent);
                        if (nw > 0) connCtx->manifestAckSent += nw;

                        if (connCtx->manifestAckSent >= total) {
                            lsquic_stream_flush(stream);
                            connCtx->pendingManifestAck = false;
                            connCtx->manifestStream = stream;
                            lsquic_stream_wantwrite(stream, 0);
                        }
                    } else if (connCtx->pendingCompleteAck) {
                        uint8_t ack = common::RECEIVER_TRANSFER_COMPLETE_ACK;
                        const auto nw = lsquic_stream_write(stream, &ack, 1);
                        if (nw == 1) {
                            lsquic_stream_flush(stream);
                            connCtx->pendingCompleteAck = false;
                            lsquic_stream_wantwrite(stream, 0);
                        }
                    }
                }
            },
            .on_close = [](lsquic_stream_t *stream, lsquic_stream_ctx_t *h) {
                auto *ctx = reinterpret_cast<ReceiverStreamContext *>(h);
                auto *connCtx = reinterpret_cast<ReceiverConnectionContext *>(lsquic_conn_get_ctx(
                    lsquic_stream_conn(stream)));

                if (connCtx && !connCtx->complete) {
                    (void) ctx->flushStage(connCtx);
                }
                if (connCtx && ctx->pinnedFileId != UINT32_MAX) {
                    connCtx->cache.release(ctx->pinnedFileId);
                    ctx->pinnedFileId = UINT32_MAX;
                    ctx->pinnedHandle = nullptr;
                }
                delete ctx;
            },
            .on_hsk_done = [](lsquic_conn_t *c, enum lsquic_hsk_status status) {
                if (status == LSQ_HSK_OK || status == LSQ_HSK_RESUMED_OK) {
                    spdlog::info("QUIC Handshake Successful");
                }
            }
        };

    public:
        static void initialize() {
            // common::init_lsquic_logging();
            sslCtx_ = createSslCtx();
            SSL_CTX_set_alpn_select_cb(sslCtx_, alpnSelectCallback, nullptr);
            loadInMemoryCertificate(sslCtx_);
            lsquic_global_init(LSQUIC_GLOBAL_SERVER);
            lsquic_engine_settings settings;
            lsquic_engine_init_settings(&settings, LSENG_SERVER);
            settings.es_versions = (1 << LSQVER_I001);
            settings.es_cc_algo = 2;
            settings.es_init_max_data = ReceiverConfig::quicConnWindowBytes;
            settings.es_init_max_streams_uni = 0;
            settings.es_init_max_streams_bidi = 2;
            settings.es_idle_conn_to = 30000000;
            settings.es_init_max_stream_data_uni = ReceiverConfig::quicStreamWindowBytes;
            settings.es_init_max_stream_data_bidi_local = ReceiverConfig::quicStreamWindowBytes;
            settings.es_init_max_stream_data_bidi_remote = ReceiverConfig::quicStreamWindowBytes;
            settings.es_handshake_to = 16777215;
            settings.es_allow_migration = 0;
            settings.es_pace_packets = 1;
            settings.es_delayed_acks = 0;
            settings.es_max_batch_size = 64;
            settings.es_scid_len = 8;
            settings.es_max_cfcw = ReceiverConfig::quicConnWindowBytes * 2;
            settings.es_max_sfcw = ReceiverConfig::quicStreamWindowBytes * 2;
            settings.es_progress_check = 10000;


            char err_buf[256];
            if (0 != lsquic_engine_check_settings(&settings, LSENG_SERVER, err_buf, sizeof(err_buf))) {
                spdlog::error("Invalid lsquic engine settings: {}", err_buf);
                return;
            }
            lsquic_engine_api api = {};
            api.ea_settings = &settings;
            api.ea_stream_if = &streamCallbacks;
            api.ea_packets_out = sendPackets;
            api.ea_get_ssl_ctx = getSslCtx;
            engine = lsquic_engine_new(LSENG_SERVER, &api);
            watchProgress();
        }


        static void receiveTransfer(NiceAgent *agent, const guint streamId) {
            spdlog::info("Saving to {}", ReceiverConfig::out);

            setAndVerifySocketBuffers(agent, streamId, 1, ReceiverConfig::udpBufferBytes);
            NiceCandidate *local = nullptr, *remote = nullptr;
            if (!nice_agent_get_selected_pair(agent, streamId, 1, &local, &remote)) {
                spdlog::error("ICE not ready for QUIC connection");
                return;
            }

            auto *ctx = new ReceiverConnectionContext();
            ctx->agent = agent;
            ctx->streamId = streamId;
            ctx->createProgressBar("Receiving ");
            ctx->connectionType = (local->type == NICE_CANDIDATE_TYPE_RELAYED || remote->type ==
                                   NICE_CANDIDATE_TYPE_RELAYED)
                                      ? common::ConnectionContext::RELAYED
                                      : common::ConnectionContext::DIRECT;
            if (ctx->connectionType == common::ConnectionContext::RELAYED) {
                ctx->progressBar->set_option(indicators::option::ForegroundColor{indicators::Color::yellow});
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

            g_timeout_add(0, engineTick, nullptr);
        }
    };
};
