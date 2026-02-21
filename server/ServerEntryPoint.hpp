#pragma once
#include <uwebsockets/App.h>

#include "ServerConfig.hpp"
#include "ServerSocketHandler.hpp"
#include "TransferSessionStore.hpp"
#include "../common/Types.hpp"

namespace server {
    inline int run(const int argc, char **argv) {
        server::TransferSessionStore::instance();

        auto *loop = reinterpret_cast<struct us_loop_t *>(uWS::Loop::get());

        auto wsConnectionRateLimiter = common::TokenBucket(server::ServerConfig::wsConnectionsPerMin / 60.0, server::ServerConfig::wsConnectionsBurst);
        auto wsMessageRateLimiter = common::TokenBucket(server::ServerConfig::wsMessagesPerSec, server::ServerConfig::wsMessagesBurst);
        std::atomic<int> wsConnections{0};

        us_timer_t *timer = us_create_timer(loop, 0, 0);
        us_timer_set(timer, [](us_timer_t *t) {
            server::TransferSessionStore::instance().cleanExpiredSessions();
        }, 5000, 5000);

        uWS::App().ws<common::SocketUserData>("/ws", {
                                                  .maxPayloadLength = server::ServerConfig::maxMessageBytes,
                                                  .idleTimeout = server::ServerConfig::wsIdleTimeout,
                                                  .upgrade = [&wsConnectionRateLimiter](auto *res, auto *req, auto *context) {

                                                      if (!wsConnectionRateLimiter.allow()) {
                                                          res->writeStatus("429 Too Many Websocket Connection Attempts - Please try again later.")
                                                          ->writeHeader("Retry-After", "60")
                                                          ->end("Too many websocket connection attempts are ongoing, please try again later.");
                                                          return;
                                                      }

                                                      auto role = std::string(req->getHeader("x-role"));
                                                      auto id = std::string(req->getHeader("x-id"));

                                                      res->template upgrade<common::SocketUserData>(
                                                          common::SocketUserData{
                                                              .id = std::move(id),
                                                              .role = std::move(role)
                                                          },
                                                          req->getHeader("sec-websocket-key"),
                                                          req->getHeader("sec-websocket-protocol"),
                                                          req->getHeader("sec-websocket-extensions"),
                                                          context
                                                      );
                                                  },
                                                  .open = [&wsConnections](auto *ws) {
                                                      const auto current = wsConnections.fetch_add(1) + 1;
                                                      if (current > server::ServerConfig::maxWsConnections) {
                                                          ws->end(4000,"Server reached max number of concurrent websocket connections. Please try again later.");
                                                          return;
                                                      }
                                                      server::ServerSocketHandler::onConnect(ws);
                                                  },
                                                  .message = [&wsMessageRateLimiter](auto *ws, std::string_view message, uWS::OpCode op) {
                                                      if (op != uWS::OpCode::TEXT) return;
                                                      if (!wsMessageRateLimiter.allow()) {
                                                          ws->end(4000,"Server reached max number of websocket messages per second. Please try again later.");
                                                          return;
                                                      }
                                                      server::ServerSocketHandler::onMessage(ws, message);
                                                  },
                                                  .close = [&wsConnections](auto *ws, int code, std::string_view message) {
                                                      wsConnections.fetch_sub(1);
                                                      server::ServerSocketHandler::onClose(ws, message);
                                                  }
                                              })
                .listen(server::ServerConfig::port, [](const us_listen_socket_t *socket) {
                    if (socket) {
                        spdlog::info("Server successfully started on port {}", server::ServerConfig::port);
                    } else {
                        spdlog::info("Server failed to start on port {}", server::ServerConfig::port);
                    }
                })
                .run();
        return 0;
    }
}