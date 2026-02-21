#pragma once

#include <CLI/App.hpp>
#include <CLI/Validators.hpp>
#include <string>
#include <spdlog/spdlog.h>

namespace server {
    class ServerConfig {
    public:
        inline static int port = 8080;
        inline static int maxSessions = 1000;
        inline static int maxReceiversPerSender = 10;
        inline static unsigned int maxMessageBytes = 65536;

        inline static int wsConnectionsPerMin = 30;
        inline static int wsConnectionsBurst = 10;
        inline static int wsMessagesPerSec = 50;
        inline static int wsMessagesBurst = 100;

        inline static int maxWsConnections = 2000;
        inline static unsigned short wsIdleTimeout = 600;
        inline static int sessionTimeout = 86400;

        inline static std::string turnServer;
        inline static std::string turnStaticAuthSecret;
        inline static int turnStaticCredTtl = 600;

        static void initialize(CLI::App* app) {



            app->set_version_flag("--version", "Thruflux Server v0.3.0");

            app->add_option("--port", port, "Port to run the server on.")
                    ->capture_default_str()
                    ->check(CLI::Range(1, 65535));

            app->add_option("--max-sessions", maxSessions, "Max number of concurrent transfer sessions.")
                    ->capture_default_str()
                    ->check(CLI::Range(1, 10'000'000));

            app->add_option(
                        "--max-receivers-per-sender",
                        maxReceiversPerSender,
                        "Max receivers allowed per sender in a single transfer session.")
                    ->capture_default_str()
                    ->check(CLI::Range(1, 1'000'000));

            app->add_option(
                        "--max-message-bytes",
                        maxMessageBytes,
                        "Max websocket message size (bytes).")
                    ->capture_default_str()
                    ->check(CLI::Range(65'536u, 256u * 1024u * 1024u));

            app->add_option(
                        "--ws-connections-per-min",
                        wsConnectionsPerMin,
                        "New websocket connections allowed per minute.")
                    ->capture_default_str()
                    ->check(CLI::Range(0, 10'000'000));

            app->add_option(
                        "--ws-connections-burst",
                        wsConnectionsBurst,
                        "Burst capacity for new websocket connections.")
                    ->capture_default_str()
                    ->check(CLI::Range(0, 10'000'000));

            app->add_option(
                        "--ws-messages-per-sec",
                        wsMessagesPerSec,
                        "Websocket messages allowed per second per process.")
                    ->capture_default_str()
                    ->check(CLI::Range(0, 10'000'000));

            app->add_option(
                        "--ws-messages-burst",
                        wsMessagesBurst,
                        "Burst capacity for websocket messages.")
                    ->capture_default_str()
                    ->check(CLI::Range(0, 10'000'000));

            app->add_option(
                        "--max-ws-connections",
                        maxWsConnections,
                        "Max concurrent websocket connections (senders + receivers).")
                    ->capture_default_str()
                    ->check(CLI::Range(1, 100'000'000));

            app->add_option(
                        "--ws-idle-timeout",
                        wsIdleTimeout,
                        "Websocket idle timeout (seconds).")
                    ->capture_default_str()
                    ->check(CLI::Range(
                        static_cast<unsigned short>(0),
                        static_cast<unsigned short>(24 * 60 * 60)));

            app->add_option(
                        "--session-timeout",
                        sessionTimeout,
                        "Transfer session lifetime (seconds). Expired sessions are destroyed.")
                    ->capture_default_str()
                    ->check(CLI::Range(1, 365 * 24 * 60 * 60));

            auto isTurnUrl = CLI::Validator(
                [](const std::string &s) -> std::string {
                    if (s.empty()) return {};
                    const bool ok =
                            (s.rfind("turn://", 0) == 0);
                    if (!ok) return "must start with turn://";
                    return {};
                },
                "TURN_URL"
            );

            app->add_option(
                        "--turn-server",
                        turnServer,
                        "TURN server URL to provide to clients (REST-based TURN).")
                    ->check(isTurnUrl);

            app->add_option(
                "--turn-static-auth-secret",
                turnStaticAuthSecret,
                "TURN static auth secret (REST-based TURN).");

            app->add_option(
                        "--turn-static-cred-ttl",
                        turnStaticCredTtl,
                        "TURN REST credentials TTL (seconds).")
                    ->capture_default_str()
                    ->check(CLI::Range(1, 7 * 24 * 60 * 60));

            app->parse_complete_callback([&]() {
                const bool hasTurnServer = !turnServer.empty();
                const bool hasTurnSecret = !turnStaticAuthSecret.empty();

                if (hasTurnServer || hasTurnSecret) {
                    if (!hasTurnServer || !hasTurnSecret) {
                        throw CLI::ValidationError(
                            "--turn-server/--turn-static-auth-secret",
                            "REST-based TURN requires both options to be set");
                    }
                }

                if (wsConnectionsPerMin == 0 && wsConnectionsBurst != 0) {
                    throw CLI::ValidationError(
                        "--ws-connections-burst",
                        "must be 0 when --ws-connections-per-min is 0");
                }
                if (wsMessagesPerSec == 0 && wsMessagesBurst != 0) {
                    throw CLI::ValidationError(
                        "--ws-messages-burst",
                        "must be 0 when --ws-messages-per-sec is 0");
                }
            });

        }
    };
}
