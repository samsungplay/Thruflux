#pragma once

#include <CLI/App.hpp>
#include <CLI/Validators.hpp>
#include <spdlog/spdlog.h>
#include <string>
#include <vector>

namespace sender {
    class SenderConfig {
    public:
        inline static std::vector<std::string> paths;

        inline static std::string serverUrl = "wss://bytepipe.app/ws";
        inline static int maxReceivers = 10;

        inline static std::string stunServer = "stun://stun.cloudflare.com:3478";
        inline static std::string turnServers;
        inline static bool forceTurn = false;

        inline static std::int64_t quicStreamWindowBytes = 32LL * 1024 * 1024;
        inline static std::int64_t quicConnWindowBytes = 256LL * 1024 * 1024;
        inline static int udpBufferBytes = 8 * 1024 * 1024;

        static void initialize(CLI::App* app) {

            const auto isWsUrl = CLI::Validator(
                [](const std::string &s) -> std::string {
                    if (s.rfind("ws://", 0) == 0 || s.rfind("wss://", 0) == 0) return {};
                    return "must start with ws:// or wss://";
                },
                "WEBSOCKET_URL"
            );

            const auto isStunUrl = CLI::Validator(
                [](const std::string &s) -> std::string {
                    if (s.empty()) return "cannot be empty";
                    const bool ok =
                            (s.rfind("stun://", 0) == 0);
                    if (!ok) return "must start with stun://";

                    return {};
                },
                "STUN_URL"
            );

            const auto isTurnUrl = CLI::Validator(
                [](const std::string &s) -> std::string {
                    if (s.empty()) return {};

                    const bool ok =
                            (s.rfind("turn://", 0) == 0);
                    if (!ok) return "must start with turn://";

                    return {};
                },
                "TURN_URL"
            );


            constexpr std::int64_t KiB = 1024;
            constexpr std::int64_t MiB = 1024 * KiB;
            constexpr std::int64_t GiB = 1024 * MiB;

            app->add_option("PATHS", paths, "File(s) or directory(ies) to transfer")
                    ->required()
                    ->check(CLI::ExistingPath);

            app->add_option("--server-url", serverUrl, "HTTP(S) URL of signaling server")
                    ->check(isWsUrl)
                    ->capture_default_str();

            app->add_option("--max-receivers", maxReceivers, "Max concurrent receivers")
                    ->check(CLI::Range(1, 1000))
                    ->capture_default_str();

            app->add_option("--stun-server", stunServer, "STUN server URL")
                    ->check(isStunUrl)
                    ->capture_default_str();

            app->add_option("--turn-server", turnServers,
                           "TURN server URL (optional). Example: turn://user:pass@turn.example.com:3478")
                    ->check(isTurnUrl);

            app->add_flag("--force-turn", forceTurn, "Force TURN relay");

            app->add_option("--quic-stream-window-bytes", quicStreamWindowBytes,
                           "Initial QUIC stream flow-control window (bytes)")
                    ->check(CLI::Range(256 * KiB, 2 * GiB))
                    ->capture_default_str();

            app->add_option("--quic-conn-window-bytes", quicConnWindowBytes,
                           "Initial QUIC connection flow-control window (bytes)")
                    ->check(CLI::Range(1 * MiB, 8 * GiB))
                    ->capture_default_str();


            app->add_option("--udp-buffer-bytes", udpBufferBytes, "UDP socket buffer size (bytes). You must raise the max on your OS too. Default installer should have raised it to 16 MiB.")
                    ->check(CLI::Range(256 * 1024, 256 * 1024 * 1024))
                    ->capture_default_str();

            app->set_version_flag("--version", "Thruflux v0.3.0");

            app->parse_complete_callback([&]() {

                if (quicConnWindowBytes < quicStreamWindowBytes) {
                    throw CLI::ValidationError("--quic-conn-window-bytes",
                                               "must be >= --quic-stream-window-bytes");
                }

                if (udpBufferBytes < 1024 * 1024) {
                    spdlog::warn("udp-buffer-bytes is < 1MiB; this may limit throughput");
                }
            });

        }
    };
}
