#pragma once
#include <CLI/App.hpp>

namespace ui {
    class UIConfig {
    public:
        inline static int port = 0;
        inline static int uiHeartBeatPort = -1;

        static void initialize(CLI::App *app) {
            app->add_option("--port", port, "Port to open the local webinterface at. Value of 0 delegates to the OS to assign a random port.")->capture_default_str();
            app->add_option("--ui-heartbeat-port", uiHeartBeatPort,
                            "Port to periodically check the life of attached UI app. "
                            "The UI app must expose /health endpoint on this port at localhost which simply returns 200 HTTP status OK. "
                            "Used to auto-kill the process itself if associated UI app dies thereby preventing orphan processes. Set to -1 to disable (no UI app attached)")
                    ->capture_default_str();
        }
    };
}
