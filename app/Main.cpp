
#include "../server/ServerEntryPoint.hpp"
#include "../sender/SenderEntryPoint.hpp"
#include "../receiver/ReceiverEntryPoint.hpp"
#include <clocale>

#ifdef _WIN32
    #include <windows.h>
    #define SET_ENV(name, value) _putenv_s(name, value)
#else
    #define SET_ENV(name, value) setenv(name, value, 1)
#endif

static void forceUtf8Locale() {
    if (std::setlocale(LC_ALL, "") != nullptr) return;

#ifdef _WIN32
    if (std::setlocale(LC_ALL, ".UTF-8") != nullptr) {
        SET_ENV("LC_ALL", ".UTF-8");
        return;
    }
#else
    if (std::setlocale(LC_ALL, "C.UTF-8") != nullptr) {
        SET_ENV("LC_ALL", "C.UTF-8");
        SET_ENV("LANG", "C.UTF-8");
        return;
    }
    if (std::setlocale(LC_ALL, "en_US.UTF-8") != nullptr) {
        SET_ENV("LC_ALL", "en_US.UTF-8");
        SET_ENV("LANG", "en_US.UTF-8");
        return;
    }
#endif
    std::setlocale(LC_ALL, "C");
    SET_ENV("LC_ALL", "C");
}

int main(const int argc, char **argv) {
    forceUtf8Locale();
    CLI::App app{"Thruflux"};
    app.require_subcommand(1);
    CLI::App* host = app.add_subcommand("host", "Share files with other multiple receivers");
    CLI::App* join = app.add_subcommand("join", "Receive files from a host");
    CLI::App* server = app.add_subcommand("server", "Start a thruflux signaling server");
    server::ServerConfig::initialize(server);
    sender::SenderConfig::initialize(host);
    receiver::ReceiverConfig::initialize(join);

    try {
        app.parse(argc, argv);
    } catch (const CLI::ParseError &e) {
        return app.exit(e);
    }

    if (app.got_subcommand(host)) {
        sender::run(argc, argv);
    }
    else if (app.got_subcommand(join)) {
        receiver::run(argc, argv);
    }
    else if (app.got_subcommand(server)) {
        server::run(argc, argv);
    }

    return 0;
}
