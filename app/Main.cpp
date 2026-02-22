
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

int runApp(const int argc, char **argv) {
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

#ifdef _WIN32

static std::string wideToUtf8(const wchar_t* w) {
    if (!w) return {};
    int n = WideCharToMultiByte(CP_UTF8, 0, w, -1, nullptr, 0, nullptr, nullptr);
    if (n <= 0) return {};
    std::string s(n - 1, '\0');
    WideCharToMultiByte(CP_UTF8, 0, w, -1, s.data(), n, nullptr, nullptr);
    return s;
}

int wmain(const int argc, wchar_t** wargv) {
    std::vector<std::string> args;
    args.reserve(argc);
    for (int i = 0; i < argc; ++i) {
        args.push_back(wideToUtf8(wargv[i]));
    }
    std::vector<char*> argv8;
    argv8.reserve(argc);
    for (auto& s : args) {
        argv8.push_back(s.data());
    }

    return runApp(argc, argv8.data());
}

#else

int main(const int argc, char** argv) {
    return runApp(argc,argv);
}

#endif


