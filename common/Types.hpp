#pragma once
#include <string>
#include <nlohmann/json.hpp>
#include <uwebsockets/App.h>

namespace common {
    struct SocketUserData {
        std::string id;
        std::string role;
        bool sessionCreationAttempted = false;
    };
    using Session = uWS::WebSocket<false, true, SocketUserData>;

    struct StunServer {
        std::string host;
        int port = 0;
    };

    struct TurnServer {
        std::string host;
        int port = 0;
        std::string username;
        std::string password;
    };

    struct CandidatesResult {
        std::string ufrag;
        std::string password;
        nlohmann::json serializedCandidates;
    };

}
