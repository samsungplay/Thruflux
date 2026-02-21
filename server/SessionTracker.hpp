#pragma once
#include <unordered_map>
#include <string>

#include "../common/Types.hpp"


namespace server {
    static std::unordered_map<std::string, common::Session*> sessionTracker;
}
