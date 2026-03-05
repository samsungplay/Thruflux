#pragma once
#include <httplib.h>
#include <boost/asio/io_context.hpp>
#include <CLI/App.hpp>
#include <nlohmann/json.hpp>

#include "UIConfig.hpp"
#include "UIData.hpp"
#include "../common/ThreadManager.hpp"
#include "../sender/SenderConfig.hpp"

//This is not an ACTUAL UI, but entrypoint to a local web interface to facilitate communication between UI and the Engine

namespace ui {
    std::string vec_to_string(const std::vector<std::string> &vec) {
        size_t total_size = 0;
        for (const auto &s: vec) total_size += s.size();

        std::string result;
        result.reserve(total_size);
        for (const auto &s: vec) result += s + ",";
        return result;
    }

    inline void run() {
        boost::asio::io_context ioContext;
        auto workGuard = boost::asio::make_work_guard(ioContext);

        std::thread engineThread([&ioContext]() {
            ioContext.run();
        });

        //ui health check
        if (UIConfig::uiHeartBeatPort > 0) {
            std::thread uiHealthCheckThread([]() {
                httplib::Client cli{"127.0.0.1",UIConfig::uiHeartBeatPort};
                cli.set_connection_timeout(30);
                cli.set_read_timeout(30);
                cli.set_write_timeout(30);
                while (true) {
                    auto res = cli.Get("/health");
                    if (res == nullptr || !res || res->status != 200) {
                        spdlog::error("Failed to health check attached UI. Terminating to prevent orphan process...");
                        std::this_thread::sleep_for(std::chrono::seconds(3));
                        std::exit(1);
                    }
                    std::this_thread::sleep_for(std::chrono::seconds(30));
                }
            });
            uiHealthCheckThread.detach();
        }

        httplib::Server server;

        server.set_read_timeout(0, 0);
        server.set_write_timeout(0, 0);
        server.set_keep_alive_timeout(0);

        server.Get("/health", [](auto &req, auto &res) {
            res.status = 200;
        });
        server.Get("/events", [](const httplib::Request &req, httplib::Response &res) {
            res.set_header("Content-Type", "text/event-stream");
            res.set_header("Cache-Control", "no-cache");
            res.set_header("Connection", "keep-alive");

            res.set_chunked_content_provider("text/event-stream", [](size_t offset, httplib::DataSink &sink) {
                std::string msg;
                {
                    std::unique_lock lock(eventStream.mutex);
                    eventStream.cv.wait(lock, [] { return !eventStream.messageQueue.empty(); });
                    msg = std::move(eventStream.messageQueue.front());
                    eventStream.messageQueue.pop();
                }
                const std::string formatted = "data: " + msg + "\n\n";
                return sink.write(formatted.data(), formatted.size());
            });
        });

        server.Post("/abortReceiver", [](const httplib::Request &req, httplib::Response &res) {
            //abort a single receiver..
            if (!common::ThreadManager::isBusy() || !common::ThreadManager::isRunningSender.load()) {
                res.status = 503;
                return;
            }
            const auto payload = nlohmann::json::parse(req.body);
            if (!payload.contains("receiverId")) {
                res.status = 400;
                return;
            }
            const auto receiverId = payload["receiverId"].get<std::string>();

            auto promisePtr = std::make_shared<std::promise<void> >();
            auto f = promisePtr->get_future();

            common::ThreadManager::postTask([receiverId, promisePtr]() {
                sender::SenderStream::disposeReceiverConnection(receiverId);
                promisePtr->set_value();
            });

            if (f.wait_for(std::chrono::seconds(10)) != std::future_status::ready) {
                res.status = 504;
                return;
            }

            res.status = 200;
        });

        server.Post("/abort", [](const httplib::Request &req, httplib::Response &res) {
            common::ThreadManager::terminate();

            int retryCnt = 0;
            while (common::ThreadManager::isBusy() && retryCnt < 100) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                retryCnt++;
            }

            res.status = common::ThreadManager::isBusy() ? 504 : 200;
        });

        server.Post("/host", [&ioContext](const httplib::Request &req, httplib::Response &res) {
            try {
                if (common::ThreadManager::isBusy()) {
                    res.status = 503;
                    return;
                }
                const auto config = nlohmann::json::parse(req.body);
                CLI::App app{"JSON Payload Validator"};
                sender::SenderConfig::initialize(&app);
                std::vector<std::string> args;
                std::vector<std::string> pathArgs;

                for (auto &[key, value]: config.items()) {
                    if (key == "paths") {
                        if (value.is_array()) {
                            for (auto &element: value) pathArgs.push_back(element.get<std::string>());
                        } else {
                            pathArgs.push_back(value.get<std::string>());
                        }
                    } else {
                        if (value.is_boolean()) {
                            if (value.get<bool>())
                                args.push_back("--" + key);
                        } else if (value.is_string() && value.get<std::string>().empty()) {
                        } else {
                            args.push_back("--" + key);
                            args.push_back(value.is_string() ? value.get<std::string>() : value.dump());
                        }
                    }
                }


                args.push_back("--");
                args.insert(args.end(), pathArgs.begin(), pathArgs.end());

                std::reverse(args.begin(), args.end());
                app.parse(args);

                boost::asio::post(ioContext, []() {
                    sender::run();
                });

                res.status = 200;
            } catch (const CLI::ParseError &e) {
                res.status = 400;
                res.set_content(nlohmann::json{{"error", e.what()}}.dump(), "application/json");
            } catch (const std::exception &e) {
                res.status = 500;
                res.set_content(nlohmann::json{{"error", e.what()}}.dump(), "application/json");
            }
        });

        server.Post("/receive", [&ioContext](const httplib::Request &req, httplib::Response &res) {
            try {
                if (common::ThreadManager::isBusy()) {
                    res.status = 503;
                    return;
                }
                const auto config = nlohmann::json::parse(req.body);
                CLI::App app{"JSON Payload Validator"};
                receiver::ReceiverConfig::initialize(&app);
                std::vector<std::string> args;
                std::vector<std::string> pathArgs;
                for (auto &[key, value]: config.items()) {
                    if (key == "joinCode") {
                        pathArgs.push_back(value.get<std::string>());
                    } else {
                        if (value.is_boolean()) {
                            if (value.get<bool>())
                                args.push_back("--" + key);
                        } else if (value.is_string() && value.get<std::string>().empty()) {
                        } else {
                            args.push_back("--" + key);
                            args.push_back(value.is_string() ? value.get<std::string>() : value.dump());
                        }
                    }
                }

                args.push_back("--");
                args.insert(args.end(), pathArgs.begin(), pathArgs.end());

                std::reverse(args.begin(), args.end());
                app.parse(args);


                boost::asio::post(ioContext, []() {
                    receiver::run();
                });

                res.status = 200;
            } catch (const CLI::ParseError &e) {
                res.status = 400;
                res.set_content(nlohmann::json{{"error", e.what()}}.dump(), "application/json");
            } catch (const std::exception &e) {
                res.status = 500;
                res.set_content(nlohmann::json{{"error", e.what()}}.dump(), "application/json");
            }
        });


        if (UIConfig::port == 0) {
            int port = server.bind_to_any_port("localhost");
            if (port != -1) {
                spdlog::info("Running local web interface on port {}", port);
                isEnabled.store(true, std::memory_order::relaxed);
                server.listen_after_bind();
            } else {
                spdlog::error("No available port found to start the local web interface.");
            }
        } else {
            if (!server.bind_to_port("localhost", UIConfig::port)) {
                spdlog::error(
                    "Could not start local web interface on specified port {}", UIConfig::port);
            } else {
                spdlog::info("Running local web interface on port {}", UIConfig::port);
                isEnabled.store(true, std::memory_order::relaxed);
                server.listen_after_bind();
            }
        }


        workGuard.reset();
        engineThread.join();
    }
}
