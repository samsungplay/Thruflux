#pragma once
#include <httplib.h>
#include <boost/asio/io_context.hpp>
#include <CLI/App.hpp>
#include <nlohmann/json.hpp>

#include "UIData.hpp"
#include "../common/ThreadManager.hpp"
#include "../sender/SenderConfig.hpp"

//This is not an ACTUAL UI, but entrypoint to a local web interface to facilitate communication between UI and the Engine

namespace ui {
    inline void run() {
        boost::asio::io_context ioContext;
        auto workGuard = boost::asio::make_work_guard(ioContext);

        std::thread engineThread([&ioContext]() {
            ioContext.run();
        });

        httplib::Server server;

        server.set_read_timeout(30, 0);
        server.set_write_timeout(30, 0);

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
                return sink.write(msg.data(), msg.size());
            });
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

                for (auto &[key, value]: config.items()) {
                    if (value.is_array()) {
                        for (auto &element: value) {
                            args.push_back("--" + key);
                            args.push_back(element.get<std::string>());
                        }
                    } else {
                        args.push_back("--" + key);
                        if (value.is_string()) {
                            args.push_back(value.get<std::string>());
                        } else {
                            args.push_back(value.dump());
                        }
                    }
                }

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


                for (auto &[key, value]: config.items()) {
                    if (value.is_array()) {
                        for (auto &element: value) {
                            args.push_back("--" + key);
                            args.push_back(element.get<std::string>());
                        }
                    } else {
                        args.push_back("--" + key);
                        if (value.is_string()) {
                            args.push_back(value.get<std::string>());
                        } else {
                            args.push_back(value.dump());
                        }
                    }
                }

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

        const std::vector ports = {48480, 48481, 48482, 48483, 48484, 48485, 48486, 48487};
        int successfulPort = -1;

        for (const auto port: ports) {
            if (server.bind_to_port("localhost", port)) {
                successfulPort = port;
                break;
            }
        }

        if (successfulPort != -1) {
            spdlog::info("Running local web interface on port {}", successfulPort);
            server.listen_after_bind();
        } else {
            spdlog::error(
                "Could not start local web interface as all ports are currently in use. (port 48480 ~ 48487)");
        }

        workGuard.reset();
        engineThread.join();
    }
}
