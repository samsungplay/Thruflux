#pragma once
#include <boost/asio/thread_pool.hpp>
#include <gio/gnetworking.h>
#include <glib/gmain.h>

namespace common {
    class ThreadManager {
        inline static GMainContext *context_;
        inline static GMainLoop *mainLoop_;

    public:
        //run some task on the main thread
        static void postTask(std::function<void()> task) {

            if (context_ == nullptr || mainLoop_ == nullptr) {
                return;
            }

            auto *taskPtr = new std::function(std::move(task));

            g_main_context_invoke_full(
                context_,
                G_PRIORITY_DEFAULT,
                [](gpointer data) -> gboolean {
                    auto *t = static_cast<std::function<void()> *>(data);
                    (*t)();
                    return G_SOURCE_REMOVE;
                },
                taskPtr,
                [](gpointer data) {
                    delete static_cast<std::function<void()> *>(data);
                }
            );
        }

        static GMainContext *getContext() {
            return context_;
        }

        static GMainLoop *getMainLoop() {
            return mainLoop_;
        }

        static void terminate() {
            if (mainLoop_) {
                    g_main_loop_quit(mainLoop_);
            }
        }

        static bool isBusy() {
            return mainLoop_ != nullptr && g_main_loop_is_running(mainLoop_);
        }

        static void runMainLoop() {
            context_ = g_main_context_default();
            mainLoop_ = g_main_loop_new(context_, FALSE);
            g_main_loop_run(mainLoop_);
            g_main_loop_quit(mainLoop_);
            g_main_loop_unref(mainLoop_);
            mainLoop_ = nullptr;
            context_ = nullptr;
        }
    };
}
