#pragma once
#include <boost/asio/thread_pool.hpp>
#include <gio/gnetworking.h>
#include <glib/gmain.h>

namespace common {
    class ThreadManager {
        inline static GMainContext *context_;
        inline static GMainLoop *mainLoop_;
        inline static std::atomic<bool> terminating_{false};

    public:
        //run some task on the main thread
        static void postTask(std::function<void()> task) {
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
            if (!terminating_.load()) {
                terminating_.store(true);
                g_main_loop_quit(mainLoop_);
            }
        }

        static void runMainLoop() {
            context_ = g_main_context_default();
            mainLoop_ = g_main_loop_new(context_, FALSE);
            g_main_loop_run(mainLoop_);
            g_main_loop_quit(mainLoop_);
            g_main_loop_unref(mainLoop_);
            g_main_context_unref(context_);
        }
    };
}
