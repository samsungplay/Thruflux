#pragma once
#include <future>
#include <string>
#include <boost/asio/post.hpp>

#include "Contexts.hpp"
#include "Stream.hpp"
#include "ThreadManager.hpp"
#include "../sender/SenderConfig.hpp"
#include "../receiver/ReceiverConfig.hpp"
#include "Types.hpp"

extern "C" {
#include <agent.h>
#include <gio/gnetworking.h>
}

namespace common {
    using ConnectionCallback = std::function<void (NiceAgent *agent, bool success, guint streamId, int n)>;


    using CandidatesCallback = std::function<void (CandidatesResult result)>;

    struct IceStreamState {
        int totalComponents;
        int readyComponents = 0;
        bool alreadyFired = false;
        ConnectionCallback callback;

        IceStreamState(const int n, ConnectionCallback callback) : totalComponents(n), callback(std::move(callback)) {
        }
    };

    NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(CandidatesResult, ufrag, password, serializedCandidates);


    struct IceAgentContext {
        NiceAgent *agent;
        guint streamId;
    };


    inline static std::vector<StunServer> stunServers_;
    inline static std::vector<TurnServer> turnServers_;
    inline static std::unordered_map<std::string, IceAgentContext> agentsMap_;
    inline static IceAgentContext receiverAgentContext_;

    class IceHandler {
    public:
        static const char *candTypeStr(NiceCandidateType t) {
            switch (t) {
                case NICE_CANDIDATE_TYPE_HOST: return "host";
                case NICE_CANDIDATE_TYPE_SERVER_REFLEXIVE: return "srflx";
                case NICE_CANDIDATE_TYPE_PEER_REFLEXIVE: return "prflx";
                case NICE_CANDIDATE_TYPE_RELAYED: return "relayed";
                default: return "unknown";
            }
        }

        static const char *candTransportStr(NiceCandidateTransport t) {
            switch (t) {
                case NICE_CANDIDATE_TRANSPORT_UDP: return "udp";
                case NICE_CANDIDATE_TRANSPORT_TCP_ACTIVE: return "tcp_active";
                case NICE_CANDIDATE_TRANSPORT_TCP_PASSIVE: return "tcp_passive";
                case NICE_CANDIDATE_TRANSPORT_TCP_SO: return "tcp_so";
                default: return "unknown";
            }
        }

        static std::string niceAddrToString(const NiceAddress &addr) {
            gchar ip[NICE_ADDRESS_STRING_LEN] = {0};
            nice_address_to_string(&addr, ip);
            const guint port = nice_address_get_port(&addr);
            return std::string(ip) + ":" + std::to_string(port);
        }

        static void initialize() {
            g_networking_init();
        }

        static std::unordered_map<std::string, IceAgentContext> &getAgentsMap() {
            return agentsMap_;
        }

        static void dispose(const std::string &receiverId) {
            if (const auto it = agentsMap_.find(receiverId); it != agentsMap_.end()) {
                NiceAgent *agent = it->second.agent;
                nice_agent_close_async(agent, nullptr, nullptr);
                agentsMap_.erase(it);
            }
        }

        static void destroy() {
            if (receiverAgentContext_.agent) {
                g_object_unref(receiverAgentContext_.agent);
            }
            for (auto &agentContext: agentsMap_ | std::views::values) {
                g_object_unref(agentContext.agent);
            }
        }

        static void addStunServer(StunServer stunServer) {
            stunServers_.push_back(std::move(stunServer));
        }

        static void addTurnServer(TurnServer turnServer) {
            turnServers_.push_back(std::move(turnServer));
        }


        static void gatherLocalCandidates(const bool isSender, const std::string receiverId, const int n,
                                          const CandidatesCallback callback
        ) {
            NiceAgent *agent = nice_agent_new(ThreadManager::getContext(), NICE_COMPATIBILITY_RFC5245);


            g_object_set(agent, "controlling-mode", isSender, NULL);

            if (!stunServers_.empty()) {
                g_object_set(agent, "stun-server", stunServers_[0].host.c_str(), NULL);
                g_object_set(agent, "stun-server-port", stunServers_[0].port, NULL);
            }

            const guint stream_id = nice_agent_add_stream(agent, n);

            if (isSender) {
                agentsMap_.emplace(std::move(receiverId), IceAgentContext{
                                       .agent = agent,
                                       .streamId = stream_id
                                   });
            } else {
                receiverAgentContext_.agent = agent;
                receiverAgentContext_.streamId = stream_id;
            }

            for (int i = 1; i <= n; i++) {
                nice_agent_set_port_range(agent, stream_id, i, 49152, 65535);
                nice_agent_attach_recv(agent, stream_id, 1, common::ThreadManager::getContext(),
                                       [](NiceAgent *agent, guint stream_id, guint component_id,
                                          guint len, gchar *buf, gpointer user_data) {
                                       },
                                       nullptr
                );
            }
            for (const auto &turn: turnServers_) {
                for (int i = 1; i <= n; i++) {
                    nice_agent_set_relay_info(agent, stream_id, i, turn.host.c_str(), turn.port,
                                              turn.username.c_str(),
                                              turn.password.c_str(), NICE_RELAY_TYPE_TURN_UDP);
                }
            }


            const auto onGatheringDoneCallback = [isSender, n, agent, stream_id, callback = std::move(callback)]() {
                auto serializedCandidates = nlohmann::json::array();
                gchar *ufrag = nullptr;
                gchar *password = nullptr;
                nice_agent_get_local_credentials(agent, stream_id, &ufrag, &password);

                for (int i = 1; i <= n; i++) {
                    GSList *localCandidates = nice_agent_get_local_candidates(agent, stream_id, i);
                    for (const GSList *iterator = localCandidates; iterator != nullptr; iterator = iterator->next) {
                        const auto candidate = static_cast<NiceCandidate *>(iterator->data);

                        // spdlog::info("Cand: type={} transport={} addr={} base={} prio={}",
                        //              (int) candidate->type,
                        //              (int) candidate->transport,
                        //              niceAddrToString(candidate->addr),
                        //              niceAddrToString(candidate->base_addr),
                        //              (uint32_t) candidate->priority);

                        if (candidate->transport != NICE_CANDIDATE_TRANSPORT_UDP) {
                            continue;
                        }
                        if ((isSender && sender::SenderConfig::forceTurn) || (
                                !isSender && receiver::ReceiverConfig::forceTurn)) {
                            if (candidate->type != NICE_CANDIDATE_TYPE_RELAYED) {
                                continue;
                            }
                        }
                        if (gchar *cand_str = nice_agent_generate_local_candidate_sdp(agent, candidate)) {
                            serializedCandidates.push_back({
                                {"candidate", cand_str},
                                {"componentId", i}
                            });
                            g_free(cand_str);
                        }
                    }
                    g_slist_free_full(localCandidates, reinterpret_cast<GDestroyNotify>(nice_candidate_free));
                }

                auto result = CandidatesResult{
                    .ufrag = ufrag ? std::string(ufrag) : "",
                    .password = password ? std::string(password) : "",
                    .serializedCandidates = std::move(serializedCandidates)
                };

                g_free(ufrag);
                g_free(password);

                callback(std::move(result));
            };


            auto *onGatheringDoneCallbackPtr = new decltype(onGatheringDoneCallback)(
                std::move(onGatheringDoneCallback));

            g_signal_connect_data(agent, "candidate-gathering-done",
                                  G_CALLBACK(+[](NiceAgent* agent, guint stream_id, gpointer data) {
                                      auto *callback = static_cast<decltype(onGatheringDoneCallback) *>(data);
                                      (*callback)();
                                      }),
                                  (gpointer) onGatheringDoneCallbackPtr,
                                  [](gpointer data, GClosure *) {
                                      delete static_cast<decltype(onGatheringDoneCallback) *>(data);
                                  },
                                  static_cast<GConnectFlags>(0)
            );
            nice_agent_gather_candidates(agent, stream_id);
        }

        static void establishConnection(bool isSender, const std::string receiverId,
                                        const CandidatesResult &remoteCredentials,
                                        const ConnectionCallback &callback
        ) {
            NiceAgent *agent = nullptr;
            int streamId = -1;
            if (isSender) {
                auto it = agentsMap_.find(std::move(receiverId));
                if (it != agentsMap_.end()) {
                    agent = it->second.agent;
                    streamId = it->second.streamId;
                }
            } else {
                agent = receiverAgentContext_.agent;
                streamId = receiverAgentContext_.streamId;
            }

            if (!agent || streamId < 0) {
                return;
            }

            if (!nice_agent_set_remote_credentials(agent, streamId, remoteCredentials.ufrag.c_str(),
                                                   remoteCredentials.password.c_str())) {
                callback(nullptr, false, streamId, -1);
                return;
            }

            std::map<int, GSList *> componentListsMap;

            for (const auto &item: remoteCredentials.serializedCandidates) {
                int componentId = item["componentId"];
                std::string candidateString = item["candidate"];
                NiceCandidate *candidate = nice_agent_parse_remote_candidate_sdp(agent, componentId,
                    candidateString.c_str());
                if (candidate) {
                    componentListsMap[componentId] = g_slist_append(componentListsMap[componentId], candidate);
                }
            }

            int n = componentListsMap.size();

            for (auto &[componentId, list]: componentListsMap) {
                nice_agent_set_remote_candidates(agent, streamId, componentId, list);
                g_slist_free_full(list, reinterpret_cast<GDestroyNotify>(nice_candidate_free));
            }


            auto streamState = std::make_shared<IceStreamState>(n,
                                                                std::move(callback));

            auto componentStateChangedCallback = [streamState = std::move(streamState), agent,n
                    ](guint stream_id, guint state) {
                if (streamState->alreadyFired) return;

                if (state == NICE_COMPONENT_STATE_READY) {
                    streamState->alreadyFired = true;
                    streamState->callback(agent, true, stream_id, n);
                } else if (state == NICE_COMPONENT_STATE_FAILED) {
                    streamState->alreadyFired = true;
                    streamState->callback(nullptr, false, stream_id, n);
                }
            };

            auto componentStateChangedCallbackPtr = new decltype(componentStateChangedCallback)(
                std::move(componentStateChangedCallback));

            g_signal_connect_data(agent, "component-state-changed",
                                  G_CALLBACK(
                                      +[](NiceAgent* agent, guint stream_id, guint component_id, guint state,
                                          gpointer
                                          data) {

                                      auto *c = static_cast<decltype(componentStateChangedCallback) *>(data);
                                      (*c)(stream_id, state);

                                      }),
                                  componentStateChangedCallbackPtr,
                                  [](const gpointer data, GClosure *) {
                                      delete static_cast<decltype(componentStateChangedCallback) *>(data);
                                  },
                                  static_cast<GConnectFlags>(0)
            );
        }
    };
}
