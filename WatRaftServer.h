#ifndef _WAT_RAFT_SERVER_H_
#define _WAT_RAFT_SERVER_H_

#include "WatRaft.h"
#include "WatRaftState.h"
#include "WatRaftConfig.h"
#include <pthread.h>
#include <string>
#include <thrift/server/TThreadedServer.h>
#include <boost/serialization/access.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>

using namespace boost::archive;

namespace WatRaft {
    template <typename T>
    struct atomwrapper
    {
        std::atomic<T> _a;
        
        atomwrapper()
        :_a()
        {}
        
        atomwrapper(const std::atomic<T> &a)
        :_a(a.load())
        {}
        
        atomwrapper(const atomwrapper &other)
        :_a(other._a.load())
        {}
        
        atomwrapper &operator=(const atomwrapper &other)
        {
            _a.store(other._a.load());
        }
    };


class WatRaftConfig; // Forward declaration
class WatRaftServer {
  public:
    const int std_election_timeout = 5000;
    int current_term = 0;
    int current_committed_index =0;
    int current_leader_id = 0;
    int last_applied_index = 0;
    int node_id;
    bool contacted_leader = false;
    bool processed_request = false;
    bool voted_this_term = false;
    int voted_for = 0;
    std::vector<Entry> commit_log;
    std::vector<atomwrapper<int>> quorum_log;
    std::vector<int> next_index;
    std::vector<int> match_index;
    
    WatRaftServer(int node_id, const WatRaftConfig* config) throw (int);
    ~WatRaftServer();
    // Block and wait until the server shutdowns.
    int wait();
    // Set the RPC server once it is created in a child thread.
    void set_rpc_server(apache::thrift::server::TThreadedServer* server);
    int get_id() { return node_id; }
    void set_candidate_state();
    int get_election_timeout();
    int get_quorum();
    void elect_as_leader();
    void set_as_follower();
    void wait_till_follower();
    RVResult send_rv_request(int node_id);
    AEResult send_ae_request(int node_id, const std::vector<Entry> & entries);
    void wait_till_candidate();
    const ServerMap* get_servers();
    void wait_till_leader();
    void client_put(int node_id,const std::string& key,const std::string& value);
    void add_log_entry(Entry entry);
    void add_log_entries(std::vector<Entry> entries, int last_index);
    void update_state_machine();
    int get_last_log_term();
    int get_last_log_index();
    bool check_prev_log(int prev_term, int prev_index);
    void serialize_state_machine();
    void deserialize_state_machine();
    void serialize_current_state();
    void deserialize_current_state();
    static void* process_ae(void* param);
    std::string get_state_machine_value(std::string key);
    std::string client_get(int node_id,const std::string& key);
    WatRaftState::State get_current_state();
    int log_level = 0;
  private:
    friend class boost::serialization::access;
        apache::thrift::server::TThreadedServer* rpc_server;
    const WatRaftConfig* config;
        pthread_t rpc_thread, timeout_thread, election_thread, heartbeat_thread, client_thread;
    WatRaftState wat_state;   // Tracks the current state of the node.
    std::map<std::string, std::string> state_machine;
    template<typename Archive>
    void serialize(Archive & ar, const unsigned int version);
    static const int num_rpc_threads = 64;
    static void* start_rpc_server(void* param);
    static void* election_timer(void* param);
    static void* do_election(void*param);
    static void* do_heartbeats(void* param);
    static void* do_client_request(void* param);
};
    
    struct AeRequest{
        WatRaftServer* raft;
        int node_id;
        int quorum_index;
    };

} // namespace WatRaft

//non_intrusive serialization of class Entry
namespace boost {
    namespace serialization {
        using namespace WatRaft;
        
        template<typename Archive>
        void serialize(Archive & ar, Entry & e, const unsigned int version)
        {
            ar & e.term;
            ar & e.key;
            ar & e.val;
        }
        
    } // namespace serialization
} // namespace boost
#endif
