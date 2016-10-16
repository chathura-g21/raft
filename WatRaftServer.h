#ifndef _WAT_RAFT_SERVER_H_
#define _WAT_RAFT_SERVER_H_

#include "WatRaft.h"
#include "WatRaftState.h"
#include "WatRaftConfig.h"
#include <pthread.h>
#include <string>
#include <thrift/server/TThreadedServer.h>

namespace WatRaft {

class WatRaftConfig; // Forward declaration
class WatRaftServer {
  public:
    WatRaftServer(int node_id, const WatRaftConfig* config) throw (int);
    ~WatRaftServer();
  
    // Block and wait until the server shutdowns.
    int wait();
    // Set the RPC server once it is created in a child thread.
    void set_rpc_server(apache::thrift::server::TThreadedServer* server);
    int get_id() { return node_id; }
    void set_candidate_state();
    int get_election_timeout();
    const int std_election_timeout = 5000;
    int current_term = 0;
    int current_committed_index =0;
    int current_leader_id = 0;
    int last_applied_index = 0;
    int get_quorum();
    void elect_as_leader();
    int node_id;
    bool contacted_leader = false;
    bool processed_request = false;
    void set_as_follower();
    void wait_till_follower();
    int voted_this_term = false;
    RVResult send_rv_request(int node_id);
    AEResult send_ae_request(int node_id, const std::vector<Entry> & entries);
    std::vector<Entry> commit_log;
    void wait_till_candidate();
    const ServerMap* get_servers();
    void wait_till_leader();
    void client_put(int node_id,const std::string& key,const std::string& value);
    void add_log_entry(Entry entry);
    void add_log_entries(std::vector<Entry> entries, int last_index);
    void update_state_machine();
    int get_last_log_term();
    int get_last_log_index();
    
  private:
        apache::thrift::server::TThreadedServer* rpc_server;
    const WatRaftConfig* config;
        pthread_t rpc_thread, timeout_thread, election_thread, heartbeat_thread, client_thread;
    WatRaftState wat_state;   // Tracks the current state of the node.
    std::map<std::string, std::string> state_machine;
    static const int num_rpc_threads = 64;
    static void* start_rpc_server(void* param);
    static void* election_timer(void* param);
    static void* do_election(void*param);
    static void* do_heartbeats(void* param);
    static void* do_client_request(void* param);
    
};
} // namespace WatRaft

#endif
