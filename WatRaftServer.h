#ifndef _WAT_RAFT_SERVER_H_
#define _WAT_RAFT_SERVER_H_

#include "WatRaft.h"
#include "WatRaftState.h"
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
  
  private:
    int node_id;     
    apache::thrift::server::TThreadedServer* rpc_server;
    const WatRaftConfig* config;
    pthread_t rpc_thread;
    WatRaftState wat_state;   // Tracks the current state of the node.
    static const int num_rpc_threads = 64;
    static void* start_rpc_server(void* param);
};
} // namespace WatRaft

#endif
