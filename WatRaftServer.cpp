#include "WatRaftServer.h"
#include "WatRaft.h"
#include "WatRaftState.h"
#include "WatRaftHandler.h"
#include "WatRaftConfig.h"
#include <pthread.h>
#include <protocol/TBinaryProtocol.h>
#include <server/TSimpleServer.h>
#include <transport/TSocket.h>
#include <transport/TServerSocket.h>
#include <transport/TBufferTransports.h>
#include <thrift/concurrency/ThreadManager.h>
#include <thrift/concurrency/PosixThreadFactory.h>
#include <thrift/server/TThreadedServer.h>
#include <future>
#include <chrono>
#include <time.h>
#include <math.h>

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;
using namespace ::apache::thrift::concurrency;

using boost::shared_ptr;

namespace WatRaft {
    
    WatRaftServer::WatRaftServer(int node_id, const WatRaftConfig* config)
    throw (int) : node_id(node_id), rpc_server(NULL), config(config) {
        int rc = pthread_create(&rpc_thread, NULL, start_rpc_server, this);
        if (rc != 0) {
            throw rc; // Just throw the error code
        }
        srand (time(NULL));
    }
    
    WatRaftServer::~WatRaftServer() {
        printf("In destructor of WatRaftServer\n");
        delete rpc_server;
    }
    
    int WatRaftServer::wait() {
        wat_state.wait_ge(WatRaftState::SERVER_CREATED);
        // Perhaps perform your periodic tasks in this thread.
        
        contacted_leader =  false;
        int election_timeout = pthread_create(&timeout_thread, NULL, election_timer, this);
        if (election_timeout != 0) {
            throw election_timeout; // Just throw the error code
        }
        
        set_as_follower();
        
        while (true) {
            wat_state.wait_e(WatRaftState::CANDIDATE);
            printf("iterating main as candidate. term: %d\n",current_term);
            ServerMap::const_iterator it = config->get_servers()->begin();
            
            int numberOfVotes = 0;
            for (; it != config->get_servers()->end(); it++) {
                if (it->first == node_id) {
                    continue; // Skip itself
                }
                
                RVResult remote_rv_result;
                try {
                    remote_rv_result = send_rv_request(it->first);
                    
                } catch (TTransportException e) {
                    printf("Caught exception: %s\n", e.what());
                }
                
                if(remote_rv_result.term == current_term && remote_rv_result.vote_granted) {
                    numberOfVotes++;
                }
            }
            
            if(numberOfVotes >= get_quorum()) {
                elect_as_leader();
            } else {
                set_as_follower();
            }
        }
        pthread_join(rpc_thread, NULL);
        return 0;
    }
    
    RVResult WatRaftServer::send_rv_request(int node_id) {
        
        IPPortPair node = config->get_servers()->at(node_id);
        boost::shared_ptr<TSocket> socket(
                                          new TSocket(node.ip, node.port));
        boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
        boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
        WatRaftClient client(protocol);
        RVResult remote_rv_result;
        transport->open();
        client.request_vote(remote_rv_result, current_term, node_id, last_log_index, last_log_term);
        transport->close();
        // Create a WatID object from the return value.
        std::cout << "Received "<< remote_rv_result << "from "<< node.ip
        << ":" << node.port << std::endl;
        
        return remote_rv_result;
    }
    
    void WatRaftServer::send_ae_request(int node_id) {
        
    }
    
    void WatRaftServer::elect_as_leader() {
        wat_state.change_state(WatRaftState::LEADER);
        std::cout << "Elected node " << get_id() << "as leader\n";
    }
    
    void WatRaftServer::set_as_follower() {
        wat_state.change_state(WatRaftState::FOLLOWER);
        std::cout << "Set node " << get_id() << "as follower\n";
    }
    
    void WatRaftServer::wait_till_follower() {
        wat_state.wait_e(WatRaftState::FOLLOWER);
    }
    
    int WatRaftServer::get_quorum() {
        
        return ceil((double) config->get_servers()->size()/2);
    }
    
    
    int WatRaftServer::get_election_timeout() {
        return rand()%std_election_timeout + std_election_timeout;
    }
    
    void WatRaftServer::set_candidate_state() {
        wat_state.change_state(WatRaftState::CANDIDATE);
        printf("Set candidate state\n");
    }
    
    void* WatRaftServer::election_timer(void* param) {
        WatRaftServer* raft = static_cast<WatRaftServer*>(param);
        while(true) {
            int timeout = raft->get_election_timeout();
            printf("restarting election timers %d\n", timeout);
            std::this_thread::sleep_for(std::chrono::milliseconds(timeout));
            raft->wait_till_follower();
            if(!(raft->contacted_leader || raft->voted_this_term)) {
                raft->set_candidate_state();
                raft->current_term++;
            }
            raft->contacted_leader = false;
            raft->voted_this_term = false;
        }
        return NULL;
    }
    
    void WatRaftServer::set_rpc_server(TThreadedServer* server) {
        rpc_server = server;
        wat_state.change_state(WatRaftState::SERVER_CREATED);
    }
    
    void* WatRaftServer::start_rpc_server(void* param) {
        WatRaftServer* raft = static_cast<WatRaftServer*>(param);
        shared_ptr<WatRaftHandler> handler(new WatRaftHandler(raft));
        shared_ptr<TProcessor> processor(new WatRaftProcessor(handler));
        // Get IP/port for this node
        IPPortPair this_node =
        raft->config->get_servers()->find(raft->node_id)->second;
        shared_ptr<TServerTransport> serverTransport(
                                                     new TServerSocket(this_node.ip, this_node.port));
        shared_ptr<TTransportFactory> transportFactory(
                                                       new TBufferedTransportFactory());
        shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());
        shared_ptr<ThreadManager> threadManager =
        ThreadManager::newSimpleThreadManager(num_rpc_threads, 0);
        shared_ptr<PosixThreadFactory> threadFactory =
        shared_ptr<PosixThreadFactory>(new PosixThreadFactory());
        threadManager->threadFactory(threadFactory);
        threadManager->start();
        TThreadedServer* server = new TThreadedServer(
                                                      processor, serverTransport, transportFactory, protocolFactory);
        raft->set_rpc_server(server);
        server->serve();
        return NULL;
    }
} // namespace WatRaft

using namespace WatRaft;

int main(int argc, char **argv) {
    if (argc < 3) {
        printf("Usage: %s server_id config_file\n", argv[0]);
        return -1;
    }
    WatRaftConfig config;
    config.parse(argv[2]);
    try {
        WatRaftServer server(atoi(argv[1]), &config);
        server.wait(); // Wait until server shutdown.
    } catch (int rc) {
        printf("Caught exception %d, exiting\n", rc);
        return -1;
    }
    return 0;
}
