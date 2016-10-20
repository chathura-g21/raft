#include "WatRaftConfig.h"
#include "WatRaftServer.h"
#include "WatRaft.h"
#include "WatRaftState.h"
#include "WatRaftHandler.h"
#include <pthread.h>
#include <protocol/TBinaryProtocol.h>
#include <server/TSimpleServer.h>
#include <transport/TSocket.h>
#include <transport/TServerSocket.h>
#include <transport/TBufferTransports.h>
#include <thrift/concurrency/ThreadManager.h>
#include <thrift/concurrency/PosixThreadFactory.h>
#include <thrift/server/TThreadedServer.h>
#include <boost/serialization/map.hpp>
#include <boost/serialization/vector.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
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
using namespace boost::archive;

namespace WatRaft {
    
    WatRaftServer::WatRaftServer(int node_id, const WatRaftConfig* config)
    throw (int) : node_id(node_id), rpc_server(NULL), config(config) {
        int rc = pthread_create(&rpc_thread, NULL, start_rpc_server, this);
        if (rc != 0) {
            throw rc; // Just throw the error code
        }
        srand (time(NULL));
        deserialize_state_machine();
        deserialize_current_state();
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
        
        int do_candidate = pthread_create(&election_thread, NULL, do_election , this);
        if(do_candidate != 0) {
            throw do_candidate;
        }
        
        int do_heartbeat = pthread_create(&heartbeat_thread, NULL, do_heartbeats , this);
        if(do_heartbeat != 0) {
            throw do_heartbeat;
        }
        
        int client = pthread_create(&client_thread, NULL, do_client_request, this);
        if(client != 0) {
            throw client;
        }
        
        //        while (true) {
        //
        //        }
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
        
        client.request_vote(remote_rv_result, current_term, this->node_id, commit_log.size(), get_last_log_term());
        transport->close();
        
        std::cout << "Received "<< remote_rv_result << "from "<< node.ip
        << ":" << node.port << std::endl;
        
        return remote_rv_result;
    }
    
    AEResult WatRaftServer::send_ae_request(int node_id, const std::vector<Entry> & entries) {
        
        IPPortPair node = config->get_servers()->at(node_id);
        boost::shared_ptr<TSocket> socket(
                                          new TSocket(node.ip, node.port));
        boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
        boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
        WatRaftClient client(protocol);
        AEResult remote_ae_result;
        transport->open();
        int prev_log_index = next_index[node_id-1];//one based index
        int prev_log_term = (prev_log_index > 0 && !commit_log.empty())?
        commit_log[prev_log_index-1].term: 0;
        std::cout << "sending_prev_log" << std::to_string(prev_log_index) << std::endl;
        
        client.append_entries(remote_ae_result, current_term, this->node_id, prev_log_index, prev_log_term, entries, current_committed_index);
        transport->close();
        match_index[node_id-1] = current_committed_index;
        std::cout << "Received "<< remote_ae_result << "from "<< node.ip
        << ":" << node.port << std::endl;
        
        return remote_ae_result;
    }
    
    void WatRaftServer::client_put(int node_id,const std::string& key,const std::string& value) {
        IPPortPair node = config->get_servers()->at(node_id);
        boost::shared_ptr<TSocket> socket(
                                          new TSocket(node.ip, node.port));
        boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
        boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
        WatRaftClient client(protocol);
        transport->open();
        client.put(key, value);
    }
    
    void WatRaftServer::elect_as_leader() {
        wat_state.change_state(WatRaftState::LEADER);
        next_index.assign(get_servers()->size(), commit_log.size());
        match_index.assign(get_servers()->size(), 0);
        std::cout << "Elected node " << get_id() << "as leader\n";
        std::cout << "Assigning next index as" << commit_log.size() << std::endl;
    }
    
    void WatRaftServer::set_as_follower() {
        wat_state.change_state(WatRaftState::FOLLOWER);
        std::cout << "Set node " << get_id() << "as follower\n";
    }
    
    void WatRaftServer::wait_till_follower() {
        wat_state.wait_e(WatRaftState::FOLLOWER);
    }
    
    void WatRaftServer::wait_till_candidate() {
        wat_state.wait_e(WatRaftState::CANDIDATE);
    }
    
    void WatRaftServer::wait_till_leader() {
        wat_state.wait_e(WatRaftState::LEADER);
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
    
    void WatRaftServer::add_log_entry(Entry entry) {
        commit_log.push_back(entry);
    }
    
    void WatRaftServer::add_log_entries(std::vector<Entry> entries, int last_index) {
        if(last_index < commit_log.size()) {
            //remeber logs are considered to be one based indices
            commit_log.erase(commit_log.begin()+last_index, commit_log.end());
        }
        commit_log.insert(commit_log.end(), entries.begin(), entries.end());
    }
    
    const ServerMap* WatRaftServer::get_servers() {
        return config->get_servers();
    }
    
    void WatRaftServer::update_state_machine() {
        
        std::vector<Entry>::iterator it= commit_log.begin()+last_applied_index;
        for(;it < commit_log.begin()+current_committed_index; it++) {
            state_machine[it->key] = it->val;
        }
        
        last_applied_index = current_committed_index;
        std::cout << "Current State Machine {";
        for(std::map<std::string,std::string>::iterator i = state_machine.begin(); i != state_machine.end(); ++i) {
            std::cout << i->first << ": " << i->second << ", ";
        }
        std::cout << "\n";
        serialize_state_machine();
    }
    
    int WatRaftServer::get_last_log_term() {
        return !commit_log.empty()? commit_log.back().term: 0;
    }
    
    int WatRaftServer::get_last_log_index() {
        return commit_log.size();
    }
    
    bool WatRaftServer::check_prev_log(int prev_term, int prev_index) {
        
        bool result = false;
        if(prev_index > 0 && prev_index <= commit_log.size()) {
            Entry prev = commit_log[prev_index-1];
            if(prev.term == prev_term)
                result = true;
        } else if(commit_log.size() == 0) {
            result = true;
        }
        return result;
    }
    
    void WatRaftServer::serialize_state_machine() {
        std::string file_name = "state_machine_" + std::to_string(get_id()) + ".out";
        std::ofstream ofs(file_name);
        boost::archive::text_oarchive oa(ofs);
        oa << state_machine;
    }
    
    void WatRaftServer::deserialize_state_machine() {
        std::string file_name = "state_machine_" + std::to_string(get_id()) + ".out";
        std::ifstream ifs(file_name);
        
        if(ifs.good()) {
            boost::archive::text_iarchive ia(ifs);
            ia >> state_machine;
        }
    }
    
    void WatRaftServer::serialize_current_state() {
        std::string file_name = "current_state" + std::to_string(get_id()) + ".out";
        std::ofstream ofs(file_name);
        boost::archive::text_oarchive oa(ofs);
        oa << (*this);
    }
    
    void WatRaftServer::deserialize_current_state() {
        std::string file_name = "current_state" + std::to_string(get_id()) + ".out";
        std::ifstream ifs(file_name);
        
        if(ifs.good()) {
            boost::archive::text_iarchive ia(ifs);
            ia >> (*this);
        }
    }
    
    template<typename Archive>
    void WatRaftServer::serialize(Archive & ar, const unsigned int version)
    {
        ar & current_term;
        ar & current_leader_id;
        ar & commit_log;
    }
    
    void* WatRaftServer::do_election(void*param) {
        WatRaftServer* raft = static_cast<WatRaftServer*>(param);
        while(true) {
            raft->wait_till_candidate();
            
            raft->current_term++;
            printf("iterating election thread as candidate. term: %d\n",raft->current_term);
            int numberOfVotes = 1;
            ServerMap::const_iterator it = raft->get_servers()->begin();
            for (; it != raft->get_servers()->end(); it++) {
                if (it->first == raft->node_id) {
                    raft->voted_for = raft->node_id;
                    continue; // Skip itself
                }
                
                RVResult remote_rv_result;
                try {
                    remote_rv_result = raft->send_rv_request(it->first);
                    
                    if(remote_rv_result.term <= raft->current_term && remote_rv_result.vote_granted) {
                        numberOfVotes++;
                    } else if(remote_rv_result.term > raft->current_term) {
                        //if term is stale stop the election and revert to follower
                        raft->current_term = remote_rv_result.term;
                        raft->set_as_follower();
                        numberOfVotes = 0;
                        break;
                    }
                } catch (TTransportException e) {
                    printf("Caught exception: %s\n", e.what());
                }
            }
            
            if(numberOfVotes >= raft->get_quorum()) {
                raft->elect_as_leader();
            } else {
                raft->set_as_follower();
            }
            
        }
    }
    
    void* WatRaftServer::do_heartbeats(void* param) {
        WatRaftServer* raft = static_cast<WatRaftServer*>(param);
        while(true) {
            std::this_thread::sleep_for(std::chrono::milliseconds(raft->std_election_timeout/2));
            raft->wait_till_leader();
            printf("Iterating hearbeat thread as leader. term %d\n", raft->current_term);
            if(raft->processed_request) {
                raft->processed_request = false;
                continue;
            }
            
            std::atomic<int> my_vote(1);
            raft->quorum_log.push_back(my_vote);
            int quorum_index = raft->quorum_log.size()-1; // this should be atopmic if put is done in multiple threads
            ServerMap::const_iterator it = raft->get_servers()->begin();
            raft->serialize_current_state();
            //create threads equal to number of servers minus current one
            std::vector<pthread_t> ae_threads;
            //            AeRequest rq[raft->get_servers()->size()];
            for (; it != raft->get_servers()->end(); ++it) {
                if (it->first == raft->node_id) {
                    continue; // Skip itself
                }
                
                AeRequest* rq = new AeRequest;
                rq->raft = raft;
                rq->node_id = it->first;
                rq->quorum_index = quorum_index;
                std::cout << "starting thread for node" << std::to_string(rq->node_id) << std::endl;
                pthread_t ae_req;
                int result = pthread_create(&ae_req, NULL,  raft->process_ae, (void *)rq);
                if(result != 0) {
                    throw result;
                }
                ae_threads.push_back(ae_req);
                
            }
            
            for(std::vector<pthread_t>::iterator t_it = ae_threads.begin(); t_it != ae_threads.end(); ++t_it) {
                pthread_join(*t_it, NULL);
            }
        }
    }
    
    void* WatRaftServer::election_timer(void* param) {
        WatRaftServer* raft = static_cast<WatRaftServer*>(param);
        
        while(true) {
            int timeout = raft->get_election_timeout();
            printf("restarting election timers %d\n", timeout);
            std::this_thread::sleep_for(std::chrono::milliseconds(timeout));
            raft->wait_till_follower();
            if(!(raft->contacted_leader || raft->voted_for)) {
                raft->set_candidate_state();
                //TODO: candidate should reset election timer and not have to revert back to
                //follower
            }
            raft->contacted_leader = false;
            raft->voted_for = 0;
        }
        return NULL;
    }
    
    void WatRaftServer::set_rpc_server(TThreadedServer* server) {
        rpc_server = server;
        wat_state.change_state(WatRaftState::SERVER_CREATED);
    }
    
    void* WatRaftServer::do_client_request(void* param) {
        WatRaftServer* raft = static_cast<WatRaftServer*>(param);
        int value = 0;
        
        //allow an initial grace period for the system to bootstrap
        
        while(true) {
            int timeout = raft->get_election_timeout();
            std::this_thread::sleep_for(std::chrono::milliseconds(timeout/3));
            raft->wait_till_follower();
            
            if(!raft->current_leader_id) {
                continue;
            }
            std::cout << "Sending request as client. leader: " + std::to_string(raft->current_leader_id) + " term: " + std::to_string(raft->current_term);
            try {
                raft->client_put(raft->current_leader_id, std::to_string(raft->get_id()), std::to_string(value));
                value++;
            } catch (TTransportException e) {
                printf("Caught exception: %s\n", e.what());
            }
        }
    }
    
    void* WatRaftServer::process_ae(void* param) {
        AeRequest* ae_args = (AeRequest*) param;
        WatRaftServer* server = ae_args->raft;
        
        AEResult remote_ae_result;
        try {
            do {
                
                
                int last_commit = server->next_index[ae_args->node_id-1];
                server->next_index[ae_args->node_id-1]--;
                
                std::cout << "prev_index for node"<< std::to_string(ae_args->node_id) << " : " << std::to_string(last_commit) << std::endl;
                
                
                if(last_commit > 0) {
                    std::vector<Entry> entryList(server->commit_log.begin()+ last_commit-1, server->commit_log.end());//commit indices are 1 based
                    remote_ae_result = server->send_ae_request(ae_args->node_id, entryList);
                } else {
                    std::vector<Entry> entryList;
                    remote_ae_result = server->send_ae_request(ae_args->node_id, entryList);
                }
                
                
                //TODO: atomically update term if its out of date. not checking it now
            } while (!remote_ae_result.success);
            server->quorum_log[ae_args->quorum_index]._a++;
            int test = server->quorum_log[ae_args->quorum_index]._a;
             printf("Votes for index: %d: from %d/ %d.\n", server->commit_log.size(), test, server->get_quorum());
            if(server->quorum_log[ae_args->quorum_index]._a >= server->get_quorum()) {
                server->quorum_log[ae_args->quorum_index]._a = -1000;
                server->current_committed_index = server->get_last_log_index();
                server->update_state_machine();
                server->serialize_current_state();
            }
        } catch (apache::thrift::transport::TTransportException e) {
            printf("Caught exception: %s\n", e.what());
        }
        server->next_index[ae_args->node_id-1] = server->commit_log.size();//reset next_index for this follower

        delete ae_args;
        return NULL;
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
