#include "WatRaftHandler.h"
#include <string>
#include <vector>
#include <WatRaftConfig.h>
#include "WatRaftServer.h"

namespace WatRaft {
    
    WatRaftHandler::WatRaftHandler(WatRaftServer* raft_server) : server(raft_server) {
        // Your initialization goes here
    }
    
    WatRaftHandler::~WatRaftHandler() {}
    
    void WatRaftHandler::get(std::string& _return, const std::string& key) {
        // Your implementation goes here
    }
    
    void WatRaftHandler::put(const std::string& key, const std::string& val) {
        // Your implementation goes here
        std::cout << "Received put request. key: " << key << "term: " << server->current_term <<"\n";
        server->processed_request = true;
        
        Entry entry;
        entry.term = server->current_term;
        entry.key = key;
        entry.val = val;
        server->add_log_entry(entry);
        std::atomic<int> my_vote(1);
        server->quorum_log.push_back(my_vote);
        int quorum_index = server->quorum_log.size()-1; // this should be atopmic if put is done in multiple threads
        ServerMap::const_iterator it = server->get_servers()->begin();
        server->serialize_current_state();
        //create threads equal to number of servers minus current one
        std::vector<pthread_t> ae_threads;
        
        for (; it != server->get_servers()->end(); it++) {
            if (it->first == server->node_id) {
                continue; // Skip itself
            }
            
            AeRequest* rq = new AeRequest;
            rq->raft = server;
            rq->node_id = it->first;
            rq->quorum_index = quorum_index;

           
            pthread_t ae_req;
            int result = pthread_create(&ae_req, NULL,  server->process_ae, (void *)rq);
            if(result != 0) {
                throw result;
            }
            ae_threads.push_back(ae_req);
            
        }
        
        for(std::vector<pthread_t>::iterator t_it = ae_threads.begin(); t_it != ae_threads.end(); ++t_it) {
            pthread_join(*t_it, NULL);
        }
    }
    
    void WatRaftHandler::append_entries(AEResult& _return,
                                        const int32_t term,
                                        const int32_t leader_id,
                                        const int32_t prev_log_index,
                                        const int32_t prev_log_term,
                                        const std::vector<Entry> & entries,
                                        const int32_t leader_commit_index) {
        // Your implementation goes here
        printf("append_entries: term: %d | leaderid: %d\n",term, leader_id);
        
        server->contacted_leader = true;
        std::cout << "prev_log_index: " << std::to_string(prev_log_index) << "prev_term: " << std::to_string(prev_log_term) << std::endl;
        
        if(term < server->current_term) {
            _return.success = false;
        } else if (!server->check_prev_log(prev_log_term, prev_log_index)) {
            _return.success = false;
        } else {
            server->set_as_follower();//the heartbeat timeout should be set to low so that this is unnecessary
            server->current_term = term;
            server->current_leader_id = leader_id;
            server->add_log_entries(entries, prev_log_index);
            server->current_committed_index = leader_commit_index;
            server->update_state_machine();
            _return.success = true;
        }
        _return.term = server->current_term;
        server->serialize_current_state();
    }
    
    void WatRaftHandler::request_vote(RVResult& _return,
                                      const int32_t term,
                                      const int32_t candidate_id,
                                      const int32_t last_log_index,
                                      const int32_t last_log_term) {
        // Your implementation goes here
        printf("request_vote: term: %d | candidateid: %d\n",term, candidate_id);
        
        server->contacted_leader = true;
        
        if((term > server->current_term || (term == server->current_term && (!server->voted_for || server->voted_for == candidate_id)))
           && (last_log_term > server->get_last_log_term() || (last_log_term == server->get_last_log_term() && last_log_index >= server->get_last_log_index()))) {
            _return.vote_granted = true;
            _return.term = term;
            server->voted_for = candidate_id;
            server->current_term = term;
            server->set_as_follower();
            printf("granted_vote: term: %d | candidateid: %d\n",term, candidate_id);
        } else {
            _return.vote_granted = false;
            _return.term = server->current_term;
        }
        server->serialize_current_state();
    }
    
    void WatRaftHandler::debug_echo(std::string& _return, const std::string& msg) {
        _return = msg;
        printf("debug_echo\n");
    }
    
    
    
} // namespace WatRaft

