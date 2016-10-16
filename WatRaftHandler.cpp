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
    ServerMap::const_iterator it = server->get_servers()->begin();
    
    int votes = 0;
    for (; it != server->get_servers()->end(); it++) {
        if (it->first == server->node_id) {
            continue; // Skip itself
        }
        
        AEResult remote_ae_result;
        try {
            std::vector<Entry> entryList;
            //TODO: keep track of last committed indices of each follower and send back a
            //batch of entries instead of just the one
            entryList.push_back(entry);
            remote_ae_result = server->send_ae_request(it->first, entryList);
            if(remote_ae_result.success) {
                votes++;
            }
        } catch (apache::thrift::transport::TTransportException e) {
            printf("Caught exception: %s\n", e.what());
        }
    }
    if(votes >= server->get_quorum()) {
        server->current_committed_index = server->commit_log.size();
        server->update_state_machine(key, val);
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
    _return.term = server->current_term;
    server->contacted_leader = true;
    
    if(term < server->current_term) {
        _return.success = false;
    } else if(prev_log_index < server->get_last_log_index()) {
        _return.success = false;
    } else {
        server->set_as_follower();
        server->current_leader_id = leader_id;
        server->add_log_entries(entries, prev_log_index);
        _return.success = true;
    }
}

void WatRaftHandler::request_vote(RVResult& _return,
                                  const int32_t term,
                                  const int32_t candidate_id,
                                  const int32_t last_log_index,
                                  const int32_t last_log_term) {
    // Your implementation goes here
    printf("request_vote: term: %d | candidateid: %d\n",term, candidate_id);
    
    server->contacted_leader = true;
    if(!server->voted_this_term && term >= server->current_term && last_log_term >= server->get_last_log_term() && last_log_index >= server->get_last_log_index()) {
        _return.vote_granted = true;
        _return.term = term;
        server->voted_this_term = true;
        server->current_term = term;
        server->set_as_follower();
        printf("granted_vote: term: %d | candidateid: %d\n",term, candidate_id);
    } else {
        _return.vote_granted = false;
        _return.term = server->current_term;
    }    
}

void WatRaftHandler::debug_echo(std::string& _return, const std::string& msg) {
    _return = msg;
    printf("debug_echo\n");
}
} // namespace WatRaft

