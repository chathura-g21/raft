#include "WatRaftHandler.h"
#include <string>
#include <vector>

#include "WatRaftServer.h"

namespace WatRaft {

WatRaftHandler::WatRaftHandler(WatRaftServer* raft_server) : server(raft_server) {
  // Your initialization goes here
}

WatRaftHandler::~WatRaftHandler() {}

void WatRaftHandler::get(std::string& _return, const std::string& key) {
    // Your implementation goes here
    printf("get\n");
}

void WatRaftHandler::put(const std::string& key, const std::string& val) {
    // Your implementation goes here
    printf("put\n");
}    

void WatRaftHandler::append_entries(AEResult& _return,
                                    const int32_t term,
                                    const int32_t leader_id,
                                    const int32_t prev_log_index,
                                    const int32_t prev_log_term,
                                    const std::vector<Entry> & entries,
                                    const int32_t leader_commit_index) {
    // Your implementation goes here
    printf("append_entries\n");
}

void WatRaftHandler::request_vote(RVResult& _return,
                                  const int32_t term,
                                  const int32_t candidate_id,
                                  const int32_t last_log_index,
                                  const int32_t last_log_term) {
    // Your implementation goes here
    printf("request_vote: term: %d | candidateid: %d\n",term, candidate_id);
    
    server->contacted_leader = true;
    if(!server->voted_this_term && term >= server->current_term && last_log_term >= server->last_log_term && last_log_index >= server->last_log_index) {
        _return.vote_granted = true;
        _return.term = term;
        server->voted_this_term = true;
        leader_id = candidate_id;
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

