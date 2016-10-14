#ifndef _WAT_RAFT_HANDLER_H_
#define _WAT_RAFT_HANDLER_H_

#include "WatRaft.h"
#include <string>
#include <vector>
#include <map>

namespace WatRaft {

class WatRaftServer; // Forward declaration.

class WatRaftHandler : virtual public WatRaftIf {
  public:
    explicit WatRaftHandler(WatRaftServer* raft_server);
    virtual ~WatRaftHandler();

    void get(std::string& _return, const std::string& key);
    void put(const std::string& key, const std::string& val);
    void append_entries(AEResult& _return, 
                        const int32_t term, 
                        const int32_t leader_id, 
                        const int32_t prev_log_index, 
                        const int32_t prev_log_term, 
                        const std::vector<Entry> & entries, 
                        const int32_t leader_commit_index);
    void request_vote(RVResult& _return, 
                      const int32_t term, 
                      const int32_t candidate_id, 
                      const int32_t last_log_index, 
                      const int32_t last_log_term);
    void debug_echo(std::string& _return, const std::string& msg);
  private:
    WatRaftServer* server;
};
} // namespace WatRaft

#endif
