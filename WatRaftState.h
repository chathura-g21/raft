#ifndef _WAT_RAFT_STATE_H_
#define _WAT_RAFT_STATE_H_

#include <pthread.h>

namespace WatRaft {

class WatRaftState {
 public:
  // Add more states as needed.
  enum State {INIT, SERVER_CREATED, FOLLOWER, CANDIDATE, LEADER};

  WatRaftState();
  ~WatRaftState();
  // Change the internal state of WatRaftState.
  void change_state(State state);  
  // Wait until state equals the parameter.
  void wait_e(State state);
  // Wait until state is greater than or equal to the parameter.
  void wait_ge(State state);

 private:
  State raft_state;
  pthread_cond_t state_change;
  pthread_mutex_t wait_on_state;
};
} // namespace WatRaft

#endif
