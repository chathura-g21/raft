#include "WatRaftState.h"

namespace WatRaft {

WatRaftState::WatRaftState() : raft_state(INIT) {
    pthread_mutex_init(&wait_on_state, NULL);
    pthread_cond_init(&state_change, NULL);
}

WatRaftState::~WatRaftState() {
    pthread_mutex_destroy(&wait_on_state);
    pthread_cond_destroy(&state_change);
}

void WatRaftState::change_state(State state) {
    pthread_mutex_lock(&wait_on_state);
    raft_state = state;
    pthread_cond_broadcast(&state_change);
    pthread_mutex_unlock(&wait_on_state);
}

void WatRaftState::wait_e(State state) {
    pthread_mutex_lock(&wait_on_state);
    while (raft_state != state) {
        pthread_cond_wait(&state_change, &wait_on_state);
    } 
    pthread_mutex_unlock(&wait_on_state);
}

void WatRaftState::wait_ge(State state) {
    pthread_mutex_lock(&wait_on_state);
    while (raft_state < state) {
        pthread_cond_wait(&state_change, &wait_on_state);
    }
    pthread_mutex_unlock(&wait_on_state);
}
} // namespace WatRaft

