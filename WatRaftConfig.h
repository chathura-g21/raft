#ifndef _WAT_RAFT_CONFIG_H_
#define _WAT_RAFT_CONFIG_H_

#include <stdio.h>
#include <fstream>
#include <iostream>
#include <sstream>
#include <map>

namespace WatRaft {

struct IPPortPair {
    std::string ip;
    int port;
};

typedef std::map<int, struct IPPortPair> ServerMap;

class WatRaftConfig {
  private:
    ServerMap servers;

  public:
    WatRaftConfig() {}
    ~WatRaftConfig() {}
    bool parse(const std::string& filename);
    const ServerMap* get_servers() const { return &servers; }
};
} // namespace WatRaft
#endif
