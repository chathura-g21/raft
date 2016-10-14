#include "WatRaftConfig.h"

using namespace std;

namespace WatRaft {

bool WatRaftConfig::parse(const string& filename) {
    fstream fs;
    fs.open(filename.c_str(), fstream::in);
    if (!fs) {
        return false;
    }
    string line;
    while (getline(fs, line)) {
        stringstream ss(line);
        int node_id;
        struct IPPortPair pair;
        ss >> node_id >> pair.ip >> pair.port;
        if (ss) {
            cout << "Node ID: " << node_id
                 << " IP: "     << pair.ip 
                 << " Port: "   << pair.port << endl; 
            servers[node_id] = pair;
        } else {
            cerr << "Error parsing: " << line << std::endl;
        }
    }
    fs.close();
    return true;
}
} // namespace WatRaft

