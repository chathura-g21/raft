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
}

WatRaftServer::~WatRaftServer() {
    printf("In destructor of WatRaftServer\n");
    delete rpc_server;
}

int WatRaftServer::wait() {
    wat_state.wait_ge(WatRaftState::SERVER_CREATED);
    // Perhaps perform your periodic tasks in this thread.
    ServerMap::const_iterator it = config->get_servers()->begin();
    for (; it != config->get_servers()->end(); it++) {
        if (it->first == node_id) {
            continue; // Skip itself
        }
        sleep(5); // Sleep
        // The following is an example of sending an echo message.
        boost::shared_ptr<TSocket> socket(
            new TSocket((it->second).ip, (it->second).port));
        boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
        boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
        WatRaftClient client(protocol);
        try {
            transport->open();
            std::string remote_str;
            client.debug_echo(remote_str, "Hello");
            transport->close();
            // Create a WatID object from the return value.
            std::cout << "Received: " << remote_str 
                      << " from " << (it->second).ip 
                      << ":" << (it->second).port << std::endl;
        } catch (TTransportException e) {
            printf("Caught exception: %s\n", e.what());
        }

    }
    pthread_join(rpc_thread, NULL);
    return 0;
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
