#include <iostream>
#include <memory>
#include <string>
#include <thread>

#include <grpcpp/grpcpp.h>
#include "Wednesday.hpp" 

void JoinNetworkTask(const std::string& target_bootstrap_address, const std::string& my_ip, int my_port, const std::string& my_id) {
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    std::cout << "[Join] Attempting to join network via bootstrap node: " << target_bootstrap_address << std::endl;

    auto channel = grpc::CreateChannel(target_bootstrap_address, grpc::InsecureChannelCredentials());
    auto stub = wednesday::StorageNodeService::NewStub(channel);

    wednesday::NodeInfo my_info;
    my_info.set_node_id(my_id);
    my_info.set_ip_address(my_ip);
    my_info.set_port(my_port);

    wednesday::NodeResponse response;
    grpc::ClientContext context;

    grpc::Status status = stub->JoinNetwork(&context, my_info, &response);

    if (status.ok()){
        std::cout << "[Join] Successfully contacted network! Assigned Successor: " 
                  << response.successor().node_id() << " @ " 
                  << response.successor().ip_address() << ":" << response.successor().port() << std::endl; //ai generated print statement
        
        std::string succ_address = response.successor().ip_address() + ":" + std::to_string(response.successor().port());
        auto succ_channel = grpc::CreateChannel(succ_address, grpc::InsecureChannelCredentials());
        auto succ_stub = wednesday::StorageNodeService::NewStub(succ_channel);

        wednesday::NotifyResponse notify_resp;
        grpc::ClientContext notify_context;
        
        grpc::Status notify_status = succ_stub->NotifyNode(&notify_context, my_info, &notify_resp);
        if (notify_status.ok()){
            std::cout << "[Join] Successor successfully notified and linked counter-clockwise." << std::endl;
        } else {
            std::cerr << "[Join Error] Failed to notify successor: " << notify_status.error_message() << std::endl;
        }

    } else {
        std::cerr << "[Join Error] Critical routing failure connecting to bootstrap node: " 
                  << status.error_message() << std::endl;
    }
}

int main(int argc, char** argv) {
    std::string ip = "127.0.0.1";
    int port = 50051;
    std::string bootstrap_address = ""; //first node is genesis node

    // Basic CLI argument parsing: ./server [port] [bootstrap_ip:port]
    if (argc >= 2) {
        port = std::stoi(argv[1]);
    }
    if (argc >= 3) {
        bootstrap_address = argv[2];
    }

    std::string binding_address = ip + ":" + std::to_string(port);
    std::string my_id = sha256(binding_address);

    std::cout << "Starting Wednesday Storage Node..." << std::endl;
    std::cout << "Address: " << binding_address << std::endl;
    std::cout << "Node ID: " << my_id << std::endl;

    Wednesday service(ip, std::to_string(port), my_id);

    grpc::ServerBuilder builder;
    builder.AddListeningPort(binding_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << binding_address << std::endl;

    // This avoids platform-dependent runtime crashes for uninitialized threads.
    std::unique_ptr<std::thread> join_thread;
    if (!bootstrap_address.empty()) {
        join_thread = std::make_unique<std::thread>(JoinNetworkTask, bootstrap_address, ip, port, my_id);
    } else {
        std::cout << "[Mode] No bootstrap address given. Acting as Genesis ring node." << std::endl;
    }
    server->Wait();
    if (join_thread && join_thread->joinable()) {
        join_thread->join();
    }

    return 0;
}