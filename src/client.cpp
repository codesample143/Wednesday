#include <iostream>
#include <memory>
#include <string>

#include <grpcpp/grpcpp.h>
#include "proto/filesystem.grpc.pb.h"

int main() {
    std::cout << "Wednesday DHT Client Test Tool" << std::endl;

    std::string node_address = "127.0.0.1:50051";
    std::cout << "[Client] Connecting to node at: " << node_address << std::endl;
    
    auto channel = grpc::CreateChannel(node_address, grpc::InsecureChannelCredentials());
    auto stub = wednesday::StorageNodeService::NewStub(channel);

    std::string test_file = "dht_test_file.txt";
    std::string test_data = "Distributed storage is working flawlessly!";
    std::cout << "\n[Test 1] Sending WriteBlock request for: " << test_file << std::endl;
    
    wednesday::BlockWriteRequest write_req;
    write_req.set_file_path(test_file);
    write_req.set_offset(0);
    write_req.set_data(test_data);

    wednesday::BlockWriteResponse write_resp;
    grpc::ClientContext write_context;

    grpc::Status write_status = stub->WriteBlock(&write_context, write_req, &write_resp);

    if (write_status.ok() && write_resp.success()) {
        std::cout << ">>> SUCCESS: Written " << write_resp.bytes_written() << " bytes!" << std::endl;
        std::cout << ">>> Note: Check your local folder. The file will only appear on the node that owns the hash!" << std::endl;
    } else {
        std::cerr << ">>> WRITE FAILED: " << write_status.error_message() << std::endl;
        return 1;
    }
    //read
    std::string fallback_read_node = "127.0.0.1:50053";
    std::cout << "\n[Test 2] Connecting to a DIFFERENT node to Read (" << fallback_read_node << ")" << std::endl;
    std::cout << "[Client] Querying for: " << test_file << std::endl;

    auto channel_read = grpc::CreateChannel(fallback_read_node, grpc::InsecureChannelCredentials());
    auto stub_read = wednesday::StorageNodeService::NewStub(channel_read);

    wednesday::BlockReadRequest read_req;
    read_req.set_file_path(test_file);
    read_req.set_offset(0);
    read_req.set_block_size(static_cast<int32_t>(test_data.size()));

    wednesday::BlockReadResponse read_resp;
    grpc::ClientContext read_context;

    grpc::Status read_status = stub_read->ReadBlock(&read_context, read_req, &read_resp);

    if (read_status.ok() && read_resp.success()) {
        std::cout << ">>> SUCCESS! Recovered Data from the DHT network:" << std::endl;
        std::cout << read_resp.data() << std::endl;
        std::cout << "Read status: " << read_resp.bytes_read() << " bytes successfully retrieved." << std::endl;
    } else {
        std::cerr << ">>> READ FAILED: " << read_status.error_message() << std::endl;
        std::cout << ">>> (Did you remember to start the server on port 50053 and have it join the ring?)" << std::endl;
    }

    return 0;
}