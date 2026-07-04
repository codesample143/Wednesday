#pragma once

#include <fstream>
#include <vector>
#include <mutex>

#include <grpcpp/grpcpp.h>
#include <openssl/sha.h>

#include "proto/filesystem.grpc.pb.h"

std::string sha256(const std::string& str) {
    unsigned char hash[SHA256_DIGEST_LENGTH];
    SHA256_CTX sha256;
    
    SHA256_Init(&sha256);
    SHA256_Update(&sha256, str.c_str(), str.size());
    SHA256_Final(hash, &sha256);
    
    std::stringstream ss;
    for(int i = 0; i < SHA256_DIGEST_LENGTH; i++){
        ss << std::hex << std::setw(2) << std::setfill('0') << (int)hash[i];
    }
    
    return ss.str();
}

class Wednesday final : public wednesday::StorageNodeService::Service{
    private:
        std::string port;
        std::string ip;
        std::string node_id;

        wednesday::NodeInfo successor;
        wednesday::NodeInfo predecessor;

        std::mutex mtx;

    public:
        Wednesday(const std::string& node_ip, const std::string& node_port, const std::string& id) {
            this->ip = node_ip;
            this->port = node_port;
            this->node_id = id;

            // Initialize successor to point to itself
            this->successor.set_node_id(id);
            this->successor.set_ip_address(node_ip);
            this->successor.set_port(std::stoi(node_port));

            // Initialize predecessor to point to itself
            this->predecessor.set_node_id(id);
            this->predecessor.set_ip_address(node_ip);
            this->predecessor.set_port(std::stoi(node_port));
        }
        grpc::Status ReadBlock(grpc::ServerContext* context, const wednesday::BlockReadRequest* request, wednesday::BlockReadResponse* response) override{
            std::string target_hash = sha256(request->file_path());
            bool owned = false;
            {
                std::lock_guard<std::mutex> lock(mtx);
                if (this->node_id == predecessor.node_id()){
                    owned = true; // Lonely node owns all hashes
                } else if (predecessor.node_id() < this->node_id) {
                    if (predecessor.node_id() < target_hash && target_hash <= this->node_id){
                        owned = true;
                    }
                } else {
                    if (predecessor.node_id() < target_hash || target_hash <= this->node_id){
                        owned = true;
                    }
                }
            }
                if(owned){
                    std::ifstream file(request->file_path(), std::ios::binary);
                    if (!file.is_open()){
                        response->set_success(false);
                        response->set_bytes_read(0);
                        return grpc::Status(grpc::StatusCode::NOT_FOUND, "File not found on this storage node");
                    }
                    file.seekg(request->offset(), std::ios::beg);

                    std::vector<char> buffer(request->block_size());
                    file.read(buffer.data(), request->block_size());
                    std::streamsize bytes_actual = file.gcount();
                    response->set_data(buffer.data(), bytes_actual);
                    response->set_bytes_read(static_cast<int32_t>(bytes_actual));   
                    response->set_success(true);
                    return grpc::Status::OK;
                } else {
                    std::string target_address = successor.ip_address() + ":" + std::to_string(successor.port());
                    auto channel = grpc::CreateChannel(target_address, grpc::InsecureChannelCredentials());
                    auto stub = wednesday::StorageNodeService::NewStub(channel);
                    
                    grpc::ClientContext client_context;
                    grpc::Status status = stub->ReadBlock(&client_context, *request, response);
                    return status;
                }
    
            return grpc::Status::OK;
        }
        
        grpc::Status WriteBlock(grpc::ServerContext* context, const wednesday::BlockWriteRequest* request, wednesday::BlockWriteResponse* response) override {
            std::string target_hash = sha256(request->file_path());
            bool owned = false;
            
            //Check topology under the lock
            {
                std::lock_guard<std::mutex> lock(mtx);
                if (this->node_id == predecessor.node_id()){
                    owned = true; 
                } else if (predecessor.node_id() < this->node_id) {
                    if (predecessor.node_id() < target_hash && target_hash <= this->node_id){
                        owned = true;
                    }
                } else {
                    if (predecessor.node_id() < target_hash || target_hash <= this->node_id){
                        owned = true;
                    }
                }
            }

            if (owned){
                std::ofstream file(request->file_path(), std::ios::binary | std::ios::in | std::ios::out);
                
                if (!file.is_open()){
                    file.clear();
                    file.open(request->file_path(), std::ios::binary | std::ios::out);
                }

                if (!file.is_open()) {
                    response->set_success(false);
                    return grpc::Status(grpc::StatusCode::INTERNAL, "Failed to open local file for writing");
                }
                
                file.seekp(request->offset(), std::ios::beg);
                file.write(request->data().data(), request->data().size());
                
                response->set_bytes_written(static_cast<int32_t>(request->data().size()));
                response->set_success(true);
                return grpc::Status::OK;
            } else {
                std::string target_address = successor.ip_address() + ":" + std::to_string(successor.port());
                auto channel = grpc::CreateChannel(target_address, grpc::InsecureChannelCredentials());
                auto stub = wednesday::StorageNodeService::NewStub(channel);
                
                grpc::ClientContext client_context;
                grpc::Status status = stub->WriteBlock(&client_context, *request, response);
                return status;
            }
        }
        grpc::Status JoinNetwork(grpc::ServerContext* context, const wednesday::NodeInfo* request, wednesday::NodeResponse* response) override{
            bool fits = false;
            {   
                std::lock_guard<std::mutex> lock(mtx);
                if (this->node_id == successor.node_id()){ // Lonely
                    fits = true;
                } else if (this->node_id < successor.node_id()){ // Standard 
                    if (this->node_id < request->node_id() && request->node_id() <= successor.node_id()) {
                        fits = true;
                    }
                } else { // Wrap-around
                    if (this->node_id < request->node_id() || request->node_id() <= successor.node_id()) {
                        fits = true;
                    }            
                };
                if (fits){
                    *response->mutable_successor() = this->successor; 
                    this->successor.set_node_id(request->node_id());
                    this->successor.set_ip_address(request->ip_address());
                    this->successor.set_port(request->port());
                    
                    return grpc::Status::OK;
                };
            }

            std::string target_address = successor.ip_address() + ":" + std::to_string(successor.port());
            auto channel = grpc::CreateChannel(target_address, grpc::InsecureChannelCredentials());
            auto stub = wednesday::StorageNodeService::NewStub(channel);
            
            grpc::ClientContext client_context;
            grpc::Status status = stub->JoinNetwork(&client_context, *request, response);
            return status;
        }

        grpc::Status NotifyNode(grpc::ServerContext* context, const wednesday::NodeInfo* request, wednesday::NotifyResponse* response) override{
            bool should_update = false;
            {
            std::lock_guard<std::mutex> lock(mtx);
                // Lonely
                if (this->predecessor.node_id() == this->node_id) {
                    should_update = true;
                } 
                // Normal
                else if (this->predecessor.node_id() < this->node_id) {
                    if (this->predecessor.node_id() < request->node_id() && request->node_id() < this->node_id) {
                        should_update = true;
                    }
                } 
                // Wrap around
                else {
                    if (this->predecessor.node_id() < request->node_id() || request->node_id() < this->node_id) {
                        should_update = true;
                    }
                }
                // If closer, update predecessor
                if (should_update) {
                    this->predecessor.set_node_id(request->node_id());
                    this->predecessor.set_ip_address(request->ip_address());
                    this->predecessor.set_port(request->port());
                }
            }
            response->set_success(true);
            return grpc::Status::OK;
        }
};