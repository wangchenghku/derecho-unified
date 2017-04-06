#include <iostream>
#include <string>
#include <cstdlib>
#include <map>
#include <cstdint>
#include <vector>
#include <iostream>
#include <chrono>
#include <thread>

using namespace std;
using std::string;
using std::cin;
using std::cout;
using std::endl;
using std::map;

#include "rdmc/rdmc.h"
#include "rdmc/util.h"

#include "derecho/derecho.h"
#include "block_size.h"

void query_node_info(derecho::node_id_t &node_id, derecho::ip_addr &node_ip, derecho::ip_addr &leader_ip) {
    cout << "Please enter this node's ID: ";
    cin >> node_id;
    cout << "Please enter this node's IP address: ";
    cin >> node_ip;
    cout << "Please enter the leader node's IP address: ";
    cin >> leader_ip;
}

int main(int argc, char *argv[]) {
    if(argc < 2) {
        cout << "Error: Expected number of nodes in experiment as the first argument."
             << endl;
        return -1;
    }
    uint32_t num_nodes = std::atoi(argv[1]);
    unsigned int msg_size = std::atoi(argv[2]);
    derecho::node_id_t node_id;
    derecho::ip_addr my_ip;
    derecho::node_id_t leader_id = 0;
    derecho::ip_addr leader_ip;

    query_node_info(node_id, my_ip, leader_ip);

    long long unsigned int max_msg_size = msg_size;
    long long unsigned int block_size = msg_size * 2;
    unsigned int window_size = 3;
    bool done = false;
    int num_messages = 10;

    auto stability_callback = [&num_messages, &done, &num_nodes](
        int sender_rank, long long int index, char *buf,
        long long int msg_size) {
        cout << "Message size is: " << msg_size << endl;
	cout << "sender_rank = " << sender_rank << ", index = " << index << endl;
        if(index == num_messages - 1 && sender_rank == (int)num_nodes - 1) {
            done = true;
        }
    };

    derecho::CallbackSet callbacks{stability_callback, nullptr};
    derecho::DerechoParams param_object{max_msg_size, block_size, std::string(), window_size};
    rpc::Dispatcher<> empty_dispatcher(node_id);
    std::unique_ptr<derecho::Group<rpc::Dispatcher<>>> managed_group;
    if(node_id == leader_id) {
        managed_group = std::make_unique<derecho::Group<rpc::Dispatcher<>>>(
            my_ip, std::move(empty_dispatcher), callbacks, param_object);
    } else {
        managed_group = std::make_unique<derecho::Group<rpc::Dispatcher<>>>(
            node_id, my_ip, leader_id, leader_ip, std::move(empty_dispatcher), callbacks);
    }

    while(managed_group->get_members().size() < num_nodes) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    int my_rank = 0;
    auto group_members = managed_group->get_members();
    while(group_members[my_rank] != node_id) my_rank++;

    for(int i = 0; i < num_messages; ++i) {
        char *buf = managed_group->get_sendbuffer_ptr(0, 0, false, true);
        while(!buf) {
            buf = managed_group->get_sendbuffer_ptr(0, 0, false, true);
        }
        managed_group->send();
    }
    
    while(!done) {
    }
    exit(0);
}
