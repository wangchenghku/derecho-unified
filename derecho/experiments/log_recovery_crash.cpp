/**
 * @file log_recovery_crash.cpp
 * A test that runs a Derecho group in persistence mode for a while, then has
 * one member exit prematurely while the others keep sending. The "crashed"
 * member should then run log_recovery_restart after running the recovery
 * helper script.
 */

#include <stddef.h>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <iostream>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "derecho/logger.h"
#include "derecho/managed_group.h"

using namespace std;

const int GMS_PORT = 12345;
const size_t message_size = 1000;
const size_t block_size = 1000;

uint32_t num_nodes, node_id;
map<uint32_t, std::string> node_addresses;

const int num_messages = 1000;
bool done = false;
shared_ptr<derecho::ManagedGroup<Dispatcher<>>> managed_group;

void stability_callback(int sender_id, long long int index, char* data, long long int size) {
    using namespace derecho;
    util::debug_log().log_event(stringstream() << "Global stability for message "
            << index << " from sender " << sender_id);
}

void persistence_callback(int sender_id, long long int index, char* data, long long int size) {
    using namespace derecho;
    util::debug_log().log_event(stringstream() << "Persistence complete for message "
            << index << " from sender " << sender_id);
    if(index == num_messages - 1 && sender_id == (int)num_nodes - 1) {
        cout << "Done" << endl;
        done = true;
    }
}

void send_messages(int count) {
    for(int i = 0; i < count; ++i) {
        char* buffer = managed_group->get_sendbuffer_ptr(message_size);
        while(!buffer) {
            buffer = managed_group->get_sendbuffer_ptr(message_size);
        }
        memset(buffer, rand() % 256, message_size);
        managed_group->send();
    }
}

void query_node_info(derecho::node_id_t& node_id, derecho::ip_addr& node_ip, derecho::ip_addr& leader_ip) {
     cout << "Please enter this node's ID: ";
     cin >> node_id;
     cout << "Please enter this node's IP address: ";
     cin >> node_ip;
     cout << "Please enter the leader node's IP address: ";
     cin >> leader_ip;
}

int main(int argc, char* argv[]) {
    srand(time(nullptr));
    if(argc < 2) {
        cout << "Error: Expected number of nodes in experiment as the first argument."
                << endl;
        return -1;
    }
    num_nodes = std::atoi(argv[1]);
    derecho::ip_addr my_ip;
    derecho::node_id_t leader_id = 0;
    derecho::ip_addr leader_ip;

    query_node_info(node_id, my_ip, leader_ip);

    string debug_log_filename = (std::stringstream() << "events_node" << node_id << ".csv").str();
    string message_log_filename = (std::stringstream() << "data" << node_id << ".dat").str();

    derecho::CallbackSet callbacks{stability_callback, persistence_callback};
    derecho::DerechoParams param_object{message_size, block_size, message_log_filename};
    if(node_id == leader_id) {
        managed_group = make_shared<derecho::ManagedGroup<Dispatcher<>>>(
                my_ip, Dispatcher<>(node_id), callbacks, param_object);
    } else {
        managed_group = make_shared<derecho::ManagedGroup<Dispatcher<>>>(
                node_id, my_ip, leader_id, leader_ip, Dispatcher<>(node_id), callbacks);
    }
    cout << "Created group, waiting for others to join." << endl;
    while(managed_group->get_members().size() < (num_nodes - 1)) {
        std::this_thread::sleep_for(1ms);
    }
    cout << "Starting to send messages." << endl;
    //Node n-1 will "crash" before sending all the messages
    if(node_id == num_nodes - 1) {
        send_messages(num_messages-250);
        managed_group->log_event("About to exit");
        ofstream log_stream(debug_log_filename);
        managed_group->print_log(log_stream);
        return 0;
    } else {
        send_messages(num_messages);
        while(!done) {
        }
        ofstream log_stream(debug_log_filename);
        managed_group->print_log(log_stream);
    }
}
