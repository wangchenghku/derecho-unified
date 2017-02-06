#pragma once

#include <algorithm>
#include <cassert>
#include <chrono>
#include <limits>
#include <thread>

#include "derecho_group.h"
#include "logger.h"

namespace derecho {

using std::chrono::milliseconds;

/**
 * Helper function to find the index of an element in a container.
 */
template <class T, class U>
size_t index_of(T container, U elem) {
    size_t n = 0;
    for(auto it = begin(container); it != end(container); ++it) {
        if(*it == elem) return n;

        n++;
    }
    return container.size();
}

/**
 *
 * @param _members A list of node IDs of members in this group
 * @param my_node_id The rank (ID) of this node in the group
 * @param _sst The SST this group will use; created by the GMS (membership
 * service) for this group.
 * @param _free_message_buffers Message buffers to use for RDMC sending/receiving
 * (there must be one for each sender in the group)
 * @param _max_payload_size The size of the largest possible message that will
 * be sent in this group, in bytes
 * @param _callbacks A set of functions to call when messages have reached
 * various levels of stability
 * @param _block_size The block size to use for RDMC
 * @param _window_size The window size (number of outstanding messages that can
 * be in progress at once before blocking sends) to use when sending a stream
 * of messages to the group; default is 3
 * @param timeout_ms The time that this node will wait for a sender in the group
 * to send its message before concluding that the sender has failed; default is 1ms
 * @param _type The type of RDMC algorithm to use; default is BINOMIAL_SEND
 * @param filename If provided, the name of the file in which to save persistent
 * copies of all messages received. If an empty filename is given (the default),
 * the node runs in non-persistent mode and no persistence callbacks will be
 * issued.
 */
// constructor 1
template <typename dispatchersType>
DerechoGroup<dispatchersType>::DerechoGroup(
    std::vector<node_id_t> _members, node_id_t my_node_id,
    std::shared_ptr<DerechoSST> _sst,
    std::map<uint32_t, std::vector<MessageBuffer>>& _free_message_buffers,
    dispatchersType _dispatchers,
    CallbackSet callbacks,
    SubgroupInfo subgroup_info,
    const DerechoParams derecho_params,
    std::map<node_id_t, std::string> ip_addrs,
    std::vector<char> already_failed)
        : members(_members),
          num_members(members.size()),
          member_index(index_of(members, my_node_id)),
          block_size(derecho_params.block_size),
          max_msg_size(compute_max_msg_size(derecho_params.max_payload_size, derecho_params.block_size)),
          type(derecho_params.type),
          window_size(derecho_params.window_size),
          callbacks(callbacks),
          subgroup_info(subgroup_info),
          dispatchers(std::move(_dispatchers)),
          connections(my_node_id, ip_addrs, derecho_params.rpc_port),
          rdmc_group_num_offset(0),
          future_message_indices(subgroup_info.num_subgroups(num_members), 0),
          next_sends(subgroup_info.num_subgroups(num_members)),
          pending_sends(subgroup_info.num_subgroups(num_members)),
          next_message_to_deliver(subgroup_info.num_subgroups(num_members)),
          sender_timeout(derecho_params.timeout_ms),
          sst(_sst) {
    assert(window_size >= 1);

    if(!derecho_params.filename.empty()) {
        file_writer = std::make_unique<FileWriter>(make_file_written_callback(),
                                                   derecho_params.filename);
    }

    for(uint i = 0; i < num_members; ++i) {
        node_id_to_sst_index[members[i]] = i;
    }

    auto num_subgroups = subgroup_info.num_subgroups(num_members);
    for(uint i = 0; i < num_subgroups; ++i) {
        uint32_t num_shards = subgroup_info.num_shards(num_members, i);
        for(uint j = 0; j < num_shards; ++j) {
            auto shard_members = subgroup_info.subgroup_membership(num_members, i, j);
            // check if the node belongs to the shard
            auto it = std::find(shard_members.begin(), shard_members.end(), members[member_index]);
            if(it != shard_members.end()) {
                subgroup_to_shard_n_index[i] = {j, std::distance(shard_members.begin(), it)};
            }
        }
    }

    free_message_buffers.swap(_free_message_buffers);
    for(const auto p : subgroup_to_shard_n_index) {
        auto num_shard_members = subgroup_info.subgroup_membership(num_members, p.first, p.second.first).size();
        while(free_message_buffers[p.first].size() < window_size * num_shard_members) {
            free_message_buffers[p.first].emplace_back(max_msg_size);
        }
    }

    p2pBuffer = std::unique_ptr<char[]>(new char[derecho_params.max_payload_size]);
    deliveryBuffer = std::unique_ptr<char[]>(new char[derecho_params.max_payload_size]);

    initialize_sst_row();
    bool no_member_failed = true;
    if(already_failed.size()) {
        for(uint i = 0; i < num_members; ++i) {
            if(already_failed[i]) {
                no_member_failed = false;
                break;
            }
        }
    }
    if(!already_failed.size() || no_member_failed) {
        // if groups are created successfully, rdmc_groups_created will be set
        // to true
        rdmc_groups_created = create_rdmc_groups();
    }
    register_predicates();
    sender_thread = std::thread(&DerechoGroup::send_loop, this);
    timeout_thread = std::thread(&DerechoGroup::check_failures_loop, this);
    rpc_thread = std::thread(&DerechoGroup::rpc_process_loop, this);
    //    cout << "DerechoGroup: Registered predicates and started thread" <<
    //    std::endl;
}

// constructor 2
template <typename dispatchersType>
DerechoGroup<dispatchersType>::DerechoGroup(
    std::vector<node_id_t> _members, node_id_t my_node_id,
    std::shared_ptr<DerechoSST> _sst,
    DerechoGroup&& old_group, std::map<node_id_t, std::string> ip_addrs,
    std::vector<char> already_failed, uint32_t rpc_port)
        : members(_members),
          num_members(members.size()),
          member_index(index_of(members, my_node_id)),
          block_size(old_group.block_size),
          max_msg_size(old_group.max_msg_size),
          type(old_group.type),
          window_size(old_group.window_size),
          callbacks(old_group.callbacks),
          subgroup_info(old_group.subgroup_info),
          dispatchers(std::move(old_group.dispatchers)),
          connections(my_node_id, ip_addrs, rpc_port),
          toFulfillQueue(std::move(old_group.toFulfillQueue)),
          fulfilledList(std::move(old_group.fulfilledList)),
          rdmc_group_num_offset(old_group.rdmc_group_num_offset),
          future_message_indices(subgroup_info.num_subgroups(num_members), 0),
          next_sends(subgroup_info.num_subgroups(num_members)),
          pending_sends(subgroup_info.num_subgroups(num_members)),
          next_message_to_deliver(subgroup_info.num_subgroups(num_members)),
          sender_timeout(old_group.sender_timeout),
          sst(_sst) {
    // Make sure rdmc_group_num_offset didn't overflow.
    assert(old_group.rdmc_group_num_offset <=
           std::numeric_limits<uint16_t>::max() - old_group.num_members -
               num_members);

    // Just in case
    old_group.wedge();

    for(uint i = 0; i < num_members; ++i) {
        node_id_to_sst_index[members[i]] = i;
    }

    // Convience function that takes a msg from the old group and
    // produces one suitable for this group.
    auto convert_msg = [this](Message& msg, uint32_t subgroup_num) {
        msg.sender_rank = member_index;
        msg.index = future_message_indices[subgroup_num]++;

        header* h = (header*)msg.message_buffer.buffer.get();
        future_message_indices[subgroup_num] += h->pause_sending_turns;

        return std::move(msg);
    };

    auto num_subgroups = subgroup_info.num_subgroups(num_members);
    for(uint i = 0; i < num_subgroups; ++i) {
        uint32_t num_shards = subgroup_info.num_shards(num_members, i);
        for(uint j = 0; j < num_shards; ++j) {
            auto shard_members = subgroup_info.subgroup_membership(num_members, i, j);
            // check if the node belongs to the shard
            auto it = std::find(shard_members.begin(), shard_members.end(), members[member_index]);
            if(it != shard_members.end()) {
                subgroup_to_shard_n_index[i] = {j, std::distance(shard_members.begin(), it)};
            }
        }
    }

    for(const auto p : subgroup_to_shard_n_index) {
        auto num_shard_members = subgroup_info.subgroup_membership(num_members, p.first, p.second.first).size();
        while(free_message_buffers[p.first].size() < window_size * num_shard_members) {
            free_message_buffers[p.first].emplace_back(max_msg_size);
        }
    }

    // Reclaim MessageBuffers from the old group, and supplement them with
    // additional if the group has grown.
    std::lock_guard<std::mutex> lock(old_group.msg_state_mtx);
    for(const auto p : subgroup_to_shard_n_index) {
        const auto subgroup_num = p.first, shard_num = p.second.first;
        const auto num_shard_members = subgroup_info.subgroup_membership(num_members, subgroup_num, shard_num).size();
        // for later: don't move extra message buffers
        free_message_buffers[subgroup_num].swap(old_group.free_message_buffers[subgroup_num]);
        while(free_message_buffers[subgroup_num].size() < old_group.window_size * num_shard_members) {
            free_message_buffers[subgroup_num].emplace_back(max_msg_size);
        }
    }

    for(auto& msg : old_group.current_receives) {
        free_message_buffers[msg.first.first].push_back(std::move(msg.second.message_buffer));
    }
    old_group.current_receives.clear();
    p2pBuffer = std::move(old_group.p2pBuffer);
    deliveryBuffer = std::move(old_group.deliveryBuffer);

    // Assume that any locally stable messages failed. If we were the sender
    // then re-attempt, otherwise discard. TODO: Presumably the ragged edge
    // cleanup will want the chance to deliver some of these.
    for(auto& p : old_group.locally_stable_messages) {
        if(p.second.size() == 0) {
            continue;
        }

        for(auto& q : p.second) {
            if(q.second.sender_rank == old_group.member_index) {
                pending_sends[p.first].push(convert_msg(q.second, p.first));
            } else {
                free_message_buffers[p.first].push_back(std::move(q.second.message_buffer));
            }
        }
    }
    old_group.locally_stable_messages.clear();

    // Any messages that were being sent should be re-attempted.
    for(auto p : subgroup_to_shard_n_index) {
        auto subgroup_num = p.first;
        if(old_group.current_sends[subgroup_num]) {
            pending_sends[subgroup_num].push(convert_msg(*old_group.current_sends[subgroup_num], subgroup_num));
        }

        while(!old_group.pending_sends[subgroup_num].empty()) {
            pending_sends[subgroup_num].push(convert_msg(old_group.pending_sends[subgroup_num].front(), subgroup_num));
            old_group.pending_sends[subgroup_num].pop();
        }

        if(old_group.next_sends[subgroup_num]) {
            next_sends[subgroup_num] = convert_msg(*old_group.next_sends[subgroup_num], subgroup_num);
        }

        for(auto& entry : old_group.non_persistent_messages[subgroup_num]) {
            non_persistent_messages[subgroup_num].emplace(entry.first,
                                                          convert_msg(entry.second, subgroup_num));
        }
        old_group.non_persistent_messages.clear();
    }

    // If the old group was using persistence, we should transfer its state to the new group
    file_writer = std::move(old_group.file_writer);
    if(file_writer) {
        file_writer->set_message_written_upcall(make_file_written_callback());
    }
    initialize_sst_row();
    bool no_member_failed = true;
    if(already_failed.size()) {
        for(uint i = 0; i < num_members; ++i) {
            if(already_failed[i]) {
                no_member_failed = false;
                break;
            }
        }
    }
    if(!already_failed.size() || no_member_failed) {
        // if groups are created successfully, rdmc_groups_created will be set
        // to true
        rdmc_groups_created = create_rdmc_groups();
    }
    register_predicates();
    sender_thread = std::thread(&DerechoGroup::send_loop, this);
    timeout_thread = std::thread(&DerechoGroup::check_failures_loop, this);
    rpc_thread = std::thread(&DerechoGroup::rpc_process_loop, this);
    //    cout << "DerechoGroup: Registered predicates and started thread" <<
    //    std::endl;
}

template <typename handlersType>
std::function<void(persistence::message)> DerechoGroup<handlersType>::make_file_written_callback() {
    return [this](persistence::message m) {
        //m.sender is an ID, not a rank
        uint sender_rank;
        for(sender_rank = 0; sender_rank < num_members; ++sender_rank) {
            if(members[sender_rank] == m.sender) break;
        }
        callbacks.local_persistence_callback(sender_rank, m.index, m.data,
                                             m.length);

        // m.data points to the char[] buffer in a MessageBuffer, so we need to find
        // the msg corresponding to m and put its MessageBuffer on free_message_buffers
        auto sequence_number = m.index * num_members + sender_rank;
        {
            std::lock_guard<std::mutex> lock(msg_state_mtx);
            auto find_result = non_persistent_messages[m.subgroup_num].find(sequence_number);
            assert(find_result != non_persistent_messages[m.subgroup_num].end());
            Message &m_msg = find_result->second;
            free_message_buffers[m.subgroup_num].push_back(std::move(m_msg.message_buffer));
            non_persistent_messages[m.subgroup_num].erase(find_result);
            sst->persisted_num[member_index][m.subgroup_num] = sequence_number;
            sst->put(get_shard_sst_indices(m.subgroup_num), (char*)std::addressof(sst->persisted_num[0][m.subgroup_num]) - sst->getBaseAddress(), sizeof(long long int));
        }

    };
}

template <typename dispatchersType>
bool DerechoGroup<dispatchersType>::create_rdmc_groups() {
    auto num_subgroups = subgroup_info.num_subgroups(num_members);
    uint32_t subgroup_offset = 0;
    for(uint i = 0; i < num_subgroups; ++i) {
        uint32_t num_shards = subgroup_info.num_shards(num_members, i);
        uint32_t max_shard_members = 0;
        for(uint j = 0; j < num_shards; ++j) {
            auto shard_members = subgroup_info.subgroup_membership(num_members, i, j);
            auto num_shard_members = shard_members.size();
            if(max_shard_members < num_shard_members) {
                max_shard_members = num_shard_members;
            }
            // check if the node belongs to the shard
            if(std::find(shard_members.begin(), shard_members.end(), members[member_index]) != shard_members.end()) {
                for(uint k = 0; k < num_shard_members; ++k) {
                    auto node_id = shard_members[k];
                    subgroup_to_nReceived_offset[i] = subgroup_offset + k;
                    // When RDMC receives a message, it should store it in
                    // locally_stable_messages and update the received count
                    auto rdmc_receive_handler = [this, i, j, k, node_id, num_shard_members, subgroup_offset](char* data, size_t size) {
		      assert(this->sst);
		      util::debug_log().log_event(std::stringstream() << "Locally received message in subgroup " << i << ", shard " << j << ", sender rank " << k << ", index " << (sst->nReceived[member_index][subgroup_offset + k] + 1));
		      std::lock_guard<std::mutex> lock(msg_state_mtx);
		      header *h = (header *)data;
		      sst->nReceived[member_index][subgroup_offset + k]++;

		      long long int index = sst->nReceived[member_index][subgroup_offset + k];
		      long long int sequence_number = index * num_shard_members + k;

		      // Move message from current_receives to locally_stable_messages.
		      if(node_id == members[member_index]) {
			assert(current_sends[i]);
			locally_stable_messages[i][sequence_number] =
			  std::move(*current_sends[i]);
			current_sends[i] = std::experimental::nullopt;
		      } else {
			auto it = current_receives.find({i, sequence_number});
			assert(it != current_receives.end());
			auto& message = it->second;
			locally_stable_messages[i].emplace(sequence_number, std::move(message));
			current_receives.erase(it);
		      }
		      // Add empty messages to locally_stable_messages for each turn that the sender is skipping.
		      for(unsigned int j = 0; j < h->pause_sending_turns; ++j) {
			index++;
			sequence_number += num_shard_members;
			sst->nReceived[member_index][subgroup_offset + k]++;
			locally_stable_messages[i][sequence_number] = {(int)k, index, 0, 0};
		      }
		      
		      std::atomic_signal_fence(std::memory_order_acq_rel);
		      auto* min_ptr = std::min_element(&sst->nReceived[member_index][subgroup_offset],
						       &sst->nReceived[member_index][subgroup_offset + num_shard_members]);
		      uint min_index = std::distance(&sst->nReceived[member_index][subgroup_offset], min_ptr);
		      auto new_seq_num = (*min_ptr + 1) * num_shard_members + min_index - 1;
		      if((int)new_seq_num > sst->seq_num[member_index][i]) {
			util::debug_log().log_event(std::stringstream() << "Updating seq_num for subgroup " << i << " to "  << new_seq_num);
			sst->seq_num[member_index][i] = new_seq_num;
			std::atomic_signal_fence(std::memory_order_acq_rel);
			sst->put(get_shard_sst_indices(i), (char*)std::addressof(sst->seq_num[0][i]) - sst->getBaseAddress(), sizeof(long long int));
			sst->put(get_shard_sst_indices(i), (char*)std::addressof(sst->nReceived[0][subgroup_offset + k]) - sst->getBaseAddress(), sizeof(long long int));
              } else {
			sst->put(get_shard_sst_indices(i), (char*)std::addressof(sst->nReceived[0][subgroup_offset + k]) - sst->getBaseAddress(), sizeof(long long int));
              }
                    };
                    // Capture rdmc_receive_handler by copy! The reference to it won't be valid
                    // after this constructor ends!
                    auto receive_handler_plus_notify =
                        [this, rdmc_receive_handler](char* data, size_t size) {
		      rdmc_receive_handler(data, size);
		      // signal background writer thread
		      sender_cv.notify_all();
                        };

                    std::vector<uint32_t> rotated_shard_members(shard_members.size());
                    for(uint l = 0; l < num_shard_members; ++l) {
                        rotated_shard_members[l] = members[(k + l) % num_shard_members];
                    }
                    if(node_id == members[member_index]) {
                        if(!rdmc::create_group(
                               rdmc_group_num_offset, shard_members, block_size, type,
                               [this](size_t length) -> rdmc::receive_destination {
			       assert(false);
			       return {nullptr, 0};
                               },
                               receive_handler_plus_notify,
                               [](boost::optional<uint32_t>) {})) {
                            return false;
                        }
                        subgroup_to_rdmc_group[i] = rdmc_group_num_offset;
                        rdmc_group_num_offset++;
                    } else {
                        if(!rdmc::create_group(
                               rdmc_group_num_offset, shard_members, block_size, type,
                               [this, i, j, k, num_shard_members, subgroup_offset](size_t length) -> rdmc::receive_destination {
                       std::lock_guard<std::mutex> lock(msg_state_mtx);
                       assert(!free_message_buffers[i].empty());

                       Message msg;
                       msg.sender_rank = k;
                       msg.index = sst->nReceived[member_index][subgroup_offset + k] + 1;
                       msg.size = length;
                       msg.message_buffer = std::move(free_message_buffers[i].back());
                       free_message_buffers[i].pop_back();

                       rdmc::receive_destination ret{msg.message_buffer.mr, 0};
                       auto sequence_number = msg.index * num_shard_members + k;
                       current_receives[{i, sequence_number}] = std::move(msg);

                       assert(ret.mr->buffer != nullptr);
                       return ret;
                               },
                               rdmc_receive_handler, [](boost::optional<uint32_t>) {})) {
                            return false;
                        }
                        rdmc_group_num_offset++;
                    }
                }
            }
        }
        subgroup_offset += max_shard_members;
    }
    // rotated list of members - used for creating n internal RDMC groups
    std::vector<uint32_t> rotated_members(num_members);

    std::cout << "The members are" << std::endl;
    for(uint i = 0; i < num_members; ++i) {
        std::cout << members[i] << " ";
    }
    std::cout << std::endl;
    return true;
}

template <typename dispatchersType>
void DerechoGroup<dispatchersType>::initialize_sst_row() {
    auto nReceived_size = sst->nReceived.size();
    auto seq_num_size = sst->seq_num.size();
    for(uint i = 0; i < num_members; ++i) {
        for(uint j = 0; j < nReceived_size; ++j) {
            sst->nReceived[i][j] = -1;
        }
        for(uint j = 0; j < seq_num_size; ++j) {
            sst->seq_num[i][j] = -1;
            sst->stable_num[i][j] = -1;
            sst->delivered_num[i][j] = -1;
            sst->persisted_num[i][j] = -1;
        }
    }

    sst->put();
    sst->sync_with_members();
}

template <typename dispatchersType>
void DerechoGroup<dispatchersType>::deliver_message(Message& msg, uint32_t subgroup_num) {
    if(msg.size > 0) {
        char* buf = msg.message_buffer.buffer.get();
        header* h = (header*)(buf);
        // cooked send
        if(h->cooked_send) {
            buf += h->header_size;
            auto payload_size = msg.size - h->header_size;
            // extract the destination vector
            size_t dest_size = ((size_t*)buf)[0];
            buf += sizeof(size_t);
            payload_size -= sizeof(size_t);
            bool in_dest = false;
            for(size_t i = 0; i < dest_size; ++i) {
                auto n = ((node_id_t*)buf)[0];
                buf += sizeof(node_id_t);
                payload_size -= sizeof(node_id_t);
                if(n == members[member_index]) {
                    in_dest = true;
                }
            }
            if(in_dest || dest_size == 0) {
                auto max_payload_size = max_msg_size - sizeof(header);
                size_t reply_size = 0;
                dispatchers.handle_receive(
                    buf, payload_size, [this, &reply_size, &max_payload_size](size_t size) -> char* {
                        reply_size = size;
                        if(reply_size <= max_payload_size) {
                            return deliveryBuffer.get();
                        } else {
                            return nullptr;
                        }
                    });
                if(reply_size > 0) {
                    node_id_t id = members[msg.sender_rank];
                    if(id == members[member_index]) {
                        dispatchers.handle_receive(
                            deliveryBuffer.get(), reply_size,
                            [](size_t size) -> char* { assert(false); });
                        if(dest_size == 0) {
                            std::lock_guard<std::mutex> lock(pending_results_mutex);
                            toFulfillQueue.front()->fulfill_map(members);
                            fulfilledList.push_back(std::move(toFulfillQueue.front()));
                            toFulfillQueue.pop();
                        }
                    } else {
                        connections.write(id, deliveryBuffer.get(),
                                          reply_size);
                    }
                }
            }
        }
        // raw send
        else {
            callbacks.global_stability_callback(msg.sender_rank, msg.index,
                                                buf + h->header_size, msg.size);
        }
        if(file_writer) {
            // msg.sender_rank is the 0-indexed rank within this group, but
            // persistence::message needs the sender's globally unique ID
            persistence::message msg_for_filewriter{buf + h->header_size,
                                                    msg.size, (uint32_t)sst->vid[member_index],
                                                    members[msg.sender_rank], (uint64_t)msg.index,
                                                    h->cooked_send};
            auto sequence_number = msg.index * num_members + msg.sender_rank;
            non_persistent_messages[subgroup_num].emplace(sequence_number, std::move(msg));
            file_writer->write_message(msg_for_filewriter);
        } else {
            free_message_buffers[subgroup_num].push_back(std::move(msg.message_buffer));
        }
    }
}

template <typename dispatchersType>
void DerechoGroup<dispatchersType>::deliver_messages_upto(
    const std::vector<long long int>& max_indices_for_senders, uint32_t subgroup_num, uint32_t num_shard_members) {
    assert(max_indices_for_senders.size() == (size_t)num_shard_members);
    std::lock_guard<std::mutex> lock(msg_state_mtx);
    auto curr_seq_num = sst->delivered_num[member_index][subgroup_num];
    auto max_seq_num = curr_seq_num;
    for(uint sender = 0; sender < max_indices_for_senders.size();
        sender++) {
        max_seq_num =
            std::max(max_seq_num,
                     max_indices_for_senders[sender] * num_shard_members + sender);
    }
    for(auto seq_num = curr_seq_num; seq_num <= max_seq_num; seq_num++) {
        auto msg_ptr = locally_stable_messages[subgroup_num].find(seq_num);
        if(msg_ptr != locally_stable_messages[subgroup_num].end()) {
            deliver_message(msg_ptr->second, subgroup_num);
            locally_stable_messages[subgroup_num].erase(msg_ptr);
        }
    }
}

template <typename dispatchersType>
void DerechoGroup<dispatchersType>::register_predicates() {
    for(const auto p : subgroup_to_shard_n_index) {
        uint32_t subgroup_num = p.first;
        uint32_t shard_num = p.second.first;
        uint32_t shard_index = p.second.second;
        auto shard_members = subgroup_info.subgroup_membership(num_members, subgroup_num, shard_num);
        auto num_shard_members = shard_members.size();
        auto stability_pred = [this](
            const DerechoSST& sst) { return true; };
        auto stability_trig =
            [this, subgroup_num, shard_members, num_shard_members](DerechoSST& sst) {
            // compute the min of the seq_num
            long long int min_seq_num = sst.seq_num[node_id_to_sst_index[shard_members[0]]][subgroup_num];
            for(uint i = 0; i < num_shard_members; ++i) {
	      if(sst.seq_num[node_id_to_sst_index[shard_members[i]]][subgroup_num] < min_seq_num) {
                    min_seq_num = sst.seq_num[node_id_to_sst_index[shard_members[i]]][subgroup_num];
                }
            }
            if(min_seq_num > sst.stable_num[member_index][subgroup_num]) {
                util::debug_log().log_event(std::stringstream()
                                            << "Subgroup " << subgroup_num << ", updating stable_num to "
                                            << min_seq_num);
                sst.stable_num[member_index][subgroup_num] = min_seq_num;
                sst.put(get_shard_sst_indices(subgroup_num), (char*)std::addressof(sst.stable_num[0][subgroup_num]) - sst.getBaseAddress(), sizeof(long long int));
            }
            };
        stability_pred_handle = sst->predicates.insert(
            stability_pred, stability_trig, sst::PredicateType::RECURRENT);

        auto delivery_pred = [this](
            const DerechoSST& sst) { return true; };
        auto delivery_trig = [this, subgroup_num, shard_members, num_shard_members](
            DerechoSST& sst) {
        std::lock_guard<std::mutex> lock(msg_state_mtx);
        // compute the min of the stable_num
        long long int min_stable_num = sst.stable_num[node_id_to_sst_index[shard_members[0]]][subgroup_num];
        for(uint i = 0; i < num_shard_members; ++i) {
            if(sst.stable_num[node_id_to_sst_index[shard_members[i]]][subgroup_num] < min_stable_num) {
                min_stable_num = sst.stable_num[node_id_to_sst_index[shard_members[i]]][subgroup_num];
            }
        }

        if(!locally_stable_messages[subgroup_num].empty()) {
            long long int least_undelivered_seq_num =
                locally_stable_messages[subgroup_num].begin()->first;
            if(least_undelivered_seq_num <= min_stable_num) {
	      util::debug_log().log_event(std::stringstream() << "Subgroup " << subgroup_num << ", can deliver a locally stable message: min_stable_num=" << min_stable_num << " and least_undelivered_seq_num=" << least_undelivered_seq_num);
                Message& msg = locally_stable_messages[subgroup_num].begin()->second;
                deliver_message(msg, subgroup_num);
                sst.delivered_num[member_index][subgroup_num] = least_undelivered_seq_num;
                sst.put(get_shard_sst_indices(subgroup_num), (char*)std::addressof(sst.delivered_num[0][subgroup_num]) - sst.getBaseAddress(), sizeof(long long int));
                locally_stable_messages[subgroup_num].erase(locally_stable_messages[subgroup_num].begin());
            }
        }
        };

        delivery_pred_handle = sst->predicates.insert(delivery_pred, delivery_trig, sst::PredicateType::RECURRENT);

        auto sender_pred = [this, subgroup_num, shard_members, shard_index, num_shard_members](const DerechoSST& sst) {
	      long long int seq_num = next_message_to_deliver[subgroup_num] * num_shard_members + shard_index;
          for(uint i = 0; i < num_shard_members; ++i) {
              if(sst.delivered_num[i][subgroup_num] < seq_num || (file_writer && sst.persisted_num[i][subgroup_num] < seq_num)) {
                  return false;
              }
          }
        return true;
        };
        auto sender_trig = [this, subgroup_num](DerechoSST& sst) {
        sender_cv.notify_all();
        next_message_to_deliver[subgroup_num]++;
        };
        sender_pred_handle = sst->predicates.insert(sender_pred, sender_trig,
                                                    sst::PredicateType::RECURRENT);
    }
}

template <typename dispatchersType>
DerechoGroup<dispatchersType>::~DerechoGroup() {
    wedge();
    if(timeout_thread.joinable()) {
        timeout_thread.join();
    }
    if(rpc_thread.joinable()) {
        rpc_thread.join();
    }
}

template <typename dispatchersType>
long long unsigned int DerechoGroup<dispatchersType>::compute_max_msg_size(
    const long long unsigned int max_payload_size,
    const long long unsigned int block_size) {
    auto max_msg_size = max_payload_size + sizeof(header);
    if(max_msg_size % block_size != 0) {
        max_msg_size = (max_msg_size / block_size + 1) * block_size;
    }
    return max_msg_size;
}

template <typename dispatchersType>
void DerechoGroup<dispatchersType>::wedge() {
    bool thread_shutdown_existing = thread_shutdown.exchange(true);
    if(thread_shutdown_existing) {  // Wedge has already been called
        return;
    }

    sst->predicates.remove(stability_pred_handle);
    sst->predicates.remove(delivery_pred_handle);
    sst->predicates.remove(sender_pred_handle);

    for(uint i = 0; i < num_members; ++i) {
        rdmc::destroy_group(i + rdmc_group_num_offset);
    }

    if(rpc_thread.joinable()) {
        rpc_thread.join();
    }
    connections.destroy();

    sender_cv.notify_all();
    if(sender_thread.joinable()) {
        sender_thread.join();
    }
}

template <typename dispatchersType>
void DerechoGroup<dispatchersType>::send_loop() {
    uint32_t subgroup_to_send = 0;
    auto num_subgroups = subgroup_info.num_subgroups(num_members);
    auto should_send_to_subgroup = [&](uint32_t subgroup_num) {
        if(!rdmc_groups_created) {
            return false;
        }
        if(pending_sends[subgroup_num].empty()) {
            return false;
        }
        Message &msg = pending_sends[subgroup_num].front();
        if(sst->nReceived[member_index][subgroup_to_nReceived_offset[subgroup_num]] < msg.index - 1) {
            return false;
        }

	auto p = subgroup_to_shard_n_index[subgroup_num];
        uint32_t shard_num = p.first;
        uint32_t shard_index = p.second;
        auto shard_members = subgroup_info.subgroup_membership(num_members, subgroup_num, shard_num);
        auto num_shard_members = shard_members.size();
        for (uint i = 0; i < num_shard_members; ++i) {
	  if (sst->delivered_num[i][subgroup_num] < (int)((msg.index - window_size) * num_shard_members + shard_index)
	      || (file_writer && sst->persisted_num[i][subgroup_num] < (int)((msg.index - window_size) * num_shard_members + shard_index))) {
                return false;
            }
        }
	
        return true;
    };
    auto should_send = [&]() {
      for (uint i = 1; i <= num_subgroups; ++i) {
	if (should_send_to_subgroup((subgroup_to_send + i)%num_subgroups)) {
	  subgroup_to_send = (subgroup_to_send + i)%num_subgroups;
	  return true;
	}
      }
      return false;
    };
    auto should_wake = [&]() { return thread_shutdown || should_send(); };
    try {
        std::unique_lock<std::mutex> lock(msg_state_mtx);
        while(!thread_shutdown) {
            sender_cv.wait(lock, should_wake);
            if(!thread_shutdown) {
                current_sends[subgroup_to_send] = std::move(pending_sends[subgroup_to_send].front());
                util::debug_log().log_event(std::stringstream() << "Calling send in subgroup " << subgroup_to_send << " on message " << current_sends[subgroup_to_send]->index
                                                                << " from sender " << current_sends[subgroup_to_send]->sender_rank);
                if(!rdmc::send(subgroup_to_rdmc_group[subgroup_to_send],
                               current_sends[subgroup_to_send]->message_buffer.mr, 0,
                               current_sends[subgroup_to_send]->size)) {
                    throw std::runtime_error("rdmc::send returned false");
                }
                pending_sends[subgroup_to_send].pop();
            }
        }
        std::cout << "DerechoGroup send thread shutting down" << std::endl;
    } catch(const std::exception& e) {
        std::cout << "DerechoGroup send thread had an exception: " << e.what() << std::endl;
    }
}

template <typename dispatchersType>
void DerechoGroup<dispatchersType>::check_failures_loop() {
    while(!thread_shutdown) {
        std::this_thread::sleep_for(milliseconds(sender_timeout));
        if(sst) sst->put((char*)std::addressof(sst->heartbeat[0]) - sst->getBaseAddress(), sizeof(bool));
    }
}

template <typename dispatchersType>
bool DerechoGroup<dispatchersType>::send(uint32_t subgroup_num) {
    std::lock_guard<std::mutex> lock(msg_state_mtx);
    if(thread_shutdown || !rdmc_groups_created) {
        return false;
    }
    assert(next_sends[subgroup_num]);
    pending_sends[subgroup_num].push(std::move(*next_sends[subgroup_num]));
    next_sends[subgroup_num] = std::experimental::nullopt;
    sender_cv.notify_all();
    return true;
}

template <typename dispatchersType>
char* DerechoGroup<dispatchersType>::get_sendbuffer_ptr(
    uint32_t subgroup_num,
    long long unsigned int payload_size,
    int pause_sending_turns, bool cooked_send) {
    // if rdmc groups were not created because of failures, return NULL
    if(!rdmc_groups_created) {
        return NULL;
    }
    long long unsigned int msg_size = payload_size + sizeof(header);
    // payload_size is 0 when max_msg_size is desired, useful for ordered send/query
    if(!payload_size) {
        msg_size = max_msg_size;
    }
    if(msg_size > max_msg_size) {
        std::cout << "Can't send messages of size larger than the maximum message "
                     "size which is equal to "
                  << max_msg_size << std::endl;
        return nullptr;
    }

    auto p = subgroup_to_shard_n_index[subgroup_num];
    auto shard_num = p.first;
    auto shard_index = p.second;
    auto shard_members = subgroup_info.subgroup_membership(num_members, subgroup_num, shard_num);
    auto num_shard_members = shard_members.size();

    for(uint i = 0; i < num_shard_members; ++i) {
        if(sst->delivered_num[i][subgroup_num] <
           (int)((future_message_indices[subgroup_num] - window_size) * num_shard_members + shard_index)) {
            return nullptr;
        }
    }

    std::unique_lock<std::mutex> lock(msg_state_mtx);
    if(thread_shutdown) return nullptr;
    if(free_message_buffers[subgroup_num].empty()) return nullptr;

    // Create new Message
    Message msg;
    msg.sender_rank = shard_index;
    msg.index = future_message_indices[subgroup_num];
    msg.size = msg_size;
    msg.message_buffer = std::move(free_message_buffers[subgroup_num].back());
    free_message_buffers[subgroup_num].pop_back();

    // Fill header
    char* buf = msg.message_buffer.buffer.get();
    ((header*)buf)->header_size = sizeof(header);
    ((header*)buf)->pause_sending_turns = pause_sending_turns;
    ((header*)buf)->cooked_send = cooked_send;

    next_sends[subgroup_num] = std::move(msg);
    future_message_indices[subgroup_num] += pause_sending_turns + 1;

    return buf + sizeof(header);
}

template <typename dispatchersType>
template <typename IdClass, unsigned long long tag, typename... Args>
auto DerechoGroup<dispatchersType>::derechoCallerSend(uint32_t subgroup_num,
                                                      const std::vector<node_id_t>& nodes, char* buf, Args&&... args) {
    auto max_payload_size = max_msg_size - sizeof(header);
    // use nodes
    ((size_t*)buf)[0] = nodes.size();
    buf += sizeof(size_t);
    max_payload_size -= sizeof(size_t);
    for(auto& n : nodes) {
        ((node_id_t*)buf)[0] = n;
        buf += sizeof(node_id_t);
        max_payload_size -= sizeof(node_id_t);
    }

    auto return_pair = dispatchers.template Send<IdClass, tag>(
        [&buf, &max_payload_size](size_t size) -> char* {
            if(size <= max_payload_size) {
                return buf;
            } else {
                return nullptr;
            }
        },
        std::forward<Args>(args)...);
    while(!send(subgroup_num)) {
    }
    auto P = createPending(return_pair.pending);

    std::lock_guard<std::mutex> lock(pending_results_mutex);
    if(nodes.size()) {
        P->fulfill_map(nodes);
        fulfilledList.push_back(std::move(P));
    } else {
        toFulfillQueue.push(std::move(P));
    }
    return std::move(return_pair.results);
}

// this should be called from the GMS, as this takes care of locks on mutexes
// view_change_mutex and msg_state_mutex
template <typename dispatchersType>
template <typename IDClass, unsigned long long tag, typename... Args>
void DerechoGroup<dispatchersType>::orderedSend(uint32_t subgroup_num, const std::vector<node_id_t>& nodes,
                                                char* buf, Args&&... args) {
    derechoCallerSend<IDClass, tag>(subgroup_num, nodes, buf, std::forward<Args>(args)...);
}

template <typename dispatchersType>
template <typename IdClass, unsigned long long tag, typename... Args>
void DerechoGroup<dispatchersType>::orderedSend(uint32_t subgroup_num, char* buf, Args&&... args) {
    // empty nodes means that the destination is the entire group
    orderedSend<IdClass, tag>(subgroup_num, {}, buf, std::forward<Args>(args)...);
}

template <typename dispatchersType>
template <typename IdClass, unsigned long long tag, typename... Args>
auto DerechoGroup<dispatchersType>::orderedQuery(uint32_t subgroup_num, const std::vector<node_id_t>& nodes,
                                                 char* buf, Args&&... args) {
    return derechoCallerSend<IdClass, tag>(subgroup_num, nodes, buf, std::forward<Args>(args)...);
}

template <typename dispatchersType>
template <typename IdClass, unsigned long long tag, typename... Args>
auto DerechoGroup<dispatchersType>::orderedQuery(uint32_t subgroup_num, char* buf, Args&&... args) {
    return orderedQuery<IdClass, tag>(subgroup_num, {}, buf, std::forward<Args>(args)...);
}

template <typename dispatchersType>
template <typename IdClass, unsigned long long tag, typename... Args>
auto DerechoGroup<dispatchersType>::tcpSend(node_id_t dest_node,
                                            Args&&... args) {
    assert(dest_node != members[member_index]);
    // use dest_node

    size_t size;
    auto max_payload_size = max_msg_size - sizeof(header);
    auto return_pair = dispatchers.template Send<IdClass, tag>(
        [this, &max_payload_size, &size](size_t _size) -> char* {
            size = _size;
            if(size <= max_payload_size) {
                return p2pBuffer.get();
            } else {
                return nullptr;
            }
        },
        std::forward<Args>(args)...);
    connections.write(dest_node, p2pBuffer.get(), size);
    auto P = createPending(return_pair.pending);
    P->fulfill_map({dest_node});

    std::lock_guard<std::mutex> lock(pending_results_mutex);
    fulfilledList.push_back(std::move(P));
    return std::move(return_pair.results);
}

template <typename dispatchersType>
template <typename IdClass, unsigned long long tag, typename... Args>
void DerechoGroup<dispatchersType>::p2pSend(node_id_t dest_node,
                                            Args&&... args) {
    tcpSend<IdClass, tag>(dest_node, std::forward<Args>(args)...);
}

template <typename dispatchersType>
template <typename IdClass, unsigned long long tag, typename... Args>
auto DerechoGroup<dispatchersType>::p2pQuery(node_id_t dest_node,
                                             Args&&... args) {
    return tcpSend<IdClass, tag>(dest_node, std::forward<Args>(args)...);
}

template <typename dispatchersType>
void DerechoGroup<dispatchersType>::send_objects(tcp::socket& new_member_socket) {
    dispatchers.send_objects(new_member_socket);
}

template <typename dispatchersType>
void DerechoGroup<dispatchersType>::rpc_process_loop() {
    using namespace ::rpc::remote_invocation_utilities;
    const auto header_size = header_space();
    auto max_payload_size = max_msg_size - sizeof(header);
    std::unique_ptr<char[]> rpcBuffer =
        std::unique_ptr<char[]>(new char[max_payload_size]);
    while(!thread_shutdown) {
        auto other_id = connections.probe_all();
        if(other_id < 0) {
            continue;
        }
        connections.read(other_id, rpcBuffer.get(), header_size);
        std::size_t payload_size;
        Opcode indx;
        Node_id received_from;
        retrieve_header(nullptr, rpcBuffer.get(), payload_size,
                        indx, received_from);
        connections.read(other_id, rpcBuffer.get() + header_size,
                         payload_size);
        size_t reply_size = 0;
        dispatchers.handle_receive(
            indx, received_from, rpcBuffer.get() + header_size, payload_size,
            [&rpcBuffer, &max_payload_size,
             &reply_size](size_t _size) -> char* {
                reply_size = _size;
                if(reply_size <= max_payload_size) {
                    return rpcBuffer.get();
                } else {
                    return nullptr;
                }
            });
        if(reply_size > 0) {
            connections.write(received_from.id, rpcBuffer.get(),
                              reply_size);
        }
    }
}

template <typename dispatchersType>
void DerechoGroup<dispatchersType>::set_exceptions_for_removed_nodes(
    std::vector<node_id_t> removed_members) {
    std::lock_guard<std::mutex> lock(pending_results_mutex);
    for(auto& pending : fulfilledList) {
        for(auto removed_id : removed_members) {
            pending->set_exception_for_removed_node(removed_id);
        }
    }
}

template <typename dispatchersType>
void DerechoGroup<dispatchersType>::debug_print() {
    std::cout << "In DerechoGroup SST has " << sst->get_num_rows()
              << " rows; member_index is " << member_index << std::endl;
    std::cout << "Printing SST" << std::endl;
    for(uint i = 0; i < num_members; ++i) {
        std::cout << sst->seq_num[i] << " " << sst->stable_num[i] << " " << sst->delivered_num[i] << std::endl;
    }
    std::cout << std::endl;

    std::cout << "Printing last_received_messages" << std::endl;
    for(uint i = 0; i < num_members; ++i) {
        std::cout << sst->nReceived[member_index][i] << " " << std::endl;
    }
    std::cout << std::endl;
}

template <typename dispatchersType>
std::vector<uint32_t> DerechoGroup<dispatchersType>::get_shard_sst_indices(uint32_t subgroup_num) {
    auto p = subgroup_to_shard_n_index[subgroup_num];
    auto shard_num = p.first;
    auto shard_members = subgroup_info.subgroup_membership(num_members, subgroup_num, shard_num);

    std::vector<uint32_t> shard_sst_indices;
    for(auto m : shard_members) {
        shard_sst_indices.push_back(node_id_to_sst_index[m]);
    }
    return shard_sst_indices;
}

}  // namespace derecho
