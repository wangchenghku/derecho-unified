
#include "group_send.h"
#include "message.h"
#include "util.h"

#include <cstring>

using namespace std;
using namespace rdma;
using namespace rdmc;

namespace rdmc {
extern map<uint16_t, shared_ptr<group>> groups;
extern mutex groups_lock;
};

decltype(polling_group::message_types) polling_group::message_types;
decltype(cross_channel_group::message_types) cross_channel_group::message_types;

group::group(uint16_t _group_number, size_t _block_size,
                       vector<uint32_t> _members, uint32_t _member_index,
                       incoming_message_callback_t upcall,
                       completion_callback_t callback,
                       unique_ptr<schedule> _schedule)
        : members(_members),
          group_number(_group_number),
          block_size(_block_size),
          num_members(members.size()),
          member_index(_member_index),
          transfer_schedule(std::move(_schedule)),
          completion_callback(callback),
          incoming_message_upcall(upcall) {}
group::~group() { unique_lock<mutex> lock(monitor); }

void polling_group::initialize_message_types() {
	static bool initialized = false;
	CHECK(!initialized);
	initialized = true;
	
    auto find_group = [](uint16_t group_number) {
        unique_lock<mutex> lock(groups_lock);
        auto it = groups.find(group_number);
        return it != groups.end() ? it->second : nullptr;
    };
    auto send_data_block = [find_group](uint64_t tag, uint32_t immediate,
                                        size_t length) {
        ParsedTag parsed_tag = parse_tag(tag);
        shared_ptr<group> g = find_group(parsed_tag.group_number);
        if(g) g->complete_block_send();
    };
    auto receive_data_block = [find_group](uint64_t tag, uint32_t immediate,
                                           size_t length) {
        ParsedTag parsed_tag = parse_tag(tag);
        shared_ptr<group> g = find_group(parsed_tag.group_number);
        if(g) g->receive_block(immediate, length);
    };
    auto send_ready_for_block = [](uint64_t, uint32_t, size_t) {};
    auto receive_ready_for_block = [find_group](
        uint64_t tag, uint32_t immediate, size_t length) {
        ParsedTag parsed_tag = parse_tag(tag);
        shared_ptr<group> g = find_group(parsed_tag.group_number);
        if(g) g->receive_ready_for_block(immediate, parsed_tag.target);
    };

    message_types.data_block =
        message_type("rdmc.data_block", send_data_block, receive_data_block);
    message_types.ready_for_block = message_type(
        "rdmc.ready_for_block", send_ready_for_block, receive_ready_for_block);
}
polling_group::polling_group(uint16_t _group_number, size_t _block_size,
                             vector<uint32_t> _members, uint32_t _member_index,
                             incoming_message_callback_t upcall,
                             completion_callback_t callback,
                             unique_ptr<schedule> _schedule)
        : group(_group_number, _block_size, _members, _member_index, upcall,
                     callback, std::move(_schedule)),
          first_block_buffer(nullptr) {
    if(member_index != 0) {
        first_block_buffer = unique_ptr<char[]>(new char[block_size]);
        memset(first_block_buffer.get(), 0, block_size);

        first_block_mr =
            make_unique<memory_region>(first_block_buffer.get(), block_size);
    }

    auto connections = transfer_schedule->get_connections();
    for(auto c : connections) {
        connect(c);
    }

    if(member_index > 0) {
        auto transfer = transfer_schedule->get_first_block(num_blocks);
        first_block_number = transfer->block_number;
        post_recv(*transfer);
        incoming_block = transfer->block_number;
        send_ready_for_block(transfer->target);
        // puts("Issued Ready For Block CCCCCCCCC");
    }
}
void polling_group::receive_block(uint32_t send_imm, size_t received_block_size) {
    unique_lock<mutex> lock(monitor);

    assert(member_index > 0);

    if(receive_step == 0) {
        num_blocks = parse_immediate(send_imm).total_blocks;
        first_block_number =
            min(transfer_schedule->get_first_block(num_blocks)->block_number,
                num_blocks - 1);
        message_size = num_blocks * block_size;
        if(num_blocks == 1) {
            message_size = received_block_size;
        }

        assert(*first_block_number == parse_immediate(send_imm).block_number);

        //////////////////////////////////////////////////////
        auto destination = incoming_message_upcall(message_size);
        mr_offset = destination.offset;
        mr = destination.mr;

        assert(mr->size >= mr_offset + message_size);
        //////////////////////////////////////////////////////

        num_received_blocks = 1;
        received_blocks = vector<bool>(num_blocks);
        received_blocks[*first_block_number] = true;

        LOG_EVENT(group_number, message_number, *first_block_number,
                  "initialized_internal_datastructures");

        assert(receive_step == 0);
        auto transfer = transfer_schedule->get_incoming_transfer(num_blocks,
                                                                 receive_step);
        while((!transfer || transfer->block_number == *first_block_number) &&
              receive_step < transfer_schedule->get_total_steps(num_blocks)) {
            transfer = transfer_schedule->get_incoming_transfer(num_blocks, ++receive_step);
        }

        // cout << "receive_step = " << receive_step
        //      << " transfer->block_number = "
        //      << transfer->block_number
        //      << " first_block_number = " << *first_block_number
        //      << " total_steps = " << get_total_steps() << endl;

        LOG_EVENT(group_number, message_number, *first_block_number,
                  "found_next_transfer");

        if(transfer) {
            LOG_EVENT(group_number, message_number, transfer->block_number,
                      "posting_recv");
            // printf("Posting recv #%d (receive_step = %d,
            // *first_block_number =
            // %d, total_steps = %d)\n",
            //        (int)transfer->block_number, (int)receive_step,
            // (int)*first_block_number, (int)get_total_steps());
            post_recv(*transfer);
            incoming_block = transfer->block_number;
            send_ready_for_block(transfer->target);
            // cout << "Issued Ready For Block AAAAAAAA (receive_step = "
            //      << receive_step << ", target = " << transfer->target << ")"
            //      << endl;

            for(auto r = receive_step + 1; r < transfer_schedule->get_total_steps(num_blocks); r++) {
                auto t = transfer_schedule->get_incoming_transfer(num_blocks, r);
                if(t) {
                    // cout << "posting block for step " << (int)r
                    //      << " (block #" << (*t).block_number << ")" << endl;
                    post_recv(*t);
                    break;
                }
            }
        }

        LOG_EVENT(group_number, message_number, *first_block_number,
                  "calling_send_next_block");

        send_next_block();

        LOG_EVENT(group_number, message_number, *first_block_number,
                  "returned_from_send_next_block");

        if(!sending && num_received_blocks == num_blocks &&
           send_step == transfer_schedule->get_total_steps(num_blocks)) {
            complete_message();
        }
    } else {
        //        assert(tag.index() <= tag.message_size());
        size_t block_number = incoming_block;
        if(block_number != parse_immediate(send_imm).block_number) {
            printf("Expected block #%d but got #%d on step %d\n",
                   (int)block_number,
                   (int)parse_immediate(send_imm).block_number,
                   (int)receive_step);
            fflush(stdout);
        }
        assert(block_number == parse_immediate(send_imm).block_number);

        if(block_number == num_blocks - 1) {
            message_size = (num_blocks - 1) * block_size + received_block_size;
        } else {
            assert(received_block_size == block_size);
        }

        received_blocks[block_number] = true;

        LOG_EVENT(group_number, message_number, block_number, "received_block");

        // Figure out the next block to receive.
        optional<schedule::block_transfer> transfer;
        while(!transfer && receive_step + 1 < transfer_schedule->get_total_steps(num_blocks)) {
            transfer = transfer_schedule->get_incoming_transfer(num_blocks, ++receive_step);
        }

        // Post a receive for it.
        if(transfer) {
            incoming_block = transfer->block_number;
            send_ready_for_block(transfer->target);
            // cout << "Issued Ready For Block BBBBBBBB (receive_step = "
            //      << receive_step << ", target = " << transfer->target
            //      << ", total_steps = " << get_total_steps() << ")" << endl;
            for(auto r = receive_step + 1; r < transfer_schedule->get_total_steps(num_blocks); r++) {
                auto t = transfer_schedule->get_incoming_transfer(num_blocks, r);
                if(t) {
                    post_recv(*t);
                    break;
                }
            }
        }

        // If we just finished receiving a block and we weren't
        // previously sending, then try to send now.
        if(!sending) {
            send_next_block();
        }
        // If we just received the last block and aren't still sending then
        // issue a completion callback
        if(++num_received_blocks == num_blocks && !sending &&
           send_step == transfer_schedule->get_total_steps(num_blocks)) {
            complete_message();
        }
    }
}
void polling_group::receive_ready_for_block(uint32_t step, uint32_t sender) {
    unique_lock<mutex> lock(monitor);

    auto it = rfb_queue_pairs.find(sender);
    assert(it != rfb_queue_pairs.end());
    it->second.post_empty_recv(form_tag(group_number, sender),
                               message_types.ready_for_block);

    receivers_ready.insert(sender);

    if(!sending && mr) {
        send_next_block();
    }
}
void polling_group::complete_block_send() {
    unique_lock<mutex> lock(monitor);

    LOG_EVENT(group_number, message_number, outgoing_block,
              "finished_sending_block");

    send_next_block();

    // If we just send the last block, and were already done
    // receiving, then signal completion and prepare for the next
    // message.
    if(!sending && send_step == transfer_schedule->get_total_steps(num_blocks) &&
       (member_index == 0 || num_received_blocks == num_blocks)) {
        complete_message();
    }
}
void polling_group::send_message(shared_ptr<memory_region> message_mr, size_t offset,
                         size_t length) {
    LOG_EVENT(group_number, -1, -1, "send()");

    unique_lock<mutex> lock(monitor);

    if(length == 0) throw rdmc::invalid_args();
    if(offset + length > message_mr->size) throw rdmc::invalid_args();
    if(member_index > 0) throw rdmc::nonroot_sender();

    // Queueing sends is not supported
    if(receive_step > 0) throw rdmc::group_busy();
    if(send_step > 0) throw rdmc::group_busy();

    mr = message_mr;
    mr_offset = offset;
    message_size = length;
    num_blocks = (message_size - 1) / block_size + 1;
    if(num_blocks > std::numeric_limits<uint16_t>::max())
        throw rdmc::invalid_args();
    // printf("message_size = %lu, block_size = %lu, num_blocks = %lu\n",
    //        message_size, block_size, num_blocks);
    LOG_EVENT(group_number, message_number, -1, "send_message");

    send_next_block();
    // No need to worry about completion here. We must send at least
    // one block, so we can't be done already.
}
void polling_group::send_next_block() {
    sending = false;
    if(send_step == transfer_schedule->get_total_steps(num_blocks)) {
        return;
    }
    auto transfer = transfer_schedule->get_outgoing_transfer(num_blocks, send_step);
    while(!transfer) {
        if(++send_step == transfer_schedule->get_total_steps(num_blocks)) return;

        transfer = transfer_schedule->get_outgoing_transfer(num_blocks, send_step);
    }

    size_t target = transfer->target;
    size_t block_number = transfer->block_number;
    //    size_t forged_block_number = transfer->forged_block_number;

    if(member_index > 0 && !received_blocks[block_number]) return;

    if(receivers_ready.count(transfer->target) == 0) {
        LOG_EVENT(group_number, message_number, block_number,
                  "receiver_not_ready");
        return;
    }

    receivers_ready.erase(transfer->target);
    sending = true;
    ++send_step;

    // printf("sending block #%d to node #%d on step %d\n", (int)block_number,
    // 	   (int)target, (int)send_step-1);
    // fflush(stdout);
    auto it = queue_pairs.find(target);
    assert(it != queue_pairs.end());

    if(first_block_number && block_number == *first_block_number) {
        CHECK(it->second.post_send(*first_block_mr, 0, block_size,
                                   form_tag(group_number, target),
                                   form_immediate(num_blocks, block_number),
                                   message_types.data_block));
    } else {
        size_t offset = block_number * block_size;
        size_t nbytes = min(block_size, message_size - offset);
        CHECK(it->second.post_send(*mr, mr_offset + offset, nbytes,
                                   form_tag(group_number, target),
                                   form_immediate(num_blocks, block_number),
                                   message_types.data_block));
    }
    outgoing_block = block_number;
    LOG_EVENT(group_number, message_number, block_number,
              "started_sending_block");
}
void polling_group::complete_message() {
    // remap first_block into buffer
    if(member_index > 0 && first_block_number) {
        LOG_EVENT(group_number, message_number, *first_block_number,
                  "starting_remap_first_block");
        // if(block_size > (128 << 10) && (block_size % 4096 == 0)) {
        //     char *tmp_buffer =
        //         (char *)mmap(NULL, block_size, PROT_READ | PROT_WRITE,
        //                      MAP_ANON | MAP_PRIVATE, -1, 0);

        //     mremap(buffer + block_size * (*first_block_number), block_size,
        //            block_size, MREMAP_FIXED | MREMAP_MAYMOVE, tmp_buffer);

        //     mremap(first_block_buffer, block_size, block_size,
        //            MREMAP_FIXED | MREMAP_MAYMOVE,
        //            buffer + block_size * (*first_block_number));
        //     first_block_buffer = tmp_buffer;
        // } else {
        memcpy(mr->buffer + mr_offset + block_size * (*first_block_number),
               first_block_buffer.get(), block_size);
        // }
        LOG_EVENT(group_number, message_number, *first_block_number,
                  "finished_remap_first_block");
    }
    completion_callback(mr->buffer + mr_offset, message_size);

    ++message_number;
    sending = false;
    send_step = 0;
    receive_step = 0;
    mr.reset();
    // if(first_block_buffer == nullptr && member_index > 0){
    //     first_block_buffer = (char*)mmap(NULL, block_size,
    // PROT_READ|PROT_WRITE,
    //                                      MAP_ANON|MAP_PRIVATE, -1, 0);
    //     memset(first_block_buffer, 1, block_size);
    //     memset(first_block_buffer, 0, block_size);
    // }
    first_block_number = boost::none;

    if(member_index != 0) {
        num_received_blocks = 0;
        received_blocks.clear();
        auto transfer = transfer_schedule->get_first_block(num_blocks);
        assert(transfer);
        first_block_number = transfer->block_number;
        post_recv(*transfer);
        incoming_block = transfer->block_number;
        send_ready_for_block(transfer->target);
        // cout << "Issued Ready For Block DDDDDDD (target = " <<
        // transfer->target
        //      << ")" << endl;
    }
}
void polling_group::post_recv(schedule::block_transfer transfer) {
    auto it = queue_pairs.find(transfer.target);
    assert(it != queue_pairs.end());

    // printf("Posting receive buffer for block #%d from node #%d\n",
    //        (int)transfer.block_number, (int)transfer.target);
    // fflush(stdout);

    if(first_block_number && transfer.block_number == *first_block_number) {
        CHECK(it->second.post_recv(*first_block_mr, 0, block_size,
                                   form_tag(group_number, transfer.target),
                                   message_types.data_block));
    } else {
        size_t offset = block_size * transfer.block_number;
        size_t length = min(block_size, (size_t)(message_size - offset));

        if(length > 0) {
            CHECK(it->second.post_recv(*mr, mr_offset + offset, length,
                                       form_tag(group_number, transfer.target),
                                       message_types.data_block));
        }
    }
    LOG_EVENT(group_number, message_number, transfer.block_number,
              "posted_receive_buffer");
}
void polling_group::connect(uint32_t neighbor) {
    queue_pairs.emplace(neighbor, queue_pair(members[neighbor]));

    auto post_recv = [this, neighbor](rdma::queue_pair* qp) {
        qp->post_empty_recv(form_tag(group_number, neighbor),
                            message_types.ready_for_block);
    };

    rfb_queue_pairs.emplace(neighbor, queue_pair(members[neighbor], post_recv));
}
void polling_group::send_ready_for_block(uint32_t neighbor) {
    auto it = rfb_queue_pairs.find(neighbor);
    assert(it != rfb_queue_pairs.end());
    it->second.post_empty_send(form_tag(group_number, neighbor), 0,
                               message_types.ready_for_block);
}


void cross_channel_group::initialize_message_types() {
	static bool initialized = false;
	CHECK(!initialized);
	initialized = true;
	
	auto send_init = [](uint64_t tag, uint32_t immediate, size_t length) {};
	auto recv_init = [](uint64_t tag, uint32_t immediate, size_t length) {
        ParsedTag parsed_tag = parse_tag(tag);
        unique_lock<mutex> lock(groups_lock);
		auto it = groups.find(parsed_tag.group_number);
        if(it != groups.end()) {
            static_pointer_cast<cross_channel_group>(it->second)->receive_init();
        }
	};

	auto send_init_ack = [](uint64_t tag, uint32_t immediate, size_t length) {};
    auto recv_init_ack = [](uint64_t tag, uint32_t immediate, size_t length) {
        ParsedTag parsed_tag = parse_tag(tag);
        unique_lock<mutex> lock(groups_lock);
		auto it = groups.find(parsed_tag.group_number);
        if(it != groups.end()) {
			auto g = static_pointer_cast<cross_channel_group>(it->second);
			unique_lock<mutex> l(g->init_ack_count_mutex);
            if(++g->init_ack_count == g->members.size() - 1) {
                g->init_ack_count_cv.notify_all();
            }
        }
    };

    auto complete_message = [](uint64_t tag, uint32_t immediate, size_t length) {
        ParsedTag parsed_tag = parse_tag(tag);
        unique_lock<mutex> lock(groups_lock);
		auto it = groups.find(parsed_tag.group_number);
        if(it != groups.end()) {
			auto g = static_pointer_cast<cross_channel_group>(it->second);
			g->complete_message();
		}
    };

    message_types.init = message_type("rdmc.cross_channel.init",
                                      send_init, recv_init);
    message_types.init_ack = message_type("rdmc.cross_channel.init_ack",
										  send_init_ack, recv_init_ack);
    message_types.completed = message_type("rdmc.cross_channel.completed",
                                           complete_message, complete_message,
                                           complete_message);
}

cross_channel_group::cross_channel_group(uint16_t _group_number, size_t _block_size,
                             vector<uint32_t> _members, uint32_t _member_index,
                             incoming_message_callback_t upcall,
                             completion_callback_t callback,
                             unique_ptr<schedule> _schedule)
        : group(_group_number, _block_size, _members, _member_index, upcall,
                     callback, std::move(_schedule)),
          first_block_buffer(nullptr),
		  init_mr(sizeof(init_message)),
		  mqp(make_shared<manager_queue_pair>()) {
	CHECK(rdma::get_supported_features().cross_channel);
	LOG_EVENT(group_number, message_number, -1, "start cross_channel_group()");
    if(member_index != 0) {
        first_block_buffer = unique_ptr<char[]>(new char[block_size]);
        memset(first_block_buffer.get(), 0, block_size);

        first_block_mr =
            make_unique<memory_region>(first_block_buffer.get(), block_size);
    }

	LOG_EVENT(group_number, message_number, -1, "about to connect");
    auto connections = transfer_schedule->get_connections();
    for(auto c : connections) {
        connect(c);
    }
	LOG_EVENT(group_number, message_number, -1, "connected");

	if(member_index == 0) {
		for(size_t i = 1; i < members.size(); i++) {
            init_queue_pairs.emplace(i, queue_pair(members[i]));
        }
    } else {
        auto post_recvs = [this](queue_pair* qp) {
            qp->post_recv(init_mr, 0, sizeof(init_message),
						  form_tag(group_number, 0),
                          message_types.init);
        };
        init_queue_pairs.emplace(0, queue_pair(members[0], post_recvs));

        auto transfer = transfer_schedule->get_first_block(num_blocks);
        first_block_number = transfer->block_number;

		// uint64_t tag = form_tag(group_number, transfer->target);

        // // Post first block receive.
        // CHECK(queue_pairs.at(transfer->target).post_recv(
		// 	  *first_block_mr, 0, block_size, tag,
		// 	  message_type::ignored()));
		// size_t recv_count = ++recv_counts[transfer->target];

		// // Send ready for block, and wait on receive.
        // rdma::task task(mqp);
        // task.append_empty_send(rfb_queue_pairs.at(transfer->target), 0);
        // task.append_enable_send(rfb_queue_pairs.at(transfer->target),
        //                         ++rfb_send_counts[transfer->target]);
        // task.append_wait(queue_pairs.at(transfer->target).rcq, recv_count, true,
		// 				 false, tag, message_types.first_block);
        // CHECK(task.post());
    }
	LOG_EVENT(group_number, message_number, -1, "end cross_channel_group()");
}

void cross_channel_group::send_message(shared_ptr<memory_region> message_mr,
                                       size_t offset,
                                       size_t length) {
    LOG_EVENT(group_number, -1, -1, "send()");

    unique_lock<mutex> lock(monitor);

    if(length == 0) throw rdmc::invalid_args();
    if(offset + length > message_mr->size) throw rdmc::invalid_args();
    if(member_index > 0) throw rdmc::nonroot_sender();

    // Queueing sends is not supported
    if(message_in_progress) throw rdmc::group_busy();

    mr = message_mr;
    mr_offset = offset;
    message_size = length;
    num_blocks = (message_size - 1) / block_size + 1;
    if(num_blocks > std::numeric_limits<uint16_t>::max())
        throw rdmc::invalid_args();
    // printf("message_size = %lu, block_size = %lu, num_blocks = %lu\n",
    //        message_size, block_size, num_blocks);
    LOG_EVENT(group_number, message_number, -1, "start_init");

	init_ack_count = 0;
	((init_message*)init_mr.buffer)->size = message_size;
	for(size_t i = 1; i < members.size(); i++) {
        init_queue_pairs.at(i).post_empty_recv(form_tag(group_number, i),
                                               message_types.init_ack);
        init_queue_pairs.at(i).post_send(init_mr, 0, sizeof(init_message),
                                         form_tag(group_number, i), 0,
                                         message_types.init);
    }
    LOG_EVENT(group_number, message_number, -1, "start_init_wait");
	{
		unique_lock<mutex> lock(init_ack_count_mutex);
        init_ack_count_cv.wait(lock, [&]() {
            return init_ack_count == members.size() - 1;
        });
    }
    LOG_EVENT(group_number, message_number, -1, "end_init");
	
	message_in_progress = true;

	rdma::task task(mqp);
	size_t total_steps = transfer_schedule->get_total_steps(num_blocks);
	for(size_t i = 0; i < total_steps; i++) {
		auto transfer = transfer_schedule->get_outgoing_transfer(num_blocks, i);
		if(!transfer) {
			continue;
		}
		
		size_t offset = transfer->block_number * block_size;
        size_t nbytes = min(block_size, message_size - offset);
        uint64_t imm = form_immediate(num_blocks, transfer->block_number);
        task.append_send(queue_pairs.at(transfer->target), *mr, offset, nbytes,
						 imm);
		task.append_empty_recv(rfb_queue_pairs.at(transfer->target));

		size_t rfb_recv_count = ++rfb_recv_counts[transfer->target];
		size_t send_count = ++send_counts[transfer->target];
        task.append_wait(rfb_queue_pairs.at(transfer->target).rcq,
						 rfb_recv_count - 1, false, false, 0x6500000 + i,
						 message_type::ignored());
        task.append_enable_send(queue_pairs.at(transfer->target), send_count);
	}

	size_t i = 0;
	size_t num_neighbors = queue_pairs.size();
	for(auto&& qp : queue_pairs) {
		task.append_wait(qp.second.scq, 0, ++i == num_neighbors, true,
						 form_tag(group_number, qp.first),
						 message_types.completed);
	}
    LOG_EVENT(group_number, message_number, -1, "task_ready");

	CHECK(task.post());
    LOG_EVENT(group_number, message_number, -1, "task_posted");
}

void cross_channel_group::complete_message() {
    // copy first block into buffer
    // if(member_index > 0 && first_block_number) {
    //     LOG_EVENT(group_number, message_number, *first_block_number,
    //               "starting_copying_first_block");
    //     memcpy(mr->buffer + mr_offset + block_size * (*first_block_number),
    //            first_block_buffer.get(), block_size);
    //     LOG_EVENT(group_number, message_number, *first_block_number,
    //               "finished_copying_first_block");
    // }
	LOG_EVENT(group_number, message_number, -1, "task_completed");

    completion_callback(mr->buffer + mr_offset, message_size);

    ++message_number;
    message_in_progress = false;
    first_block_number = boost::none;
	mr.reset();

    if(member_index != 0) {
        auto transfer = transfer_schedule->get_first_block(num_blocks);
        assert(transfer);
        first_block_number = transfer->block_number;
        // post_recv(*transfer);
		// send_ready_for_block(transfer->target);
		// TODO: post wait on first_block
    }
	LOG_EVENT(group_number, message_number, -1, "message_completed");
}
void cross_channel_group::connect(uint32_t neighbor) {
    queue_pairs.emplace(neighbor, managed_queue_pair(members[neighbor],
													 [](auto){}));

    auto post_recv = [this, neighbor](rdma::managed_queue_pair* qp) {
        qp->post_empty_recv(form_tag(group_number, neighbor),
                            rdma::message_type::ignored());
    };

    rfb_queue_pairs.emplace(neighbor, managed_queue_pair(members[neighbor],
														 post_recv));
}

void cross_channel_group::post_relay_task() {
	rdma::task task(mqp);
	size_t total_steps = transfer_schedule->get_total_steps(num_blocks);
	for(size_t i = 0; i < total_steps; i++) {
		auto incoming = transfer_schedule->get_incoming_transfer(num_blocks, i);
		if(incoming) {
            // Append recv block and send ready_for_block.
            size_t offset = incoming->block_number * block_size;
            size_t length = min(block_size, message_size - offset);
			task.append_recv(queue_pairs.at(incoming->target), *mr, offset,
							 length);
			task.append_empty_send(rfb_queue_pairs.at(incoming->target), 0);

			// Update send/recv counts.
            ++recv_counts[incoming->target];
            size_t rfb_send_count = ++rfb_send_counts[incoming->target];
			
            // Enable ready_for_block.
            task.append_enable_send(rfb_queue_pairs.at(incoming->target),
									rfb_send_count);
        }

		auto outgoing = transfer_schedule->get_outgoing_transfer(num_blocks, i);
		if(outgoing) {
            // Append send and ready_for_block recv.
            size_t offset = outgoing->block_number * block_size;
            size_t length = min(block_size, message_size - offset);
            uint64_t immediate = form_immediate(num_blocks,
												outgoing->block_number);
			task.append_send(queue_pairs.at(outgoing->target), *mr, offset,
                             length, immediate);

            task.append_empty_recv(rfb_queue_pairs.at(outgoing->target));

			// Update send/recv counts.
            size_t send_count = ++send_counts[outgoing->target];
            size_t rfb_recv_count = ++rfb_recv_counts[outgoing->target];

            // Wait on ready_for_block and then enable send.
            task.append_wait(rfb_queue_pairs.at(outgoing->target).rcq,
							 rfb_recv_count - 1, false, false, 0x6500000 + i,
							 message_type::ignored());
            task.append_enable_send(queue_pairs.at(outgoing->target),
									send_count);
        }

		if(incoming) {
			// Wait for receive to complete.
			task.append_wait(queue_pairs.at(incoming->target).rcq,
							 recv_counts[incoming->target], false, false,
							 0x7600000 + i, message_type::ignored());
		}
    }

	// Wait on all queue pairs, with the last one signalling.
	size_t i = 0;
	size_t num_neighbors = queue_pairs.size();
	for(auto&& qp : queue_pairs) {
		task.append_wait(qp.second.scq, 0, ++i == num_neighbors, true,
						 form_tag(group_number, qp.first),
						 message_types.completed);
	}

	CHECK(task.post());
}
void cross_channel_group::receive_init() {
	LOG_EVENT(group_number, message_number, -1, "init_received");

	message_size = ((init_message*)init_mr.buffer)->size;
    num_blocks = (message_size - 1) / block_size + 1;

    init_queue_pairs.at(0).post_recv(init_mr, 0, sizeof(init_message),
									 form_tag(group_number, 0),
                                     message_types.init);

    auto destination = incoming_message_upcall(message_size);
    mr_offset = destination.offset;
    mr = destination.mr;
    CHECK(mr->size >= mr_offset + message_size);
	LOG_EVENT(group_number, message_number, -1, "upcall_done");
	post_relay_task();
	LOG_EVENT(group_number, message_number, -1, "relay_task_posted");
    init_queue_pairs.at(0).post_empty_send(form_tag(group_number, 0), 0,
                                           message_types.init_ack);
	LOG_EVENT(group_number, message_number, -1, "init_acked");
}
