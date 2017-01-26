
#include "rdmc.h"
#include "schedule.h"
#include "util.h"
#include "verbs_helper.h"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <mutex>
#include <queue>
#include <random>
#include <set>
#include <string>
#include <thread>
#include <tuple>
#include <vector>

using namespace std;
using namespace rdma;

template <class T>
struct stat {
    T mean;
    T stddev;
};

struct send_stats {
    stat<double> time;  // in ms
    stat<double> bandwidth;  // in Gb/s
	stat<double> cpu_usage; // core seconds/wall time seconds

    size_t size;
    size_t block_size;
    size_t group_size;
    size_t iterations;
};

uint32_t node_rank;
uint32_t num_nodes;

unique_ptr<rdmc::barrier_group> universal_barrier_group;
uint16_t next_group_number;

send_stats measure_partially_concurrent_multicast(
    size_t size, size_t block_size, uint32_t group_size, uint32_t num_senders,
    size_t iterations, rdmc::send_algorithm type = rdmc::BINOMIAL_SEND,
    bool use_cv = true) {
    if(node_rank >= group_size) {
        // Each iteration involves two barriers: one at the start and one at the
        // end.
        for(size_t i = 0; i < iterations; i++) {
            universal_barrier_group->barrier_wait();
			universal_barrier_group->barrier_wait();
			std::this_thread::sleep_for(std::chrono::microseconds(20));
        }

        return send_stats();
    }

	std::mutex send_mutex;
	std::condition_variable send_done_cv;
	atomic<uint64_t> end_time;
	atomic<uint64_t> end_ptime;
	
    size_t num_blocks = (size - 1) / block_size + 1;
    size_t buffer_size = num_blocks * block_size;
	auto mr = make_shared<memory_region>(buffer_size * num_senders);
	char* buffer = mr->buffer;

    uint16_t base_group_number = next_group_number;
    atomic<uint32_t> sends_remaining;

    for(uint16_t i = 0u; i < num_senders; i++) {
        vector<uint32_t> members;
        for(uint32_t j = 0; j < group_size; j++) {
            members.push_back((j + i) % group_size);
        }
        CHECK(rdmc::create_group(
            base_group_number + i, members, block_size, type,
            [&mr, i, buffer_size](size_t length) -> rdmc::receive_destination {
                return {mr, buffer_size * i};
            },
            [&](char *data, size_t) {
                if(--sends_remaining == 0) {
					universal_barrier_group->barrier_wait();
					end_ptime = get_process_time();
					end_time = get_time();
					unique_lock<mutex> lk(send_mutex);
                    send_done_cv.notify_all();
                }
            },
            [group_number = base_group_number + i](optional<uint32_t>) {
                LOG_EVENT(group_number, -1, -1, "send_failed");
                CHECK(false);
            }));
    }

    vector<double> rates;
    vector<double> times;
	vector<double> cpu_usages;

    for(size_t i = 0; i < iterations; i++) {
        sends_remaining = num_senders;
		end_time = 0;
		end_ptime = 0;
		
        if(node_rank < num_senders) {
            for(size_t j = 0; j < size; j += 256)
                buffer[node_rank * buffer_size + j] = (rand() >> 5) % 256;
        }

        universal_barrier_group->barrier_wait();

		uint64_t start_ptime = get_process_time();
        uint64_t start_time = get_time();

        if(node_rank < num_senders) {
            CHECK(rdmc::send(base_group_number + node_rank, mr,
                             buffer_size * node_rank, size));
        }

		if(use_cv) {
			unique_lock<mutex> lk(send_mutex);
			send_done_cv.wait(lk, [&] { return end_time != 0; });
		} else {
            while(end_time == 0)
                /* do nothing*/;
        }

		uint64_t time_diff = end_time - start_time;
		uint64_t ptime_diff = end_ptime - start_ptime;
		rates.push_back(8.0 * size * num_senders / time_diff);
		times.push_back(1.0e-6 * time_diff);
		cpu_usages.push_back((double)ptime_diff / time_diff);
    }

    for(auto i = 0u; i < group_size; i++) {
        rdmc::destroy_group(base_group_number + i);
    }

    send_stats s;
    s.size = size;
    s.block_size = block_size;
    s.group_size = group_size;
    s.iterations = iterations;

    s.time.mean = compute_mean(times);
    s.time.stddev = compute_stddev(times);
    s.bandwidth.mean = compute_mean(rates);
    s.bandwidth.stddev = compute_stddev(rates);
	s.cpu_usage.mean = compute_mean(cpu_usages);
	s.cpu_usage.stddev = compute_stddev(cpu_usages);
    return s;
}

send_stats measure_multicast(size_t size, size_t block_size,
                             uint32_t group_size, size_t iterations,
                             rdmc::send_algorithm type = rdmc::BINOMIAL_SEND,
							 bool use_cv = true) {
	return measure_partially_concurrent_multicast(size, block_size, group_size,
												  1, iterations, type, use_cv);
}

void cosmos(float timescale){
	const size_t replication_factor = 3;
    const size_t block_size = 1024 * 1024;
    const size_t buffer_size = 1024 * 1024 * 1024;
	CHECK(num_nodes >= replication_factor + 1);

	auto mr = make_shared<memory_region>(buffer_size + block_size);
	vector<uint16_t> group_numbers;
	vector<bool> active_groups;
	vector<size_t> group_current_transfer;
	std::mutex active_mutex;
	
	atomic<size_t> buffer_offset;
	buffer_offset = 0;

	atomic<size_t> num_started_transfers;
	atomic<size_t> num_completed_transfers;
	num_started_transfers = 0;
	num_completed_transfers = 0;

	CHECK(replication_factor == 3);
	vector<std::tuple<unsigned int, unsigned int, unsigned int>> groups;
	for(unsigned int i = 1; i < num_nodes; i++) {
		for(unsigned int j = i+1; j < num_nodes; j++) {
			for(unsigned int k = j+1; k < num_nodes; k++) {
				groups.emplace_back(i, j, k);
			}
		}
	}
	size_t num_groups = groups.size();

	std::mutex ack_mutex;
	std::condition_variable ack_cv;
	unique_ptr<message_type> ack_message;
	vector<rdma::queue_pair> ack_qps;
	bool ack_qp_ready = true;
	std::queue<size_t> pending_acks;
	std::thread ack_thread;
	if(node_rank > 0) {
        ack_thread = std::thread([&] {
            while(true) {
                unique_lock<mutex> lk(ack_mutex);
				ack_cv.wait(lk, [&] { return ack_qp_ready && !pending_acks.empty(); });
				size_t group_index = pending_acks.front();

				pending_acks.pop();
				ack_qp_ready = false;
                CHECK(ack_qps[0].post_empty_recv(0, *ack_message));
                CHECK(ack_qps[0].post_empty_send(0, group_index, *ack_message));
             }
        });
    }


	std::thread cpu_monitor_thread;
	vector<float> cpu_measurements;
	atomic<bool> stop_cpu_monitor;
	stop_cpu_monitor = false;
	auto monitor_cpu = [&stop_cpu_monitor, &cpu_measurements]{
		uint64_t last_time = get_time();
		uint64_t last_ptime = get_process_time();
		
		while(!stop_cpu_monitor) {
			std::this_thread::sleep_for(std::chrono::seconds(10));

			uint64_t time = get_time();
			uint64_t ptime = get_process_time();
			cpu_measurements.emplace_back(100.0 * (ptime-last_ptime) / (time - last_time));
			last_time = time;
			last_ptime = ptime;
		}
	};
	
	
	for(auto&& g : groups) {
		uint16_t group_number = next_group_number++;
		size_t group_index = group_numbers.size();
		group_numbers.push_back(group_number);
		active_groups.push_back(false);
		group_current_transfer.push_back(0);

		
        if(node_rank != 0 && node_rank != get<0>(g) && node_rank != get<1>(g) && node_rank != get<2>(g)) {
            continue;
		}
		
		vector<uint32_t> members;
		members.push_back(0);
		members.push_back(get<0>(g));
		members.push_back(get<1>(g));
		members.push_back(get<2>(g));

		auto incoming_transfer = [&](size_t length) -> rdmc::receive_destination {
			++num_started_transfers;
            size_t offset = buffer_offset.fetch_add(length) % (buffer_size - length);
            return {mr, offset};
		};
        auto transfer_complete = [&, group_number, group_index](char *data, size_t) {
			++num_completed_transfers;

			if(node_rank > 0) {
				unique_lock<mutex> lk(ack_mutex);
				pending_acks.push(group_index);
				ack_cv.notify_all();
			}
		};
		auto transfer_failed = [group_number](optional<uint32_t>) {
			LOG_EVENT(group_number, -1, -1, "send_failed");
			CHECK(false);
		};
		
        CHECK(rdmc::create_group(group_number, members, block_size,
								 rdmc::BINOMIAL_SEND,
								 incoming_transfer, transfer_complete,
								 transfer_failed));
    }

	CHECK(num_nodes <= 32);
	struct control_message {
		struct {
			size_t total_transfers = 0;
		} nodes[64];
		size_t total_transfer_size;
		double expected_time;
	};

	vector<uint32_t> control_group_members;
	for(uint32_t i = 0; i < num_nodes; i++) control_group_members.push_back(i);
	atomic<bool> control_message_arrived;
	uint16_t control_group_number = next_group_number++;
	auto control_mr = make_shared<memory_region>(sizeof(control_message) + 4096);
	CHECK(rdmc::create_group(control_group_number, control_group_members, 4096,
							 rdmc::BINOMIAL_SEND,
							 [&](size_t){return rdmc::receive_destination{control_mr, 0ull};},
							 [&](char*, size_t){control_message_arrived = true;},
							 [](optional<uint32_t>){CHECK(false);}));

	uint64_t start_time, end_time;
	
	if(node_rank == 0) {
		control_message* message = (control_message*)(control_mr->buffer);

		auto engine = std::default_random_engine{};
		auto random_group = std::uniform_int_distribution<unsigned int>(0, num_groups-1);

		FILE *f = fopen("extentReplicas.txt", "r");
		CHECK(f != nullptr);
		
		struct transfer {
			double time;
			size_t size;
			unsigned int group_index;
			uint64_t start_time;
		};
		vector<transfer> transfers;
		double first_time = 0;
		while(transfers.size() < 20000) {
			char replicas[1024];
			unsigned long long uncompressed, compressed, level;
			unsigned int year, month, day, hour, minute;
			double second;

			if (fscanf(f, "%s\t%u-%u-%uT%u:%u:%lfZ\t%llu\t%llu\t%llu", replicas, &year,
					   &month, &day, &hour, &minute, &second, &uncompressed,
					   &compressed, &level) == EOF) {
				break;
			}

			CHECK(year == 2016);
			CHECK(month == 11);
		
			double t = ((day * 24 + hour) * 60 + minute) * 60 + second;
			if(transfers.empty()) {
				first_time = t;
			}

			unsigned int group_index = random_group(engine);
			message->nodes[get<0>(groups[group_index])].total_transfers++;
			message->nodes[get<1>(groups[group_index])].total_transfers++;
			message->nodes[get<2>(groups[group_index])].total_transfers++;
			message->total_transfer_size += compressed;
			
			transfers.emplace_back(transfer{t - first_time, compressed, group_index});
		}
		message->expected_time = transfers.back().time;

		atomic<size_t> acks_remaining;
		acks_remaining = replication_factor * transfers.size();
		vector<char> transfer_receivers_left(transfers.size(), replication_factor);
		vector<uint64_t> transfer_end_times(transfers.size(), 0);
        ack_message = make_unique<rdma::message_type>("ACK", [](uint64_t, uint32_t, size_t) {},
            [&](uint64_t tag, uint32_t imm, size_t len) {
				CHECK(ack_qps[tag].post_empty_recv(tag, *ack_message));
				CHECK(ack_qps[tag].post_empty_send(tag, 0, *ack_message));

				unique_lock<mutex> lk(active_mutex);
				size_t transfer = group_current_transfer[imm];
				CHECK(transfer_receivers_left[transfer] > 0);
				if(--transfer_receivers_left[transfer] == 0) {
					CHECK(transfer_end_times[transfer] == 0);
					transfer_end_times[transfer] = get_time();
					active_groups[imm] = false;
				}
				--acks_remaining;
			});

        for(unsigned int i = 1; i < num_nodes; i++) {
			ack_qps.emplace_back(i);
			ack_qps[i - 1].post_empty_recv(i - 1, *ack_message);
		}

		CHECK(rdmc::send(control_group_number, control_mr, 0, sizeof(control_message)));
		universal_barrier_group->barrier_wait();

		cpu_monitor_thread = std::thread(monitor_cpu);
		start_time = get_time();
		uint64_t next_send_time = start_time;

		for(size_t transfer_number = 0; transfer_number < transfers.size(); transfer_number++) {
			uint16_t group_index = transfers[transfer_number].group_index;
			while(true) {
				if(get_time() < next_send_time) {
					continue;
				}
					
				if(num_started_transfers - num_completed_transfers > 15) {
					continue;
				}
					
				unique_lock<mutex> lk(active_mutex);
				if(active_groups[group_index]) {
					continue;
				}

				{ // No more waiting is required..
					if(transfer_number == transfers.size() - 1) {
						uint64_t dt = get_time() - next_send_time;
						if(dt < 1000000) {
							puts("Last message sent ON TIME.");
						} else {
							printf("Last message sent LATE by %f ms.\n", dt * 1e-6);
						}
					}
					
					active_groups[group_index] = true;
					group_current_transfer[group_index] = transfer_number;
					transfers[transfer_number].start_time = get_time();
					size_t transfer_size = transfers[transfer_number].size;
					size_t offset = buffer_offset.fetch_add(transfer_size) % (buffer_size - transfer_size);
					CHECK(rdmc::send(group_numbers[group_index], mr, offset, transfer_size));

					if(transfer_number != transfers.size() - 1) {
						next_send_time += 1e9 * (transfers[transfer_number+1].time - transfers[transfer_number].time) / timescale;
					}
					++num_started_transfers;
					break;
				}
			}
		}
		puts("Waiting for transfers to complete");
		while(num_completed_transfers < transfers.size()) {
			std::this_thread::sleep_for(std::chrono::milliseconds(100));
		}
		puts("Waiting for rest of ACKS");
		while(acks_remaining > 0) {}
		universal_barrier_group->barrier_wait();
		end_time = get_time();

		// Nanoseconds per bin.
		const uint64_t resolution = 5000'000'000;
		
		uint64_t dt = end_time - start_time;
		unsigned long long nbins = (dt-1) / resolution + 1;
		vector<size_t> bins(nbins, 0);
		for(size_t t = 0; t < transfers.size(); t++) {
			if(transfers[t].start_time >= transfer_end_times[t]) {
				printf("END BEFORE START (start = %lu, end = %lu)\n",
					   transfers[t].start_time, transfer_end_times[t]);
				continue;
			}
			uint64_t stime = transfers[t].start_time - start_time;
			uint64_t etime = transfer_end_times[t] - start_time;

			uint64_t first_bin = stime / resolution;
			uint64_t last_bin = etime / resolution;
			for(uint64_t b = first_bin; b <= last_bin; b++) {
				uint64_t bstart = b * resolution;
				uint64_t bend = (b+1) * resolution;
				
				uint64_t s = std::max(bstart, stime);
				uint64_t e = std::min(bend, etime);
				
				bins[b] += 8.0 * transfers[t].size * (e-s) / (etime - stime);
			}
		}
		printf("Bandwidth Measurements\nTime, Bandwidth\n");
		for(size_t i = 0; i < bins.size(); i++) {
			printf("%f, %f\n", i / 60.0 * resolution * 1e-9, (double)bins[i] / resolution);
		}
	} else {
		ack_qps.emplace_back(0);
        ack_message = make_unique<rdma::message_type>("ACK", [](uint64_t, uint32_t, size_t){},
			[&] (uint64_t, uint32_t, size_t) {
				unique_lock<mutex> lk(ack_mutex);
				CHECK(ack_qp_ready == false);
				ack_qp_ready = true;
				ack_cv.notify_all();
			});

		while(!control_message_arrived);
        puts("Got Control Message");
		universal_barrier_group->barrier_wait();
		cpu_monitor_thread = std::thread(monitor_cpu);
		start_time = get_time();
		puts("Did barrier");
		while(num_completed_transfers < ((control_message*)control_mr->buffer)->nodes[node_rank].total_transfers){
			std::this_thread::sleep_for(std::chrono::milliseconds(100));
		}
		universal_barrier_group->barrier_wait();
		end_time = get_time();
	}
	

	uint64_t dt = end_time - start_time;

	stop_cpu_monitor = true;
	cpu_monitor_thread.join();

	printf("\ncpu measurements\nTime, Load\n");
	for(size_t i = 0; i < cpu_measurements.size(); i++) {
		printf("%f, %f\n", i / 6.0, cpu_measurements[i]);
	}
	
	printf("num_completed_transfers = %d\n", (int)num_completed_transfers);
	printf("num_groups = %d\n", (int)num_groups);
	printf("Latency of final message = %f ms\n",
		   (end_time - (start_time + 1e9 * ((control_message*)control_mr->buffer)->expected_time / timescale)) * 1e-6);
	printf("Total Time = %f ms. Average Bandwidth = %f\n", dt * 1e-6,
		   8.0 * ((control_message*)control_mr->buffer)->total_transfer_size / dt);
	fflush(stdout);

    for(auto g : group_numbers) {
        rdmc::destroy_group(g);
    }
}

void blocksize_v_bandwidth(uint16_t gsize) {
    const size_t min_block_size = 16ull << 10;
    const size_t max_block_size = 16ull << 20;

    puts("=========================================================");
    puts("=             Block Size vs. Bandwdith (Gb/s)           =");
    puts("=========================================================");
    printf("Group Size = %d\n", (int)gsize);
    printf("Send Size, ");
    for(auto block_size = min_block_size; block_size <= max_block_size;
        block_size *= 2) {
        if(block_size >= 1 << 20)
            printf("%d MB, ", (int)(block_size >> 20));
        else
            printf("%d KB, ", (int)(block_size >> 10));
    }
    for(auto block_size = min_block_size; block_size <= max_block_size;
        block_size *= 2) {
        if(block_size >= 1 << 20)
            printf("%d MB stddev, ", (int)(block_size >> 20));
        else
            printf("%d KB stddev, ", (int)(block_size >> 10));
    }
    puts("");
    fflush(stdout);
    for(auto size : {256ull << 20, 64ull << 20, 16ull << 20, 4ull << 20,
                     1ull << 20, 256ull << 10, 64ull << 10, 16ull << 10}) {
        if(size >= 1 << 20)
            printf("%d MB, ", (int)(size >> 20));
        else if(size >= 1 << 10)
            printf("%d KB, ", (int)(size >> 10));
        else
            printf("%d B, ", (int)(size));

        vector<double> stddevs;
        for(auto block_size = min_block_size; block_size <= max_block_size;
            block_size *= 2) {
            if(block_size > size) {
                printf(", ");
                continue;
            }
            auto s = measure_multicast(size, block_size, gsize, 8);
            printf("%f, ", s.bandwidth.mean);
            fflush(stdout);

            stddevs.push_back(s.bandwidth.stddev);
        }
        for(auto s : stddevs) {
            printf("%f, ", s);
        }
        puts("");
        fflush(stdout);
    }
    puts("");
    fflush(stdout);
}
void compare_send_types() {
    puts("=========================================================");
    puts("=         Compare Send Types - Bandwidth (Gb/s)         =");
    puts("=========================================================");
    puts(
        "Group Size,"
        "Binomial Pipeline (256 MB),Chain Send (256 MB),Sequential Send (256 "
        "MB),Tree Send (256 MB),"
        "Binomial Pipeline (64 MB),Chain Send (64 MB),Sequential Send (64 "
        "MB),Tree Send (64 MB),"
        "Binomial Pipeline (8 MB),Chain Send (8 MB),Sequential Send (8 "
        "MB),Tree Send (8 MB),");
    fflush(stdout);

    const size_t iterations = 64;
    for(int gsize = num_nodes; gsize >= 2; --gsize) {
        auto bp10 = measure_multicast(10000, 4096, gsize, iterations,
                                     rdmc::BINOMIAL_SEND);
        auto bp1 = measure_multicast(1000000, 100000, gsize, iterations,
                                      rdmc::BINOMIAL_SEND);
        auto bp100 = measure_multicast(100000000, 1 << 20, gsize, iterations,
                                       rdmc::BINOMIAL_SEND);
        auto cs10 = measure_multicast(10000, 4096, gsize, iterations,
                                     rdmc::CHAIN_SEND);
        auto cs1 = measure_multicast(1000000, 100000, gsize, iterations,
                                      rdmc::CHAIN_SEND);
        auto cs100 = measure_multicast(100000000, 1 << 20, gsize, iterations,
                                       rdmc::CHAIN_SEND);
        auto ss10 = measure_multicast(10000, 4096, gsize, iterations,
                                     rdmc::SEQUENTIAL_SEND);
        auto ss1 = measure_multicast(1000000, 100000, gsize, iterations,
                                      rdmc::SEQUENTIAL_SEND);
        auto ss100 = measure_multicast(100000000, 1 << 20, gsize, iterations,
                                       rdmc::SEQUENTIAL_SEND);
        auto ts10 = measure_multicast(10000, 4096, gsize, iterations,
                                     rdmc::TREE_SEND);
        auto ts1 = measure_multicast(1000000, 100000, gsize, iterations,
                                      rdmc::TREE_SEND);
        auto ts100 = measure_multicast(100000000, 1 << 20, gsize, iterations,
                                       rdmc::TREE_SEND);
        printf(
            "%d, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, "
            "%f, %f, %f, %f, %f, %f, %f, %f, %f\n",
            gsize, bp100.bandwidth.mean, cs100.bandwidth.mean,
            ss100.bandwidth.mean, ts100.bandwidth.mean, bp1.bandwidth.mean,
            cs1.bandwidth.mean, ss1.bandwidth.mean, ts1.bandwidth.mean,
            bp10.bandwidth.mean, cs10.bandwidth.mean, ss10.bandwidth.mean,
            ts10.bandwidth.mean, bp100.bandwidth.stddev, cs100.bandwidth.stddev,
            ss100.bandwidth.stddev, ts100.bandwidth.stddev,
            bp1.bandwidth.stddev, cs1.bandwidth.stddev, ss1.bandwidth.stddev,
            ts1.bandwidth.stddev, bp10.bandwidth.stddev, cs10.bandwidth.stddev,
            ss10.bandwidth.stddev, ts10.bandwidth.stddev);

        // ss256.bandwidth.mean, 0.0f /*ts256.bandwidth.mean*/,
        // bp64.bandwidth.mean, cs64.bandwidth.mean, ss64.bandwidth.mean,
        // 0.0f /*ts64.bandwidth.mean*/, bp256.bandwidth.stddev,
        // cs256.bandwidth.stddev, ss256.bandwidth.stddev,
        // 0.0f /*ts256.bandwidth.stddev*/, bp64.bandwidth.stddev,
        // cs64.bandwidth.stddev, ss64.bandwidth.stddev,
        // 0.0f /*ts64.bandwidth.stddev*/);
        fflush(stdout);
    }
    puts("");
    fflush(stdout);
}
void bandwidth_group_size() {
    puts("=========================================================");
    puts("=              Bandwidth vs. Group Size                 =");
    puts("=========================================================");
    puts(
        "Group Size, 256 MB, 64 MB, 16 MB, 4 MB, "
        "256stddev, 64stddev, 16stddev, 4stddev");
    fflush(stdout);

    for(int gsize = num_nodes; gsize >= 2; --gsize) {
        auto bp256 = measure_multicast(256 << 20, 1 << 20, gsize, 64,
                                       rdmc::BINOMIAL_SEND);
        auto bp64 = measure_multicast(64 << 20, 1 << 20, gsize, 64,
                                      rdmc::BINOMIAL_SEND);
        auto bp16 = measure_multicast(16 << 20, 1 << 20, gsize, 64,
                                      rdmc::BINOMIAL_SEND);
        auto bp4 =
            measure_multicast(4 << 20, 1 << 20, gsize, 64, rdmc::BINOMIAL_SEND);
        printf("%d, %f, %f, %f, %f, %f, %f, %f, %f\n", gsize,
               bp256.bandwidth.mean, bp64.bandwidth.mean, bp16.bandwidth.mean,
               bp4.bandwidth.mean, bp256.bandwidth.stddev,
               bp64.bandwidth.stddev, bp16.bandwidth.stddev,
               bp4.bandwidth.stddev);
        fflush(stdout);
    }
    puts("");
    fflush(stdout);
}
void concurrent_bandwidth_group_size() {
    puts("=========================================================");
    puts("=         Concurrent Bandwidth vs. Group Size           =");
    puts("=========================================================");
    puts(
        "Group Size, 256 MB, 64 MB, 16 MB, 4 MB, "
        "256stddev, 64stddev, 16stddev, 4stddev");
    fflush(stdout);

    for(int gsize = num_nodes; gsize >= 2; --gsize) {
        auto bp256 = measure_partially_concurrent_multicast(256 << 20, 1 << 20,
            gsize, gsize, 16, rdmc::BINOMIAL_SEND);
        auto bp64 = measure_partially_concurrent_multicast(64 << 20, 1 << 20,
            gsize, gsize, 16, rdmc::BINOMIAL_SEND);
        auto bp16 = measure_partially_concurrent_multicast(16 << 20, 1 << 20,
			gsize, gsize, 16, rdmc::BINOMIAL_SEND);
        auto bp4 = measure_partially_concurrent_multicast(4 << 20, 1 << 20,
            gsize, gsize, 16, rdmc::BINOMIAL_SEND);
        printf("%d, %f, %f, %f, %f, %f, %f, %f, %f\n", gsize,
               bp256.bandwidth.mean, bp64.bandwidth.mean, bp16.bandwidth.mean,
               bp4.bandwidth.mean, bp256.bandwidth.stddev,
               bp64.bandwidth.stddev, bp16.bandwidth.stddev,
               bp4.bandwidth.stddev);
        fflush(stdout);
    }
    puts("");
    fflush(stdout);
}
void active_senders(bool interrupts = false, bool labels = true) {
    auto compute_block_size = [](size_t message_size) -> size_t {
        if(message_size < 4096 * 2) return message_size;
        if(message_size < 4096 * 10) return 4096;
        if(message_size < 10 * (1 << 20)) return message_size / 10;
        return 1 << 20;
    };

    auto compute_iterations = [](size_t message_size) -> size_t {
        if(message_size == 1) return 20000;
        if(message_size == 10000) return 10000;
        if(message_size == 1'000'000) return 1000;
        if(message_size == 100'000'000) return 100;
        return max<size_t>(100000000 / message_size, 4u);
    };

    if(labels) {
        printf("Interrupts, Message Size, Group Size, 1-sender Bandwidth, "
			   "half-sending Bandwidth, all-sending Bandwidth, 1-sender CPU, "
			   "half-sending CPU, all-sending CPU\n");
    }

	rdma::impl::set_interrupt_mode(interrupts);
	
    for(size_t message_size : {1, 10000, 1'000'000, 100'000'000}) {
        for(uint32_t group_size = 3; group_size <= num_nodes; group_size++) {
            printf("%s, %d, %d, ", interrupts ? "enabled" : "disabled",
				   (int)message_size, (int)group_size);
            fflush(stdout);

			vector<double> cpu_usage;
            for(uint32_t num_senders : {1u, (group_size + 1) / 2, group_size}) {
                auto s = measure_partially_concurrent_multicast(
                    message_size, compute_block_size(message_size), group_size,
                    num_senders, compute_iterations(message_size),
                    rdmc::BINOMIAL_SEND);
                printf("%f, ", s.bandwidth.mean);
                fflush(stdout);
				cpu_usage.push_back(s.cpu_usage.mean * 100);
            }
			for(double usage : cpu_usage) {
                printf("%f, ", usage);
			}
            printf("\n");
            fflush(stdout);
        }
    }
}
void latency_group_size() {
    puts("=========================================================");
    puts("=               Latency vs. Group Size                  =");
    puts("=========================================================");
    puts(
        "Group Size,64 KB,16 KB,4 KB,1 KB,256 B,"
        "64stddev,16stddev,4stddev,1stddev,256stddev");
    fflush(stdout);

    size_t iterations = 10000;

    for(int gsize = num_nodes; gsize >= 2; gsize /= 2) {
        auto bp64 = measure_multicast(64 << 10, 32 << 10, gsize, iterations,
                                      rdmc::BINOMIAL_SEND);
        auto bp16 = measure_multicast(16 << 10, 8 << 10, gsize, iterations,
                                      rdmc::BINOMIAL_SEND);
        auto bp4 = measure_multicast(4 << 10, 4 << 10, gsize, iterations,
                                     rdmc::BINOMIAL_SEND);
        auto bp1 = measure_multicast(1 << 10, 1 << 10, gsize, iterations,
                                     rdmc::BINOMIAL_SEND);
        auto bp256 =
            measure_multicast(256, 256, gsize, iterations, rdmc::BINOMIAL_SEND);
        printf("%d, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f\n", gsize,
               bp64.time.mean, bp16.time.mean, bp4.time.mean, bp1.time.mean,
               bp256.time.mean, bp64.time.stddev, bp16.time.stddev,
               bp4.time.stddev, bp1.time.stddev, bp256.time.stddev);
        fflush(stdout);
    }
    puts("");
    fflush(stdout);
}
void rate() {
	puts("Group Size, One Send, Half Send, All Send");
	for(uint32_t group_size = 3; group_size <= num_nodes; group_size++) {
		printf("%d, ", (int)group_size);

		uint32_t half = (group_size + 1) / 2;
		
		auto s1 = measure_partially_concurrent_multicast(
			1, 4096, group_size, 1, 100000, rdmc::BINOMIAL_SEND);
		auto s2 = measure_partially_concurrent_multicast(
			1, 4096, group_size, half, 100000, rdmc::BINOMIAL_SEND);
		auto s3 = measure_partially_concurrent_multicast(
			1, 4096, group_size, group_size, 100000, rdmc::BINOMIAL_SEND);

		printf("%f, %f, %f\n", 1000.0 / s1.time.mean, half * 1000.0 / s2.time.mean, group_size * 1000.0 / s3.time.mean);
		fflush(stdout);
	}
}
// void small_send_latency_group_size() {
//     puts("=========================================================");
//     puts("=               Latency vs. Group Size                  =");
//     puts("=========================================================");
//     puts(
//         "Group Size, 16 KB, 4 KB, 1 KB, 256 Bytes, "
//         "64stddev, 16stddev, 4stddev, 1stddev");
//     fflush(stdout);

//     for(int gsize = num_nodes; gsize >= 2; --gsize) {
//         auto bp16 = measure_small_multicast(16 << 10, gsize, 16, 512);
//         auto bp4 = measure_small_multicast(4 << 10, gsize, 16, 512);
//         auto bp1 = measure_small_multicast(1 << 10, gsize, 16, 512);
//         auto bp256 = measure_small_multicast(256, gsize, 16, 512);
//         printf("%d, %f, %f, %f, %f, %f, %f, %f, %f\n", gsize, bp16.time.mean,
//                bp4.time.mean, bp1.time.mean, bp256.time.mean,
//                bp16.time.stddev,
//                bp4.time.stddev, bp1.time.stddev, bp256.time.stddev);
//         fflush(stdout);
//     }
//     puts("");
//     fflush(stdout);
// }
void large_send() {
    LOG_EVENT(-1, -1, -1, "start_large_send");
    auto s = measure_multicast(10000000, 1 << 20, num_nodes, 100,
                               rdmc::BINOMIAL_SEND);
    //    flush_events();
    // printf("Bandwidth = %f (%f) Gb/s\n", s.bandwidth.mean, s.bandwidth.stddev);
    printf("Latency = %f (%f) ms\n", s.time.mean, s.time.stddev);
    // uint64_t eTime = get_time();
    // double diff = 1.e-6 * (eTime - sTime);
    // printf("Percent time sending: %f%%", 100.0 * s.time.mean * 16 / diff);
    fflush(stdout);
}
void concurrent_send() {
    LOG_EVENT(-1, -1, -1, "start_concurrent_send");
    auto s = measure_partially_concurrent_multicast(256 << 20, 8 << 20,
													num_nodes, num_nodes, 64);
    //    flush_events();
    printf("Bandwidth = %f (%f) Gb/s\n", s.bandwidth.mean, s.bandwidth.stddev);
    // printf("Latency = %f(%f) ms\n", s.time.mean, s.time.stddev);
    // uint64_t eTime = get_time();
    // double diff = 1.e-6 * (eTime - sTime);
    // printf("Percent time sending: %f%%", 100.0 * s.time.mean * 16 / diff);
    fflush(stdout);
}
// void small_send() {
//     auto s = measure_small_multicast(1024, num_nodes, 4, 128);
//     printf("Latency = %.2f(%.2f) us\n", s.time.mean * 1000.0,
//            s.time.stddev * 1000.0);
//     fflush(stdout);
// }

void test_cross_channel() {
	if(node_rank > 1) {
		return;
	}
	
    static volatile atomic<bool> done_flag;
    done_flag = false;

    auto nop_handler = [](auto, auto, auto) {};
    auto done_handler = [](uint64_t tag, uint32_t immediate, size_t length) {
        if(tag == 0x6000000) done_flag = true;
    };

    static message_type mtype_done("ccc.done", nop_handler, nop_handler,
                                   done_handler);

    const int steps = 128;
	const size_t chunk_size = 1024;
    const size_t buffer_size = (steps + 1) * chunk_size;

    // Setup memory region
    memory_region mr{buffer_size};
    memset(mr.buffer, 1 + node_rank, buffer_size);
    memset(mr.buffer, node_rank * 10 + 10, chunk_size);
    mr.buffer[buffer_size - 1] = 0;

	auto mqp = make_shared<manager_queue_pair>();
    managed_queue_pair qp(node_rank == 0 ? 1 : 0, [&](managed_queue_pair *qp) {
        qp->post_recv(mr, chunk_size, chunk_size, 125, mtype_done);
    });

	rdma::task t(mqp);
	for(int i = 1; i < steps; i++) {
		t.append_recv(qp, mr, (i+1) * chunk_size, chunk_size);
	}
	for(int i = 0; i < steps; i++) {
		t.append_send(qp, mr, i * chunk_size, chunk_size, 0);
	}
	for(int i = 0; i < steps; i++) {
        t.append_enable_send(qp, i + 1);
        t.append_wait(qp.rcq, i + 1, false, false, 0x321000 + i, mtype_done);
    }
    t.append_wait(qp.scq, 0, true, true, 0x6000000, mtype_done);
    CHECK(t.post());

    while(!done_flag) {
    }

    // std::this_thread::sleep_for(std::chrono::seconds(1));
	// for(int i = 0; i < steps && i < 16; i++) {
	// 	printf("%2d ", mr.buffer[i * chunk_size]);
	// }
	// printf("\n");
	
	puts("PASS");
}

void test_create_group_failure() {
    if(num_nodes <= 2) {
        puts("FAILURE: must run with at least 3 nodes");
    }
    if(node_rank == 0) {
        puts("Node 0 exiting...");
        exit(0);
    }

    std::this_thread::sleep_for(std::chrono::seconds(1));

    vector<uint32_t> members;
    for(uint32_t i = 0; i < num_nodes; i++) {
        members.push_back(i);
    }

    puts("Starting test...");
    uint64_t t = get_time();
    bool ret = rdmc::create_group(
        0, members, 1 << 20, rdmc::BINOMIAL_SEND,
        [&](size_t length) -> rdmc::receive_destination {
            puts("FAILURE: incoming message called");
            return {nullptr, 0};
        },
        [&](char *data, size_t) { puts("FAILURE: received message called"); },
        [group_number = next_group_number](optional<uint32_t>){});

    t = get_time() - t;
    if(ret) {
        puts("FAILURE: Managed to create group containing failed node");
    } else {
        printf("time taken: %f ms\n", t * 1e-6);
        puts("PASS");
    }
}

#define ANSI_COLOR_RED "\x1b[31m"
#define ANSI_COLOR_GREEN "\x1b[32m"
#define ANSI_COLOR_YELLOW "\x1b[33m"
#define ANSI_COLOR_BLUE "\x1b[34m"
#define ANSI_COLOR_MAGENTA "\x1b[35m"
#define ANSI_COLOR_CYAN "\x1b[36m"
#define ANSI_COLOR_RESET "\x1b[0m"
void test_pattern() {
    size_t n = 0;
    auto t = get_time();
    for(size_t group_size = 2; group_size <= 64; group_size++) {
        for(size_t message_size = 1; message_size <= 32; message_size++) {
            size_t total_steps = message_size + ceil(log2(group_size)) - 1;
            for(unsigned int step = 0; step < total_steps; step++) {
                for(unsigned int node = 0; node < group_size; node++) {
                    // Compute the outgoing transfer for this node/step
                    auto transfer = binomial_schedule::get_outgoing_transfer(
                        node, step, group_size, floor(log2(group_size)),
                        message_size, total_steps);
                    n++;

                    if(transfer) {
                        // See what the supposed sender is doing this step
                        auto reverse = binomial_schedule::get_incoming_transfer(
                            transfer->target, step, group_size,
                            floor(log2(group_size)), message_size, total_steps);
                        n++;

                        // Make sure the two nodes agree
                        if(!reverse) throw false;
                        if(transfer->block_number != reverse->block_number)
                            throw false;

                        // If we aren't the root sender, also check that the
                        // node got this block on a past step.
                        if(node != 0) {
                            for(int s = step - 1; s >= 0; s--) {
                                auto prev =
                                    binomial_schedule::get_incoming_transfer(
                                        node, s, group_size,
                                        floor(log2(group_size)), message_size,
                                        total_steps);
                                n++;
                                if(prev &&
                                   prev->block_number == transfer->block_number)
                                    break;

                                if(s == 0) {
                                    throw false;
                                }
                            }
                        }
                    }

                    // Compute the incoming transfer for this node/step
                    transfer = binomial_schedule::get_incoming_transfer(
                        node, step, group_size, floor(log2(group_size)),
                        message_size, total_steps);
                    n++;

                    if(transfer) {
                        // Again make sure the supposed receiver agrees
                        auto reverse = binomial_schedule::get_outgoing_transfer(
                            transfer->target, step, group_size,
                            floor(log2(group_size)), message_size, total_steps);
                        n++;
                        if(!reverse) throw false;
                        if(transfer->block_number != reverse->block_number)
                            throw false;
                        if(reverse->target != node) throw false;

                        // Make sure we don't already have the block we're
                        // getting.
                        for(int s = step - 1; s >= 0; s--) {
                            auto prev = binomial_schedule::get_incoming_transfer(
                                node, s, group_size, floor(log2(group_size)),
                                message_size, total_steps);
                            n++;
                            if(prev &&
                               prev->block_number == transfer->block_number) {
                                throw false;
                            }
                        }
                    }
                }
            }

            // Make sure that all nodes get every block
            for(unsigned int node = 1; node < group_size; node++) {
                set<size_t> blocks;
                for(unsigned int step = 0; step < total_steps; step++) {
                    auto transfer = binomial_schedule::get_incoming_transfer(
                        node, step, group_size, floor(log2(group_size)),
                        message_size, total_steps);
                    n++;

                    if(transfer) blocks.insert(transfer->block_number);
                }
                if(blocks.size() != message_size) throw false;
            }
        }
    }
    auto diff = get_time() - t;
    printf("average time = %f ns\n", (double)diff / n);
    puts("PASS");
}

int main(int argc, char *argv[]) {
    // rlimit rlim;
    // rlim.rlim_cur = RLIM_INFINITY;
    // rlim.rlim_max = RLIM_INFINITY;
    // setrlimit(RLIMIT_CORE, &rlim);

    if(argc >= 2 && strcmp(argv[1], "test_pattern") == 0) {
        test_pattern();
        exit(0);
    } else if(argc >= 2 && strcmp(argv[1], "spin") == 0) {
        volatile bool b = true;
		while(b);
		CHECK(false);
	}

    LOG_EVENT(-1, -1, -1, "querying_addresses");
    map<uint32_t, string> addresses;
	rdmc::query_addresses(addresses, node_rank);
    num_nodes = addresses.size();

    LOG_EVENT(-1, -1, -1, "calling_init");
    assert(rdmc::initialize(addresses, node_rank));

    LOG_EVENT(-1, -1, -1, "creating_barrier_group");
    vector<uint32_t> members;
    for(uint32_t i = 0; i < num_nodes; i++) members.push_back(i);
    universal_barrier_group = make_unique<rdmc::barrier_group>(members);

    universal_barrier_group->barrier_wait();
    uint64_t t1 = get_time();
    universal_barrier_group->barrier_wait();
    uint64_t t2 = get_time();
    reset_epoch();
    universal_barrier_group->barrier_wait();
    uint64_t t3 = get_time();

    printf(
        "Synchronized clocks.\nTotal possible variation = %5.3f us\n"
        "Max possible variation from local = %5.3f us\n",
        (t3 - t1) * 1e-3f, max(t2 - t1, t3 - t2) * 1e-3f);
    fflush(stdout);

    TRACE("Finished initializing.");

    printf("Experiment Name: %s\n", argv[1]);
    if(argc <= 1 || strcmp(argv[1], "custom") == 0) {
        for(int i = 0; i < 3; i++) {
            large_send();
        }
    } else if(strcmp(argv[1], "blocksize4") == 0) {
        blocksize_v_bandwidth(4);
    } else if(strcmp(argv[1], "blocksize16") == 0) {
        blocksize_v_bandwidth(16);
    } else if(strcmp(argv[1], "sendtypes") == 0) {
        compare_send_types();
    } else if(strcmp(argv[1], "bandwidth") == 0) {
        bandwidth_group_size();
    } else if(strcmp(argv[1], "overhead") == 0) {
        latency_group_size();
    } else if(strcmp(argv[1], "rate") == 0) {
		rate();
    } else if(strcmp(argv[1], "concurrent") == 0) {
        concurrent_bandwidth_group_size();
    } else if(strcmp(argv[1], "active_senders") == 0) {
		active_senders();
	} else if(strcmp(argv[1], "polling_interrupts") == 0) {
		active_senders(false, true);
		active_senders(true, false);
	} else if(strcmp(argv[1], "cosmos") == 0) {
		rdma::impl::set_interrupt_mode(true);
		cosmos(20);
    } else if(strcmp(argv[1], "test_create_group_failure") == 0) {
        test_create_group_failure();
        exit(0);
	} else if(strcmp(argv[1], "test_cross_channel") == 0) {
		test_cross_channel();
		exit(0);
    } else {
        puts("Unrecognized experiment name.");
        fflush(stdout);
    }

    TRACE("About to trigger shutdown");
    universal_barrier_group->barrier_wait();
    universal_barrier_group.reset();
    rdmc::shutdown();
}
