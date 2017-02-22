/**
 * @file group_impl.h
 * @brief Contains implementations of all the ManagedGroup functions
 * @date Apr 22, 2016
 * @author Edward
 */


#include <mutils-serialization/SerializationSupport.hpp>

#include "group.h"

namespace derecho {

template <typename... ReplicatedObjects>
Group<ReplicatedObjects...>::Group(
        const ip_addr my_ip,
        const CallbackSet& callbacks,
        const SubgroupInfo& subgroup_info,
        const DerechoParams& derecho_params,
        std::vector<view_upcall_t> _view_upcalls,
        const int gms_port,
        Factory<ReplicatedObjects>... factories)
        : view_manager(my_ip, callbacks, subgroup_info, derecho_params, _view_upcalls, gms_port),
          rpc_manager(0, view_manager),
          raw_subgroups(construct_raw_subgroups(0, subgroup_info)) {
    //    ^ In this constructor, this is the first node to start, so my ID will be 0
    construct_objects(0, subgroup_info, factories...);
    set_up_components();
    view_manager.start();
}

template <typename... ReplicatedObjects>
Group<ReplicatedObjects...>::Group(const node_id_t my_id,
        const ip_addr my_ip,
        const ip_addr leader_ip,
        const CallbackSet& callbacks,
        const SubgroupInfo& subgroup_info,
        std::vector<view_upcall_t> _view_upcalls,
        const int gms_port,
        Factory<ReplicatedObjects>... factories)
        : Group(my_id, tcp::socket{leader_ip, gms_port},
                callbacks, subgroup_info, _view_upcalls,
                gms_port, factories...) {}

template <typename... ReplicatedObjects>
Group<ReplicatedObjects...>::Group(const node_id_t my_id,
          tcp::socket leader_connection,
          const CallbackSet& callbacks,
          const SubgroupInfo& subgroup_info,
          std::vector<view_upcall_t> _view_upcalls,
          const int gms_port,
          Factory<ReplicatedObjects>... factories)
          : view_manager(my_id, leader_connection, callbacks, subgroup_info, _view_upcalls, gms_port),
            rpc_manager(my_id, view_manager),
            raw_subgroups(construct_raw_subgroups(my_id, subgroup_info)) {
    construct_objects(my_id, subgroup_info, factories...);
    receive_objects(leader_connection);
    set_up_components();
    view_manager.start();
}

template <typename... ReplicatedObjects>
Group<ReplicatedObjects...>::Group(const std::string& recovery_filename,
        const node_id_t my_id,
        const ip_addr my_ip,
        const CallbackSet& callbacks,
        const SubgroupInfo& subgroup_info,
        std::experimental::optional<DerechoParams> _derecho_params,
        std::vector<view_upcall_t> _view_upcalls,
        const int gms_port,
        Factory<ReplicatedObjects>... factories)
        : view_manager(recovery_filename, my_id, my_ip, callbacks, subgroup_info, _derecho_params, _view_upcalls, gms_port),
          rpc_manager(my_id, view_manager),
          raw_subgroups(construct_raw_subgroups(my_id, subgroup_info)) {
    //TODO: This is the recover-from-saved-file constructor; I don't know how it will work
    construct_objects(my_id, subgroup_info, factories...);
    set_up_components();
    view_manager.start();
}

template <typename... ReplicatedObjects>
Group<ReplicatedObjects...>::~Group() {

}

template <typename... ReplicatedObjects>
std::map<uint32_t, RawSubgroup> Group<ReplicatedObjects...>::construct_raw_subgroups(
        node_id_t my_id, const SubgroupInfo& subgroup_info) {
    std::map<uint32_t, RawSubgroup> raw_subgroup_map;
    std::type_index raw_object_type(typeid(RawObject));
    for(uint32_t subgroup_index = 0;
            subgroup_index < subgroup_info.num_subgroups.at(raw_object_type);
            ++subgroup_index) {
        subgroup_id_t raw_subgroup_id = view_manager.get_subgroup_ids_by_type().at(
                {raw_object_type, subgroup_index});
        raw_subgroup_map.insert({subgroup_index, RawSubgroup(my_id, raw_subgroup_id, view_manager)});
    }
    return raw_subgroup_map;
}

template <typename... ReplicatedObjects>
void Group<ReplicatedObjects...>::set_up_components() {
    view_manager.add_view_upcall([this](std::vector<node_id_t> new_members,
            std::vector<node_id_t> old_members) {
        rpc_manager.new_view_callback(new_members, old_members);
    });
    view_manager.get_current_view().multicast_group->register_rpc_callback(
            [this](node_id_t sender, char* buf, uint32_t size) {
        rpc_manager.rpc_message_handler(sender, buf, size);
    });
    view_manager.register_send_objects_upcall([this](tcp::socket& joiner_socket){
        send_objects(joiner_socket);
    });
}

template<typename... ReplicatedObjects>
RawSubgroup& Group<ReplicatedObjects...>::get_subgroup(RawObject*, uint32_t subgroup_index) {
    return raw_subgroups.at(subgroup_index);
}

template<typename... ReplicatedObjects>
template<typename SubgroupType>
Replicated<SubgroupType>& Group<ReplicatedObjects...>::get_subgroup(SubgroupType*, uint32_t subgroup_index) {
    return replicated_objects.template get<SubgroupType>().at(subgroup_index);
}

template<typename... ReplicatedObjects>
template<typename SubgroupType>
auto& Group<ReplicatedObjects...>::get_subgroup(uint32_t subgroup_index) {
    SubgroupType* overload_selector = nullptr;
    return get_subgroup(overload_selector, subgroup_index);
}

template<typename... ReplicatedObjects>
void Group<ReplicatedObjects...>::send_objects(tcp::socket& receiver_socket) {
    std::size_t total_size = 0;
    replicated_objects.for_each([&](const auto&, const auto& objects_map){
        for(const auto& index_object_pair : objects_map) {
            if(index_object_pair.second.is_valid()) {
                total_size += index_object_pair.second.object_size();
            }
        }
    });
    mutils::post_object([&receiver_socket](const char *bytes, std::size_t size) {
        receiver_socket.write(bytes, size); },
        total_size);
    replicated_objects.for_each([&](const auto&, const auto& objects_map){
        for(const auto& index_object_pair : objects_map) {
            if(index_object_pair.second.is_valid()) {
                index_object_pair.second.send_object_raw(receiver_socket);
            }
        }
    });
}

template<typename... ReplicatedObjects>
void Group<ReplicatedObjects...>::receive_objects(tcp::socket& sender_socket) {
    std::size_t total_size;
    bool success = sender_socket.read((char*)&total_size, sizeof(size_t));
    assert(success);
    //If there are no objects to receive, don't try to receive any
    if(total_size == 0)
        return;
    char* buf = new char[total_size];
    success = sender_socket.read(buf, total_size);
    assert(success);
    size_t offset = 0;
    replicated_objects.for_each([&](const auto&, auto& objects_map){
        for(auto& index_object_pair : objects_map) {
            if(index_object_pair.second.is_valid()) {
                std::size_t bytes_read = index_object_pair.second.receive_object(buf + offset);
                offset += bytes_read;
            }
        }
    });
}

template <typename... ReplicatedObjects>
void Group<ReplicatedObjects...>::report_failure(const node_id_t who) {
    view_manager.report_failure(who);
}

template <typename... ReplicatedObjects>
void Group<ReplicatedObjects...>::leave() {
    view_manager.leave();
}

template <typename... ReplicatedObjects>
std::vector<node_id_t> Group<ReplicatedObjects...>::get_members() {
    return view_manager.get_members();
}

template <typename... ReplicatedObjects>
void Group<ReplicatedObjects...>::barrier_sync() {
    view_manager.barrier_sync();
}

template <typename... ReplicatedObjects>
void Group<ReplicatedObjects...>::debug_print_status() const {
    view_manager.debug_print_status();
}

template <typename... ReplicatedObjects>
void Group<ReplicatedObjects...>::print_log(std::ostream& output_dest) const {
    view_manager.print_log(output_dest);
}

} /* namespace derecho */