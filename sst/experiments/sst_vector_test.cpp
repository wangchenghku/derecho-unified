#include "../sst.h"
#include "../verbs.h"

using std::cin;
using std::cout;
using std::endl;
using std::vector;
using std::map;

class mySST : public sst::SST<mySST> {
public:
    mySST(const vector<uint32_t>& _members, uint32_t my_id) : SST(this, sst::SSTParams{_members, my_id}),
                                                              b(5) {
        SSTInit(a, c, b);
    }
    sst::SSTField<int> a;
    sst::SSTField<char> c;
    sst::SSTFieldVector<long long int> b;
};

int main() {
    // input number of nodes and the local node id
    uint32_t num_nodes, my_id;
    cin >> my_id >> num_nodes;

    // input the ip addresses
    map<uint32_t, std::string> ip_addrs;
    for(size_t i = 0; i < num_nodes; ++i) {
        cin >> ip_addrs[i];
    }

    // initialize the rdma resources
    sst::verbs_initialize(ip_addrs, my_id);

    vector<uint32_t> members(num_nodes);
    for(uint i = 0; i < num_nodes; ++i) {
        members[i] = i;
    }

    mySST sst(members, my_id);
    cout << "Using address of: " << std::addressof(sst.b[0][0]) << endl;
    cout << "Base value: " << const_cast<long long int*>(sst.b[0]) << endl;
    // cout << sst.a[0] << endl;
    // cout << sst.c[0] << endl;
    sst::sync(1-my_id);
    // for(uint i = 0; i < num_nodes; ++i) {
    //     cout << sst.a(i) << endl;
    // }
    while (true) {
      
    }
}
