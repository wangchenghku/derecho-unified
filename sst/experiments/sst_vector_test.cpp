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

void print (mySST& sst) {
  for (int i = 0; i < 2; ++i) {
    cout << sst.a[i] << " " << sst.c[i] << " ";
    for (int j = 0; j < 5; ++j) {
      cout << sst.b[i][j] << " ";
    }
    cout << endl;
  }
}

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
    cout << "Using address of: " << (char*)std::addressof(sst.b[0][0]) - sst.getBaseAddress() << endl;
    cout << "Base value: " << sst.b.get_base() - sst.getBaseAddress() << endl;
    sst.a[0] = 0;
    sst.a[1] = 1;
    sst.c[0] = 'a';
    sst.c[1] = 'b';
    for (int i = 0; i < 5; ++i) {
      sst.b[0][i] = i;
      sst.b[1][i] = i+1;
    }
    sst::sync(1-my_id);
    print(sst);
    // for(uint i = 0; i < num_nodes; ++i) {
    //     cout << sst.a(i) << endl;
    // }
    while (true) {
      int op;
      int a;
      char c;
      long long int lb;
      int pos;
      cout << "Enter option" << endl;
      cin >> op;
      switch (op) {
      case 0:
	print(sst);
	break;
      case 1:
	cin >> a;
	sst.a[my_id] = a;
	print(sst);
	sst.put();
	break;
      case 2:
	cin >> c;
	sst.c[my_id] = c;
	print(sst);
	sst.put();
	break;
      case 3:
	cin >> pos;
	cin >> lb;
	sst.b[my_id][pos] = lb;
	print(sst);
	sst.put();
	break;
      }
    }
}
