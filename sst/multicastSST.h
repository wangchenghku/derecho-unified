#include "max_sst_msg_size.h"
#include "sst.h"
#include "sst_multicast_msg.h"

using namespace sst;

class multicastSST : public SST<multicastSST> {
public:
    SSTFieldVector<Message> slots;
    SSTFieldVector<uint64_t> num_received_sst;
    SSTField<bool> heartbeat;
    multicastSST(const SSTParams& parameters, uint32_t window_size)
            : SST<multicastSST>(this, parameters),
              slots(window_size),
              num_received_sst(parameters.members.size()) {
        SSTInit(slots, num_received_sst, heartbeat);
    }
};
