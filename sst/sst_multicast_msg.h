#pragma once

#include "max_sst_msg_size.h"

struct Message {
    char buf[max_sst_msg_size];
    uint32_t size;
    uint64_t next_seq;
};

