#pragma once

#include "node/yisync_node_common.hpp"

namespace yisync::node {

int run_sender(T_NodeOptions options);
int run_receiver(T_NodeOptions options);

}  // namespace yisync::node
