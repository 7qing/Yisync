#pragma once

#include "yisync_node_common.hpp"

namespace yisync::node {

int run_sender(NodeOptions options);
int run_receiver(NodeOptions options);

}  // namespace yisync::node
