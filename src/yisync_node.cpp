#include "yisync_node_apps.hpp"

#include <exception>
#include <iostream>

int main(int argc, char** argv) {
  std::cout.setf(std::ios::unitbuf);
  std::cerr.setf(std::ios::unitbuf);
  try {
    const auto options = yisync::node::parse_options(argc, argv);
    if (options.mode == "receiver") {
      return yisync::node::run_receiver(options);
    }
    return yisync::node::run_sender(options);
  } catch (const std::exception& ex) {
    std::cerr << "error: " << ex.what() << "\n";
    return 1;
  }
}
