#pragma once
#include <thread>
namespace probkit::cli::util {
inline auto decide_num_workers(int requested) -> int {
  if (requested > 0) {
    return requested;
  }
  const int hw = static_cast<int>(std::thread::hardware_concurrency());
  return (hw > 0) ? hw : 1;
}
} // namespace probkit::cli::util
