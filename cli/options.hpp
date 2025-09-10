#pragma once

#include <cstdint>
#include <string>

#include "probkit/hash.hpp"

namespace probkit::cli {

struct GlobalOptions {
  int threads{0};        // 0 => use hardware_concurrency
  std::string file_path; // empty => stdin
  bool json{false};
  std::uint64_t stop_after{0}; // lines; 0 => unlimited
  probkit::hashing::HashConfig hash{};
  // Observability / rotation flags (parsed; functionality may be enabled in later PRs)
  bool stats{false};
  unsigned stats_interval_seconds{5}; // default interval when --stats is present without value
  std::string bucket;                 // e.g., "30s", "1m"; empty => no rotation
  bool prom{false};
  std::string prom_path; // empty => stdout
};

auto cmd_bloom(int argc, char** argv, const GlobalOptions& g) -> int;
auto cmd_hll(int argc, char** argv, const GlobalOptions& g) -> int;
auto cmd_cms(int argc, char** argv, const GlobalOptions& g) -> int;

} // namespace probkit::cli
