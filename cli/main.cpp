#include "options.hpp"
#include "probkit/hash.hpp"
#include "util/string_utils.hpp"
#include <array>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iterator>
#include <limits>
#include <string>
#include <string_view>

using probkit::cli::util::sv_starts_with;
using probkit::hashing::parse_hash_kind;

namespace probkit::cli {} // namespace probkit::cli

namespace {

inline auto safe_argv_at(char** argv, int argc, int index) -> char* {
  if (index >= 0 && index < argc) {
    return *std::next(argv, index);
  }
  return nullptr;
}

inline auto safe_argv_from(char** argv, int argc, int start_index) -> char** {
  if (start_index >= 0 && start_index < argc) {
    return std::next(argv, start_index);
  }
  return nullptr;
}

inline void print_root_help() {
  std::fputs("probkit: approximate stream summarization (Bloom/HLL/CMS)\n"
             "usage: probkit <subcommand> [global-options] [subcommand-options]\n"
             "  subcommands: hll | bloom | cms\n\n"
             "global-options:\n"
             "  --threads=<N>           number of worker threads (default: HW threads)\n"
             "  --file=<path>          read from file (default: stdin)\n"
             "  --json                  machine-readable output\n"
             "  --hash=wyhash|xxhash   hash algorithm\n"
             "  --stop-after=<count>   stop after processing N lines\n"
             "  --stats[=<seconds>]    print periodic stats (default interval: 5s)\n"
             "  --bucket=<dur>         output per time-bucket (e.g., 30s, 1m)\n"
             "  --prom[=<path>]        emit Prometheus textfile (to path or stdout)\n",
             stdout);
}

[[nodiscard]] inline auto parse_u64(std::string_view s, std::uint64_t& out) noexcept -> bool {
  if (s.empty()) {
    return false;
  }
  std::uint64_t value = 0;
  for (char i : s) {
    const auto ch = static_cast<unsigned char>(i);
    if (ch < '0' || ch > '9') {
      return false;
    }
    const auto digit = static_cast<std::uint64_t>(ch - '0');
    if (value > (std::numeric_limits<std::uint64_t>::max() - digit) / 10ULL) {
      return false; // overflow
    }
    value = (value * 10ULL) + digit;
  }
  out = value;
  return true;
}

using HandlerFn = int (*)(std::string_view, probkit::cli::GlobalOptions&);

inline auto handle_json(std::string_view a, probkit::cli::GlobalOptions& g) -> int {
  if (a == "--json") {
    g.json = true;
    return 1;
  }
  return 2;
}
inline auto handle_threads(std::string_view a, probkit::cli::GlobalOptions& g) -> int {
  if (!sv_starts_with(a, "--threads=")) {
    return 2;
  }
  std::uint64_t v = 0;
  auto val = a;
  val.remove_prefix(std::string_view{"--threads="}.size());
  if (!parse_u64(val, v) || v == 0 || v > 1024) {
    std::fputs("error: invalid --threads value\n", stderr);
    return -1;
  }
  g.threads = static_cast<int>(v);
  return 1;
}
inline auto handle_file(std::string_view a, probkit::cli::GlobalOptions& g) -> int {
  if (!sv_starts_with(a, "--file=")) {
    return 2;
  }
  auto val = a;
  val.remove_prefix(std::string_view{"--file="}.size());
  g.file_path = std::string(val);
  return 1;
}
inline auto handle_hash(std::string_view a, probkit::cli::GlobalOptions& g) -> int {
  if (!sv_starts_with(a, "--hash=")) {
    return 2;
  }
  auto algo = a;
  algo.remove_prefix(std::string_view{"--hash="}.size());
  probkit::hashing::HashKind k{};
  if (!parse_hash_kind(algo, k)) {
    std::fputs("error: unknown --hash value\n", stderr);
    return -1;
  }
  g.hash.kind = k;
  return 1;
}
inline auto handle_stop_after(std::string_view a, probkit::cli::GlobalOptions& g) -> int {
  if (!sv_starts_with(a, "--stop-after=")) {
    return 2;
  }
  std::uint64_t v = 0;
  auto val = a;
  val.remove_prefix(std::string_view{"--stop-after="}.size());
  if (!parse_u64(val, v)) {
    std::fputs("error: invalid --stop-after value\n", stderr);
    return -1;
  }
  g.stop_after = v;
  return 1;
}
inline auto handle_stats(std::string_view a, probkit::cli::GlobalOptions& g) -> int {
  if (a == "--stats") {
    g.stats = true;
    g.stats_interval_seconds = 5U;
    return 1;
  }
  if (sv_starts_with(a, "--stats=")) {
    std::uint64_t v = 0;
    auto val = a;
    val.remove_prefix(std::string_view{"--stats="}.size());
    if (!parse_u64(val, v) || v == 0 || v > 3600) {
      std::fputs("error: invalid --stats value (1..3600)\n", stderr);
      return -1;
    }
    g.stats = true;
    g.stats_interval_seconds = static_cast<unsigned>(v);
    return 1;
  }
  return 2;
}
inline auto handle_bucket(std::string_view a, probkit::cli::GlobalOptions& g) -> int {
  if (!sv_starts_with(a, "--bucket=")) {
    return 2;
  }
  auto val = a;
  val.remove_prefix(std::string_view{"--bucket="}.size());
  if (val.empty()) {
    std::fputs("error: invalid --bucket value\n", stderr);
    return -1;
  }
  g.bucket = std::string(val);
  return 1;
}
inline auto handle_prom(std::string_view a, probkit::cli::GlobalOptions& g) -> int {
  if (a == "--prom") {
    g.prom = true;
    g.prom_path.clear();
    return 1;
  }
  if (sv_starts_with(a, "--prom=")) {
    auto val = a;
    val.remove_prefix(std::string_view{"--prom="}.size());
    g.prom = true;
    g.prom_path = std::string(val);
    return 1;
  }
  return 2;
}
constexpr std::array<HandlerFn, 8> kGlobalHandlers{handle_json,       handle_threads, handle_file,   handle_hash,
                                                   handle_stop_after, handle_stats,   handle_bucket, handle_prom};

inline auto process_global_option(std::string_view a, probkit::cli::GlobalOptions& g) -> int {
  if (a.empty() || a.front() != '-') {
    return 3; // subcommand start
  }
  if (a == "--help") {
    print_root_help();
    return 0;
  }
  for (auto fn : kGlobalHandlers) {
    const int r = fn(a, g);
    if (r != 2) {
      return r; // 1=handled / -1=error
    }
  }
  std::fprintf(stderr, "error: unknown option: %.*s\n", (int)a.size(), a.data());
  return -1;
}

[[nodiscard]] inline auto parse_global_options(int argc, char** argv, probkit::cli::GlobalOptions& g) -> int {
  using probkit::hashing::HashKind;

  int argi = 1;
  for (; argi < argc; ++argi) {
    char* arg_ptr = safe_argv_at(argv, argc, argi);
    if (arg_ptr == nullptr) {
      break;
    }
    std::string_view a{arg_ptr};
    const int r = process_global_option(a, g);
    if (r == 0) {
      return 0; // help shown
    }
    if (r == -1) {
      return -1; // error
    }
    if (r == 3) {
      break; // subcommand reached
    }
  }
  return argi;
}

[[nodiscard]] inline auto dispatch_command(int argc, char** argv, int cmd_start, const probkit::cli::GlobalOptions& g)
    -> int {
  if (cmd_start >= argc) {
    print_root_help();
    return 0;
  }

  char* cmd_ptr = safe_argv_at(argv, argc, cmd_start);
  if (cmd_ptr == nullptr) {
    print_root_help();
    return 1;
  }
  const std::string_view cmd{cmd_ptr};
  const int cmd_argc = argc - cmd_start - 1;
  char** cmd_argv = safe_argv_from(argv, argc, cmd_start + 1);

  if (cmd == std::string_view{"bloom"}) {
    return probkit::cli::cmd_bloom(cmd_argc, cmd_argv, g);
  }
  if (cmd == std::string_view{"hll"}) {
    return probkit::cli::cmd_hll(cmd_argc, cmd_argv, g);
  }
  if (cmd == std::string_view{"cms"}) {
    return probkit::cli::cmd_cms(cmd_argc, cmd_argv, g);
  }

  std::fputs("error: unknown subcommand\n", stderr);
  print_root_help();
  return 2;
}

} // namespace

auto main(int argc, char** argv) -> int {
  probkit::cli::GlobalOptions g{};

  if (argc <= 1) {
    print_root_help();
    return 0;
  }

  // NOLINTBEGIN(cppcoreguidelines-pro-bounds-pointer-arithmetic)
  const int cmd_start = parse_global_options(argc, argv, g);

  if (cmd_start == 0) {
    return 0; // help was shown
  }

  if (cmd_start < 0) {
    return 2; // error occurred
  }

  const int result = dispatch_command(argc, argv, cmd_start, g);
  return result;
  // NOLINTEND(cppcoreguidelines-pro-bounds-pointer-arithmetic)
}
