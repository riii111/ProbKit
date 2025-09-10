#include "options.hpp"
#include "probkit/hash.hpp"
#include "util/string_utils.hpp"
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

// Safe argv access to avoid pointer arithmetic warnings
inline auto safe_argv_at(char** argv, int argc, int index) -> char* {
  if (index >= 0 && index < argc) {
    // Use std::next to avoid direct pointer arithmetic
    return *std::next(argv, index);
  }
  return nullptr;
}

// Safe argv pointer access for subcommands
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
             "  --threads=<N>          number of worker threads (default: HW threads)\n"
             "  --file=<path>         read from file (default: stdin)\n"
             "  --json                 machine-readable output\n"
             "  --hash=wyhash|xxhash  hash algorithm\n"
             "  --stop-after=<count>  stop after processing N lines\n",
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

[[nodiscard]] inline auto parse_global_options(int argc, char** argv, probkit::cli::GlobalOptions& g) -> int {
  using probkit::hashing::HashKind;

  int argi = 1;
  for (; argi < argc; ++argi) {
    char* arg_ptr = safe_argv_at(argv, argc, argi);
    if (arg_ptr == nullptr) {
      break;
    }
    std::string_view a{arg_ptr};
    if (a.empty() || a.front() != '-') {
      break; // reached subcommand
    }
    if (a == std::string_view{"--help"}) {
      print_root_help();
      return 0;
    }
    if (sv_starts_with(a, std::string_view{"--threads="})) {
      std::uint64_t v = 0;
      auto val = a;
      val.remove_prefix(std::string_view{"--threads="}.size());
      if (!parse_u64(val, v) || v == 0 || v > 1024) {
        std::fputs("error: invalid --threads value\n", stderr);
        return -1;
      }
      g.threads = static_cast<int>(v);
      continue;
    }
    if (sv_starts_with(a, std::string_view{"--file="})) {
      auto val = a;
      val.remove_prefix(std::string_view{"--file="}.size());
      g.file_path = std::string(val);
      continue;
    }
    if (a == std::string_view{"--json"}) {
      g.json = true;
      continue;
    }
    if (sv_starts_with(a, std::string_view{"--hash="})) {
      auto algo = a;
      algo.remove_prefix(std::string_view{"--hash="}.size());
      HashKind k{};
      if (!parse_hash_kind(algo, k)) {
        std::fputs("error: unknown --hash value\n", stderr);
        return -1;
      }
      g.hash.kind = k;
      continue;
    }
    if (sv_starts_with(a, std::string_view{"--stop-after="})) {
      std::uint64_t v = 0;
      auto val = a;
      val.remove_prefix(std::string_view{"--stop-after="}.size());
      if (!parse_u64(val, v)) {
        std::fputs("error: invalid --stop-after value\n", stderr);
        return -1;
      }
      g.stop_after = v;
      continue;
    }
    std::fprintf(stderr, "error: unknown option: %.*s\n", static_cast<int>(a.size()), a.data());
    return -1;
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
