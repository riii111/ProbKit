#include "options.hpp"
#include "probkit/hash.hpp"
#include "util/parse.hpp"
#include "util/string_utils.hpp"
#include <array>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iterator>
#include <string>
#include <string_view>

using probkit::cli::CommandResult;
using probkit::cli::ExitCode;
using probkit::cli::OptionResult;
using probkit::cli::to_int;
using probkit::cli::util::parse_u64;
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

using HandlerFn = OptionResult (*)(std::string_view, probkit::cli::GlobalOptions&);

inline auto handle_json(std::string_view a, probkit::cli::GlobalOptions& g) -> OptionResult {
  if (a == "--json") {
    g.json = true;
    return OptionResult::Handled;
  }
  return OptionResult::NotHandled;
}
inline auto handle_threads(std::string_view a, probkit::cli::GlobalOptions& g) -> OptionResult {
  if (!sv_starts_with(a, "--threads=")) {
    return OptionResult::NotHandled;
  }
  std::uint64_t v = 0;
  auto val = a;
  val.remove_prefix(std::string_view{"--threads="}.size());
  if (!parse_u64(val, v) || v == 0 || v > 1024) {
    std::fputs("error: invalid --threads value\n", stderr);
    return OptionResult::Error;
  }
  g.threads = static_cast<int>(v);
  return OptionResult::Handled;
}
inline auto handle_file(std::string_view a, probkit::cli::GlobalOptions& g) -> OptionResult {
  if (!sv_starts_with(a, "--file=")) {
    return OptionResult::NotHandled;
  }
  auto val = a;
  val.remove_prefix(std::string_view{"--file="}.size());
  g.file_path = std::string(val);
  return OptionResult::Handled;
}
inline auto handle_hash(std::string_view a, probkit::cli::GlobalOptions& g) -> OptionResult {
  if (!sv_starts_with(a, "--hash=")) {
    return OptionResult::NotHandled;
  }
  auto algo = a;
  algo.remove_prefix(std::string_view{"--hash="}.size());
  probkit::hashing::HashKind k{};
  if (!parse_hash_kind(algo, k)) {
    std::fputs("error: unknown --hash value\n", stderr);
    return OptionResult::Error;
  }
  g.hash.kind = k;
  return OptionResult::Handled;
}
inline auto handle_stop_after(std::string_view a, probkit::cli::GlobalOptions& g) -> OptionResult {
  if (!sv_starts_with(a, "--stop-after=")) {
    return OptionResult::NotHandled;
  }
  std::uint64_t v = 0;
  auto val = a;
  val.remove_prefix(std::string_view{"--stop-after="}.size());
  if (!parse_u64(val, v)) {
    std::fputs("error: invalid --stop-after value\n", stderr);
    return OptionResult::Error;
  }
  g.stop_after = v;
  return OptionResult::Handled;
}
inline auto handle_stats(std::string_view a, probkit::cli::GlobalOptions& g) -> OptionResult {
  if (a == "--stats") {
    g.stats = true;
    g.stats_interval_seconds = 5U;
    return OptionResult::Handled;
  }
  if (sv_starts_with(a, "--stats=")) {
    std::uint64_t v = 0;
    auto val = a;
    val.remove_prefix(std::string_view{"--stats="}.size());
    if (!parse_u64(val, v) || v == 0 || v > 3600) {
      std::fputs("error: invalid --stats value (1..3600)\n", stderr);
      return OptionResult::Error;
    }
    g.stats = true;
    g.stats_interval_seconds = static_cast<unsigned>(v);
    return OptionResult::Handled;
  }
  return OptionResult::NotHandled;
}
inline auto handle_bucket(std::string_view a, probkit::cli::GlobalOptions& g) -> OptionResult {
  if (!sv_starts_with(a, "--bucket=")) {
    return OptionResult::NotHandled;
  }
  auto val = a;
  val.remove_prefix(std::string_view{"--bucket="}.size());
  if (val.empty()) {
    std::fputs("error: invalid --bucket value\n", stderr);
    return OptionResult::Error;
  }
  g.bucket = std::string(val);
  return OptionResult::Handled;
}
inline auto handle_prom(std::string_view a, probkit::cli::GlobalOptions& g) -> OptionResult {
  if (a == "--prom") {
    g.prom = true;
    g.prom_path.clear();
    return OptionResult::Handled;
  }
  if (sv_starts_with(a, "--prom=")) {
    auto val = a;
    val.remove_prefix(std::string_view{"--prom="}.size());
    g.prom = true;
    g.prom_path = std::string(val);
    return OptionResult::Handled;
  }
  return OptionResult::NotHandled;
}
inline auto handle_mem_budget(std::string_view a, probkit::cli::GlobalOptions& g) -> OptionResult {
  if (!sv_starts_with(a, "--mem-budget=")) {
    return OptionResult::NotHandled;
  }
  std::uint64_t v = 0;
  auto val = a;
  val.remove_prefix(std::string_view{"--mem-budget="}.size());
  if (!parse_u64(val, v)) {
    std::fputs("error: invalid --mem-budget value\n", stderr);
    return OptionResult::Error;
  }
  g.mem_budget_bytes = v;
  return OptionResult::Handled;
}

constexpr std::array<HandlerFn, 9> kGlobalHandlers{handle_json,   handle_threads,    handle_file,
                                                   handle_hash,   handle_stop_after, handle_stats,
                                                   handle_bucket, handle_prom,       handle_mem_budget};

inline auto process_global_option(std::string_view a, probkit::cli::GlobalOptions& g) -> OptionResult {
  if (a.empty() || a.front() != '-') {
    return OptionResult::SubcommandStart;
  }
  if (a == "--help") {
    print_root_help();
    return OptionResult::HelpShown;
  }
  for (auto fn : kGlobalHandlers) {
    const OptionResult r = fn(a, g);
    if (r != OptionResult::NotHandled) {
      return r;
    }
  }
  std::fprintf(stderr, "error: unknown option: %.*s\n", (int)a.size(), a.data());
  return OptionResult::Error;
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
    const OptionResult r = process_global_option(a, g);
    if (r == OptionResult::HelpShown) {
      return to_int(ExitCode::Success);
    }
    if (r == OptionResult::Error) {
      return to_int(ExitCode::ArgumentError);
    }
    if (r == OptionResult::SubcommandStart) {
      break;
    }
  }
  return argi;
}

[[nodiscard]] inline auto dispatch_command(int argc, char** argv, int cmd_start, const probkit::cli::GlobalOptions& g)
    -> ExitCode {
  if (cmd_start >= argc) {
    print_root_help();
    return ExitCode::Success;
  }

  char* cmd_ptr = safe_argv_at(argv, argc, cmd_start);
  if (cmd_ptr == nullptr) {
    print_root_help();
    return ExitCode::GeneralError;
  }
  const std::string_view cmd{cmd_ptr};
  const int cmd_argc = argc - cmd_start - 1;
  char** cmd_argv = safe_argv_from(argv, argc, cmd_start + 1);

  if (cmd == std::string_view{"bloom"}) {
    const CommandResult result = probkit::cli::cmd_bloom(cmd_argc, cmd_argv, g);
    return (result == CommandResult::Success) ? ExitCode::Success : ExitCode::GeneralError;
  }
  if (cmd == std::string_view{"hll"}) {
    const CommandResult result = probkit::cli::cmd_hll(cmd_argc, cmd_argv, g);
    return (result == CommandResult::Success) ? ExitCode::Success : ExitCode::GeneralError;
  }
  if (cmd == std::string_view{"cms"}) {
    const CommandResult result = probkit::cli::cmd_cms(cmd_argc, cmd_argv, g);
    return (result == CommandResult::Success) ? ExitCode::Success : ExitCode::GeneralError;
  }

  std::fputs("error: unknown subcommand\n", stderr);
  print_root_help();
  return ExitCode::ArgumentError;
}

} // namespace

auto main(int argc, char** argv) -> int {
  probkit::cli::GlobalOptions g{};

  if (argc <= 1) {
    print_root_help();
    return to_int(ExitCode::Success);
  }

  // NOLINTBEGIN(cppcoreguidelines-pro-bounds-pointer-arithmetic)
  const int cmd_start = parse_global_options(argc, argv, g);

  if (cmd_start == to_int(ExitCode::Success)) {
    return to_int(ExitCode::Success);
  }

  if (cmd_start < 0) {
    return to_int(ExitCode::ArgumentError);
  }

  const ExitCode result = dispatch_command(argc, argv, cmd_start, g);
  return to_int(result);
  // NOLINTEND(cppcoreguidelines-pro-bounds-pointer-arithmetic)
}
