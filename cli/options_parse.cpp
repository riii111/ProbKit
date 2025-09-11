#include "options_parse.hpp"

#include "probkit/hash.hpp"
#include "util/parse.hpp"
#include "util/string_utils.hpp"

#include <array>
#include <cstdio>
#include <iterator>
#include <string>
#include <string_view>

using probkit::cli::OptionResult;
using probkit::cli::util::parse_u64;
using probkit::cli::util::sv_starts_with;
using probkit::hashing::parse_hash_kind;

namespace probkit::cli {
namespace {

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

inline auto safe_argv_at(char** argv, int argc, int index) -> char* {
  if (index >= 0 && index < argc) {
    return *std::next(argv, index);
  }
  return nullptr;
}

} // namespace

[[nodiscard]] auto parse_global_options(int argc, char** argv, probkit::cli::GlobalOptions& g) -> ParseResult {
  int argi = 1;
  for (; argi < argc; ++argi) {
    char* arg_ptr = safe_argv_at(argv, argc, argi);
    if (arg_ptr == nullptr) {
      break;
    }
    std::string_view a{arg_ptr};
    const OptionResult r = process_global_option(a, g);
    if (r == OptionResult::HelpShown) {
      return ParseResult{.status = ExitCode::Success, /*next_index=*/.next_index = -1};
    }
    if (r == OptionResult::Error) {
      return ParseResult{.status = ExitCode::ArgumentError, /*next_index=*/.next_index = -1};
    }
    if (r == OptionResult::SubcommandStart) {
      break;
    }
  }
  return ParseResult{.status = ExitCode::Success, .next_index = argi};
}

} // namespace probkit::cli
