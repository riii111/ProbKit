#include "options.hpp"
#include "options_parse.hpp"
#include <array>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iterator>
#include <string_view>

using probkit::cli::CommandResult;
using probkit::cli::ExitCode;
using probkit::cli::to_int;

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

struct SubCmd {
  std::string_view name;
  CommandResult (*fn)(int, char**, const probkit::cli::GlobalOptions&);
};

constexpr std::array<SubCmd, 3> kSubCmds{{
    {.name = "bloom", .fn = probkit::cli::cmd_bloom},
    {.name = "hll", .fn = probkit::cli::cmd_hll},
    {.name = "cms", .fn = probkit::cli::cmd_cms},
}}; // std::array to avoid C-style array warning

[[nodiscard]] inline auto dispatch_command(int argc, char** argv, int cmd_start, const probkit::cli::GlobalOptions& g)
    -> ExitCode {
  if (cmd_start >= argc) {
    print_root_help();
    return ExitCode::Success;
  }

  char* cmd_ptr = (cmd_start >= 0 && cmd_start < argc) ? *std::next(argv, cmd_start) : nullptr;
  if (cmd_ptr == nullptr) {
    print_root_help();
    return ExitCode::GeneralError;
  }
  const std::string_view cmd{cmd_ptr};
  const int cmd_argc = argc - cmd_start - 1;
  char** cmd_argv = (cmd_start + 1 >= 0 && cmd_start + 1 < argc) ? std::next(argv, cmd_start + 1) : nullptr;

  for (const auto& sc : kSubCmds) {
    if (cmd == sc.name) {
      const CommandResult r = sc.fn(cmd_argc, cmd_argv, g);
      return (r == CommandResult::Success) ? ExitCode::Success : ExitCode::GeneralError;
    }
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
  const probkit::cli::ParseResult pr = parse_global_options(argc, argv, g);

  if (pr.status == ExitCode::Success && pr.next_index < 0) {
    return to_int(ExitCode::Success);
  }

  if (pr.status != ExitCode::Success) {
    return to_int(ExitCode::ArgumentError);
  }

  const ExitCode result = dispatch_command(argc, argv, pr.next_index, g);
  return to_int(result);
  // NOLINTEND(cppcoreguidelines-pro-bounds-pointer-arithmetic)
}
