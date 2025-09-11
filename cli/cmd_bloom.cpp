#include "options.hpp"
#include "probkit/bloom.hpp"
#include "probkit/hash.hpp"
#include "util/parse.hpp"
#include "util/string_utils.hpp"
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <string>
#include <string_view>
#include <vector>

using probkit::cli::CommandResult;
using probkit::cli::util::parse_double;
using probkit::cli::util::parse_u64;
using probkit::cli::util::sv_starts_with;

namespace probkit::cli {

namespace {
struct BloomOptions {
  bool show_help{false};
  bool have_fp{false};
  double fp{0.0};
  bool have_mem{false};
  std::uint64_t mem{0};
  bool have_cap{false};
  std::uint64_t cap{0};
  enum class Action : std::uint8_t { none, dedup };
  Action action{Action::none};
};

constexpr std::string_view kFP = "--fp=";
constexpr std::string_view kCAP = "--capacity-hint=";
constexpr std::string_view kMEM = "--mem-budget=";
constexpr std::string_view kACT = "--action=";

inline void print_usage() {
  std::fputs("usage: probkit bloom [--fp=<p> [--capacity-hint=<n>]] | [--mem-budget=<bytes>] [--action=dedup]\n",
             stdout);
}

auto parse_bloom_options(const std::vector<std::string_view>& args) -> BloomOptions;

auto parse_bloom_options(int argc, char** argv) -> BloomOptions {
  std::vector<std::string_view> args;
  args.reserve(static_cast<std::size_t>(argc));
  // NOLINTBEGIN(cppcoreguidelines-pro-bounds-pointer-arithmetic)
  for (int i = 0; i < argc; ++i) {
    args.emplace_back(argv[i]);
  }
  // NOLINTEND(cppcoreguidelines-pro-bounds-pointer-arithmetic)
  return parse_bloom_options(args);
}

auto parse_bloom_options(const std::vector<std::string_view>& args) -> BloomOptions {
  BloomOptions opts{};
  for (auto arg : args) {
    if (arg == std::string_view{"--help"}) {
      opts.show_help = true;
      break;
    }
    if (sv_starts_with(arg, kFP)) {
      double value = 0.0;
      if (!parse_double(arg.substr(kFP.size()), value)) {
        std::fputs("error: invalid --fp\n", stderr);
        opts.show_help = true;
        break;
      }
      opts.fp = value;
      opts.have_fp = true;
      continue;
    }
    if (sv_starts_with(arg, kCAP)) {
      std::uint64_t value = 0;
      if (!parse_u64(arg.substr(kCAP.size()), value)) {
        std::fputs("error: invalid --capacity-hint\n", stderr);
        opts.show_help = true;
        break;
      }
      opts.cap = value;
      opts.have_cap = true;
      continue;
    }
    if (sv_starts_with(arg, kMEM)) {
      std::uint64_t value = 0;
      if (!parse_u64(arg.substr(kMEM.size()), value)) {
        std::fputs("error: invalid --mem-budget\n", stderr);
        opts.show_help = true;
        break;
      }
      opts.mem = value;
      opts.have_mem = true;
      continue;
    }
    if (sv_starts_with(arg, kACT)) {
      auto v = arg.substr(kACT.size());
      if (v == std::string_view{"dedup"}) {
        opts.action = BloomOptions::Action::dedup;
      } else {
        std::fputs("error: invalid --action\n", stderr);
        opts.show_help = true;
        break;
      }
      continue;
    }
  }
  return opts;
}

inline auto make_filter_from(const BloomOptions& opt, const hashing::HashConfig& h)
    -> probkit::result<probkit::bloom::filter> {
  if (opt.have_fp) {
    if (opt.have_cap) {
      return probkit::bloom::filter::make_by_fp(opt.fp, h, static_cast<std::size_t>(opt.cap));
    }
    return probkit::bloom::filter::make_by_fp(opt.fp, h);
  }
  if (opt.have_mem) {
    return probkit::bloom::filter::make_by_mem(static_cast<std::size_t>(opt.mem), h);
  }
  return probkit::result<probkit::bloom::filter>::from_error(
      probkit::make_error(probkit::errc::invalid_argument, "missing args"));
}
} // end anonymous namespace

auto cmd_bloom_sv(const std::vector<std::string_view>& args, const hashing::HashConfig& default_hash) -> CommandResult {
  const BloomOptions opt = parse_bloom_options(args);
  if (opt.show_help) {
    print_usage();
    return CommandResult::Success;
  }
  if (opt.have_fp && opt.have_mem) {
    std::fputs("error: specify either --fp or --mem-budget\n", stderr);
    return CommandResult::GeneralError;
  }
  if (opt.have_fp) {
    if (opt.fp <= 0.0 || opt.fp >= 1.0) {
      std::fputs("error: --fp must be in (0,1)\n", stderr);
      return CommandResult::GeneralError;
    }
    if (opt.have_cap && opt.cap == 0) {
      std::fputs("error: --capacity-hint must be > 0\n", stderr);
      return CommandResult::GeneralError;
    }
  } else if (opt.have_mem && opt.mem == 0) {
    std::fputs("error: --mem-budget must be > 0 (>= 8 recommended)\n", stderr);
    return CommandResult::GeneralError;
  }
  const auto hash = default_hash;
  auto r = make_filter_from(opt, hash);
  if (!r) {
    if (!opt.have_fp && !opt.have_mem) {
      std::fputs("error: missing args (specify --fp or --mem-budget)\n", stderr);
    } else {
      std::fputs("error: failed to build bloom filter\n", stderr);
    }
    return CommandResult::GeneralError;
  }
  probkit::bloom::filter f = std::move(r.value());
  std::fprintf(stdout, "bloom: m_bits=%zu k=%u\n", f.bit_size(), static_cast<unsigned>(f.k()));
  return CommandResult::Success;
}

auto cmd_bloom(int argc, char** argv, const GlobalOptions& g) -> CommandResult {
  const BloomOptions opt = parse_bloom_options(argc, argv);
  if (opt.show_help) {
    print_usage();
    return CommandResult::Success;
  }
  // Validate file if provided (non-gz only; gz via zcat is recommended)
  std::ifstream file_in;
  if (!g.file_path.empty()) {
    file_in.open(g.file_path, std::ios::in);
    if (!file_in.is_open()) {
      std::fputs("error: failed to open --file\n", stderr);
      return CommandResult::IOError;
    }
  }
  if (opt.have_fp && opt.have_mem) {
    std::fputs("error: specify either --fp or --mem-budget\n", stderr);
    return CommandResult::GeneralError;
  }
  if (opt.have_fp) {
    if (opt.fp <= 0.0 || opt.fp >= 1.0) {
      std::fputs("error: --fp must be in (0,1)\n", stderr);
      return CommandResult::GeneralError;
    }
    if (opt.have_cap && opt.cap == 0) {
      std::fputs("error: --capacity-hint must be > 0\n", stderr);
      return CommandResult::GeneralError;
    }
  } else if (opt.have_mem && opt.mem == 0) {
    std::fputs("error: --mem-budget must be > 0 (>= 8 recommended)\n", stderr);
    return CommandResult::GeneralError;
  }

  const auto hash = g.hash;
  auto r = make_filter_from(opt, hash);
  if (!r) {
    if (!opt.have_fp && !opt.have_mem) {
      std::fputs("error: missing args (specify --fp or --mem-budget)\n", stderr);
    } else {
      std::fputs("error: failed to build bloom filter\n", stderr);
    }
    return CommandResult::GeneralError;
  }
  probkit::bloom::filter f = std::move(r.value());
  if (opt.action != BloomOptions::Action::dedup) {
    if (g.json) {
      std::fprintf(stdout, "{\"m_bits\":%zu,\"k\":%u}\n", f.bit_size(), static_cast<unsigned>(f.k()));
    } else {
      std::fprintf(stdout, "bloom: m_bits=%zu k=%u\n", f.bit_size(), static_cast<unsigned>(f.k()));
    }
    return CommandResult::Success;
  }

  // dedup streaming (single-threaded baseline)
  std::istream* in = nullptr;
  std::ifstream file_in_local;
  if (!g.file_path.empty() && g.file_path != "-") {
    file_in_local.open(g.file_path, std::ios::in);
    if (!file_in_local.is_open()) {
      std::fputs("error: failed to open --file\n", stderr);
      return CommandResult::IOError;
    }
    in = &file_in_local;
  } else {
#ifdef _WIN32
    in = &std::cin;
#else
    // Avoid direct use of std::cin to placate certain toolchains; use /dev/stdin on POSIX
    file_in_local.open("/dev/stdin", std::ios::in);
    if (!file_in_local.is_open()) {
      std::fputs("error: failed to open stdin\n", stderr);
      return CommandResult::IOError;
    }
    in = &file_in_local;
#endif
  }

  std::uint64_t seen = 0;
  std::uint64_t passed = 0;
  std::string line;
  line.reserve(256);
  while (true) {
    if (!std::getline(*in, line)) {
      break;
    }
    ++seen;
    auto maybe_cont = f.might_contain(line);
    if (!maybe_cont) {
      std::fputs("error: bloom query failed\n", stderr);
      return CommandResult::GeneralError;
    }
    if (!maybe_cont.value()) {
      (void)f.add(line);
      std::fputs(line.c_str(), stdout);
      std::fputc('\n', stdout);
      ++passed;
    }
    if ((g.stop_after != 0U) && seen >= g.stop_after) {
      break;
    }
  }
  if (g.json) {
    if (opt.have_fp) {
      std::fprintf(stderr, "{\"seen\":%llu,\"passed\":%llu,\"fp_target\":%.6f}\n",
                   static_cast<unsigned long long>(seen), static_cast<unsigned long long>(passed), opt.fp);
    } else {
      std::fprintf(stderr, "{\"seen\":%llu,\"passed\":%llu}\n", static_cast<unsigned long long>(seen),
                   static_cast<unsigned long long>(passed));
    }
  }
  return CommandResult::Success;
}

} // namespace probkit::cli
