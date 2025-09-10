#include "probkit/bloom.hpp"
#include "probkit/hash.hpp"
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <string_view>
#include <vector>

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
};

// Option prefixes to avoid magic numbers for substr offsets
constexpr std::string_view kFP = "--fp=";
constexpr std::string_view kCAP = "--capacity-hint=";
constexpr std::string_view kMEM = "--mem-budget=";

// C++20 portability: prefer std::string_view::starts_with when available.
// Fallback to compare() to support older libstdc++/libc++.
constexpr auto sv_starts_with(std::string_view s, std::string_view prefix) noexcept -> bool {
#if defined(__cpp_lib_starts_ends_with) && (__cpp_lib_starts_ends_with >= 201711L)
  return s.starts_with(prefix);
#else
  return s.size() >= prefix.size() && s.compare(0, prefix.size(), prefix) == 0;
#endif
}

[[nodiscard]] inline auto parse_double(std::string_view s, double& out) -> bool {
  char* end = nullptr;
  std::string tmp{s};
  const double v = std::strtod(tmp.c_str(), &end);
  if (end == tmp.c_str() || *end != '\0') {
    return false;
  }
  out = v;
  return true;
}

[[nodiscard]] inline auto parse_u64(std::string_view s, std::uint64_t& out) -> bool {
  char* end = nullptr;
  std::string tmp{s};
  const unsigned long long v = std::strtoull(tmp.c_str(), &end, 10); // NOLINT
  if (end == tmp.c_str() || *end != '\0') {
    return false;
  }
  out = static_cast<std::uint64_t>(v);
  return true;
}

inline void print_usage() {
  std::fputs("usage: probkit bloom [--fp=<p> [--capacity-hint=<n>]] | [--mem-budget=<bytes>]\n", stdout);
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
  }
  return opts;
}

inline auto make_filter_from(const BloomOptions& opt, const hashing::HashConfig& h)
    -> probkit::result<probkit::bloom::filter> {
  if (opt.have_fp) {
    if (opt.have_cap) {
      return probkit::bloom::filter::make_by_fp(opt.fp, static_cast<std::size_t>(opt.cap), h);
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

auto cmd_bloom_sv(const std::vector<std::string_view>& args, const hashing::HashConfig& default_hash) -> int {
  const BloomOptions opt = parse_bloom_options(args);
  if (opt.show_help) {
    print_usage();
    return 0;
  }
  if (opt.have_fp && opt.have_mem) {
    std::fputs("error: specify either --fp or --mem-budget\n", stderr);
    return 2;
  }
  if (opt.have_fp) {
    if (opt.fp <= 0.0 || opt.fp >= 1.0) {
      std::fputs("error: --fp must be in (0,1)\n", stderr);
      return 2;
    }
    if (opt.have_cap && opt.cap == 0) {
      std::fputs("error: --capacity-hint must be > 0\n", stderr);
      return 2;
    }
  } else if (opt.have_mem && opt.mem == 0) {
    std::fputs("error: --mem-budget must be > 0 (>= 8 recommended)\n", stderr);
    return 2;
  }
  const auto hash = default_hash;
  auto r = make_filter_from(opt, hash);
  if (!r) {
    if (!opt.have_fp && !opt.have_mem) {
      std::fputs("error: missing args (specify --fp or --mem-budget)\n", stderr);
    } else {
      std::fputs("error: failed to build bloom filter\n", stderr);
    }
    return 2;
  }
  probkit::bloom::filter f = std::move(r.value());
  std::fprintf(stdout, "bloom: m_bits=%zu k=%u\n", f.bit_size(), static_cast<unsigned>(f.k()));
  return 0;
}

auto cmd_bloom(int argc, char** argv, const hashing::HashConfig& default_hash) -> int {
  const BloomOptions opt = parse_bloom_options(argc, argv);
  if (opt.show_help) {
    print_usage();
    return 0;
  }
  if (opt.have_fp && opt.have_mem) {
    std::fputs("error: specify either --fp or --mem-budget\n", stderr);
    return 2;
  }
  if (opt.have_fp) {
    if (opt.fp <= 0.0 || opt.fp >= 1.0) {
      std::fputs("error: --fp must be in (0,1)\n", stderr);
      return 2;
    }
    if (opt.have_cap && opt.cap == 0) {
      std::fputs("error: --capacity-hint must be > 0\n", stderr);
      return 2;
    }
  } else if (opt.have_mem && opt.mem == 0) {
    std::fputs("error: --mem-budget must be > 0 (>= 8 recommended)\n", stderr);
    return 2;
  }

  const auto hash = default_hash;
  auto r = make_filter_from(opt, hash);
  if (!r) {
    if (!opt.have_fp && !opt.have_mem) {
      std::fputs("error: missing args (specify --fp or --mem-budget)\n", stderr);
    } else {
      std::fputs("error: failed to build bloom filter\n", stderr);
    }
    return 2;
  }
  probkit::bloom::filter f = std::move(r.value());

  std::fprintf(stdout, "bloom: m_bits=%zu k=%u\n", f.bit_size(), static_cast<unsigned>(f.k()));
  return 0;
}

} // namespace probkit::cli
