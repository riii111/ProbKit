#include "options.hpp"
#include "probkit/bloom.hpp"
#include "probkit/hash.hpp"
#include "util/parse.hpp"
#include "util/spsc_ring.hpp"
#include "util/string_utils.hpp"
#include "util/threads.hpp"
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

using probkit::cli::CommandResult;
using probkit::cli::util::decide_num_workers;
using probkit::cli::util::parse_double;
using probkit::cli::util::parse_u64;
using probkit::cli::util::spsc_ring;
using probkit::cli::util::sv_starts_with;
using probkit::hashing::hash64;

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

struct LineItem {
  std::string data;
};

inline auto open_input(const GlobalOptions& g, std::ifstream& file_in, std::istream*& in) -> bool {
  if (g.file_path.empty() || g.file_path == "-") {
    in = &std::cin;
    return true;
  }
  file_in.open(g.file_path, std::ios::in);
  if (!file_in.is_open()) {
    std::fputs("error: failed to open --file\n", stderr);
    return false;
  }
  in = &file_in;
  return true;
}

inline void dispatch_line(spsc_ring<LineItem>& ring, const std::string& line) {
  using namespace std::chrono_literals;
  while (!ring.try_emplace(line)) {
    std::this_thread::sleep_for(50us);
  }
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

  // dedup streaming: single-thread path, then multi-thread sharded path
  {
    const int num_workers = decide_num_workers(g.threads);
    if (num_workers <= 1) {
      std::ifstream file_in_local;
      std::istream* in = nullptr;
      if (!open_input(g, file_in_local, in)) {
        return CommandResult::IOError;
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

    // Multi-thread sharded dedup
    const std::size_t ring_capacity = 1U << 14;
    std::vector<std::unique_ptr<spsc_ring<LineItem>>> ring_storage;
    std::vector<spsc_ring<LineItem>*> rings;
    ring_storage.reserve(static_cast<std::size_t>(num_workers));
    rings.reserve(static_cast<std::size_t>(num_workers));
    for (int i = 0; i < num_workers; ++i) {
      ring_storage.emplace_back(new spsc_ring<LineItem>(ring_capacity));
      rings.push_back(ring_storage.back().get());
    }

    std::vector<probkit::bloom::filter> locals;
    locals.reserve(static_cast<std::size_t>(num_workers));
    for (int i = 0; i < num_workers; ++i) {
      hashing::HashConfig hc = g.hash;
      const std::uint64_t thread_index = static_cast<std::uint64_t>(i) + 1ULL;
      hc.thread_salt = probkit::hashing::derive_thread_salt(hc.seed, thread_index);
      auto rlocal = make_filter_from(opt, hc);
      if (!rlocal) {
        std::fputs("error: failed to init bloom shard\n", stderr);
        return CommandResult::ConfigError;
      }
      locals.emplace_back(std::move(rlocal.value()));
    }

    std::mutex out_mtx;
    std::atomic<bool> done{false};
    std::atomic<std::uint64_t> seen{0};
    std::atomic<std::uint64_t> passed{0};

    auto worker_fn = [&](int wi) -> void {
      auto& ring = *rings[static_cast<std::size_t>(wi)];
      auto& flt = locals[static_cast<std::size_t>(wi)];
      LineItem item;
      while (true) {
        if (ring.pop(item)) {
          seen.fetch_add(1, std::memory_order_relaxed);
          auto mc = flt.might_contain(item.data);
          if (!mc) {
            continue; // skip on error
          }
          if (!mc.value()) {
            (void)flt.add(item.data);
            std::scoped_lock lk(out_mtx);
            std::fputs(item.data.c_str(), stdout);
            std::fputc('\n', stdout);
            passed.fetch_add(1, std::memory_order_relaxed);
          }
        } else if (done.load(std::memory_order_acquire)) {
          break;
        } else {
          std::this_thread::sleep_for(std::chrono::microseconds(50));
        }
      }
    };

    std::vector<std::thread> workers;
    workers.reserve(static_cast<std::size_t>(num_workers));
    for (int wi = 0; wi < num_workers; ++wi) {
      workers.emplace_back(worker_fn, wi);
    }

    std::ifstream file_in_local;
    std::istream* in = nullptr;
    if (!open_input(g, file_in_local, in)) {
      done.store(true, std::memory_order_release);
      for (auto& w : workers) {
        w.join();
      }
      return CommandResult::IOError;
    }
    std::string line;
    line.reserve(256);
    std::uint64_t limit_counter = 0;
    while (std::getline(*in, line)) {
      const std::uint64_t hv = hash64(line, g.hash);
      const int shard = static_cast<int>(hv % static_cast<std::uint64_t>(num_workers));
      dispatch_line(*rings[static_cast<std::size_t>(shard)], line);
      if ((g.stop_after != 0U) && ++limit_counter >= g.stop_after) {
        break;
      }
    }
    done.store(true, std::memory_order_release);
    for (auto& w : workers) {
      w.join();
    }

    if (g.json) {
      if (opt.have_fp) {
        std::fprintf(stderr, "{\"seen\":%llu,\"passed\":%llu,\"fp_target\":%.6f}\n",
                     static_cast<unsigned long long>(seen.load()), static_cast<unsigned long long>(passed.load()),
                     opt.fp);
      } else {
        std::fprintf(stderr, "{\"seen\":%llu,\"passed\":%llu}\n", static_cast<unsigned long long>(seen.load()),
                     static_cast<unsigned long long>(passed.load()));
      }
    }
    return CommandResult::Success;
  }
}

} // namespace probkit::cli
