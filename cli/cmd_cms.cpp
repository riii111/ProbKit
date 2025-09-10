#include "options.hpp"
#include "probkit/cms.hpp"
#include "probkit/hash.hpp"
#include "util/parse.hpp"
#include "util/spsc_ring.hpp"
#include "util/string_utils.hpp"
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

using probkit::cli::CommandResult;
using probkit::cli::util::sv_starts_with;

// Feature detection & aliases
#ifdef __has_include
#if __has_include(<stop_token>)
#define PROBKIT_HAS_STOP_TOKEN 1
#else
#define PROBKIT_HAS_STOP_TOKEN 0
#endif
#else
#define PROBKIT_HAS_STOP_TOKEN 0
#endif
#if defined(__cpp_lib_jthread) && (__cpp_lib_jthread >= 201911L)
#define PROBKIT_HAS_JTHREAD 1
#else
#define PROBKIT_HAS_JTHREAD 0
#endif

namespace probkit::cli {

namespace {
using probkit::cli::util::parse_double;
using probkit::cli::util::parse_u64;
using probkit::cli::util::spsc_ring;

struct CmsOptions {
  bool show_help{false};
  bool have_eps{false};
  bool have_delta{false};
  double eps{1e-3};
  double delta{1e-4};
  std::size_t topk{0};
};

struct LineItem {
  std::string data;
};

struct Rings {
  std::vector<std::unique_ptr<spsc_ring<LineItem>>> store;
  std::vector<spsc_ring<LineItem>*> views;
};

inline auto decide_num_workers(int requested) -> int {
  if (requested > 0) {
    return requested;
  }
  const int hw = static_cast<int>(std::thread::hardware_concurrency());
  return (hw > 0) ? hw : 1;
}

struct RingConfig {
  std::size_t capacity;
  unsigned int worker_count;
};

inline auto make_rings(const RingConfig& config) -> Rings {
  Rings r{};
  r.store.reserve(static_cast<std::size_t>(config.worker_count));
  r.views.reserve(static_cast<std::size_t>(config.worker_count));
  for (unsigned int i = 0; i < config.worker_count; ++i) {
    r.store.emplace_back(new spsc_ring<LineItem>(config.capacity));
    r.views.push_back(r.store.back().get());
  }
  return r;
}

inline void print_dims(FILE* out, const probkit::cms::sketch& sk) {
  auto [d, w] = sk.dims();
  std::fprintf(out, "{\"depth\":%zu,\"width\":%zu}\n", d, w);
}

inline void print_help() {
  std::fputs("usage: probkit cms [--eps=<e>] [--delta=<d>] [--topk=<k>]\n", stdout);
}

// Minimal JSON string escaper for keys in --topk output
inline void json_escape_and_print(FILE* out, std::string_view s) {
  std::fputc('"', out);
  for (const char c : s) {
    const auto ch = static_cast<unsigned char>(c);
    switch (ch) {
    case '\\':
      std::fputs("\\\\", out);
      break;
    case '"':
      std::fputs("\\\"", out);
      break;
    case '\b':
      std::fputs("\\b", out);
      break;
    case '\f':
      std::fputs("\\f", out);
      break;
    case '\n':
      std::fputs("\\n", out);
      break;
    case '\r':
      std::fputs("\\r", out);
      break;
    case '\t':
      std::fputs("\\t", out);
      break;
    default:
      if (ch < 0x20) {
        std::fprintf(out, "\\u%04x", ch);
      } else {
        std::fputc(ch, out);
      }
    }
  }
  std::fputc('"', out);
}

template <class Items> inline void print_topk_json(FILE* out, const Items& items) {
  std::fputs("{\"topk\":[", out);
  for (std::size_t i = 0; i < items.size(); ++i) {
    const auto& it = items[i];
    if (i != 0U) {
      std::fputc(',', out);
    }
    std::fputs(R"({"key":)", out);
    json_escape_and_print(out, it.key);
    std::fprintf(out, R"(,"est":%llu})", static_cast<unsigned long long>(it.est));
  }
  std::fputs("]}\n", out);
}

inline void dispatch_line(spsc_ring<LineItem>& ring, std::string& line) {
  using namespace std::chrono_literals;
  while (!ring.try_emplace(std::move(line))) {
    std::this_thread::sleep_for(50us);
  }
}

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

inline auto build_locals(int num_workers, const CmsOptions& co, const GlobalOptions& g,
                         std::vector<probkit::cms::sketch>& out) -> bool {
  out.reserve(static_cast<std::size_t>(num_workers));
  for (int i = 0; i < num_workers; ++i) {
    probkit::hashing::HashConfig hc = g.hash;
    const std::uint64_t thread_index = static_cast<std::uint64_t>(i) + 1ULL;
    hc.thread_salt = probkit::hashing::derive_thread_salt(hc.seed, thread_index);
    auto s = probkit::cms::sketch::make_by_eps_delta(co.have_eps ? co.eps : 1e-3, co.have_delta ? co.delta : 1e-4, hc);
    if (!s) {
      return false;
    }
    out.emplace_back(std::move(s.value()));
  }
  return true;
}

#if PROBKIT_HAS_JTHREAD && PROBKIT_HAS_STOP_TOKEN
using StatsThread = std::jthread;
#else
using StatsThread = std::thread;
#endif

inline auto start_stats_if_enabled(const GlobalOptions& g, std::atomic<bool>& done,
                                   std::atomic<std::uint64_t>& processed_total, StatsThread& thr_out) -> bool {
  if (!g.stats) {
    return false;
  }
#if PROBKIT_HAS_JTHREAD && PROBKIT_HAS_STOP_TOKEN
  thr_out = std::jthread([&](std::stop_token st) {
#else
  thr_out = std::thread([&]() -> void {
#endif
    const auto interval = std::chrono::seconds(g.stats ? g.stats_interval_seconds : 1U);
    while (
#if PROBKIT_HAS_JTHREAD && PROBKIT_HAS_STOP_TOKEN
        !st.stop_requested()
#else
        true
#endif
    ) {
      std::this_thread::sleep_for(interval);
      const auto proc = processed_total.load(std::memory_order_relaxed);
      std::fprintf(stderr, "processed=%llu\n", static_cast<unsigned long long>(proc));
      if (done.load(std::memory_order_acquire)) {
        break;
      }
    }
  });
  return true;
}

auto parse_cms_opts(int argc, char** argv) -> CmsOptions {
  CmsOptions o{};
  // NOLINTBEGIN(cppcoreguidelines-pro-bounds-pointer-arithmetic)
  for (int i = 0; i < argc; ++i) {
    std::string_view a{argv[i]};
    if (a == std::string_view{"--help"}) {
      o.show_help = true;
      break;
    }
    if (sv_starts_with(a, std::string_view{"--eps="})) {
      double v = 0.0;
      if (!parse_double(a.substr(std::string_view{"--eps="}.size()), v) || v <= 0.0 || v >= 1.0) {
        std::fputs("error: invalid --eps\n", stderr);
        o.show_help = true;
        break;
      }
      o.have_eps = true;
      o.eps = v;
    } else if (sv_starts_with(a, std::string_view{"--delta="})) {
      double v = 0.0;
      if (!parse_double(a.substr(std::string_view{"--delta="}.size()), v) || v <= 0.0 || v >= 1.0) {
        std::fputs("error: invalid --delta\n", stderr);
        o.show_help = true;
        break;
      }
      o.have_delta = true;
      o.delta = v;
    } else if (sv_starts_with(a, std::string_view{"--topk="})) {
      std::uint64_t v = 0;
      if (!parse_u64(a.substr(std::string_view{"--topk="}.size()), v)) {
        std::fputs("error: invalid --topk\n", stderr);
        o.show_help = true;
        break;
      }
      o.topk = static_cast<std::size_t>(v);
    }
  }
  // NOLINTEND(cppcoreguidelines-pro-bounds-pointer-arithmetic)
  return o;
}

template <class StopQ> inline void worker_loop(spsc_ring<LineItem>& ring, probkit::cms::sketch& sk, StopQ stopq) {
  LineItem item;
  while (true) {
    if (ring.pop(item)) {
      (void)sk.inc(item.data);
    } else if (stopq()) {
      break;
    } else {
      std::this_thread::sleep_for(std::chrono::microseconds(50));
    }
  }
}

#if PROBKIT_HAS_JTHREAD && PROBKIT_HAS_STOP_TOKEN
using WorkerThread = std::jthread;
using ReaderThread = std::jthread;
#else
using WorkerThread = std::thread;
using ReaderThread = std::thread;
#endif

inline void spawn_worker(std::vector<WorkerThread>& ws, spsc_ring<LineItem>& ring, probkit::cms::sketch& sk,
                         std::atomic<bool>& done) {
#if PROBKIT_HAS_JTHREAD && PROBKIT_HAS_STOP_TOKEN
  ws.emplace_back([&](std::stop_token st) {
    auto stopq = [&]() -> bool { return done.load(std::memory_order_acquire) || st.stop_requested(); };
    worker_loop(ring, sk, stopq);
  });
#else
  ws.emplace_back([&]() -> void {
    auto stopq = [&]() -> bool { return done.load(std::memory_order_acquire); };
    worker_loop(ring, sk, stopq);
  });
#endif
}

inline auto start_reader(const GlobalOptions& g, const std::vector<spsc_ring<LineItem>*>& rings, int num_workers,
                         std::atomic<bool>& done, std::atomic<std::uint64_t>& processed_total) -> ReaderThread {
#if PROBKIT_HAS_JTHREAD && PROBKIT_HAS_STOP_TOKEN
  return ReaderThread([&](std::stop_token rst) {
#else
  return ReaderThread([&]() -> void {
#endif
    std::ifstream file_in;
    std::istream* in = nullptr;
    if (!open_input(g, file_in, in)) {
      done.store(true, std::memory_order_release);
      return;
    }
    std::string line;
    line.reserve(256);
    std::uint64_t processed = 0;
    int shard = 0;
    while (
#if PROBKIT_HAS_JTHREAD && PROBKIT_HAS_STOP_TOKEN
        !rst.stop_requested()
#else
        true
#endif
    ) {
      if (!std::getline(*in, line)) {
        break;
      }
      dispatch_line(*rings[static_cast<std::size_t>(shard)], line);
      shard = (shard + 1) % num_workers;
      processed_total.fetch_add(1, std::memory_order_relaxed);
      if (g.stop_after && ++processed >= g.stop_after) {
        break;
      }
    }
    done.store(true, std::memory_order_release);
  });
}

} // namespace

auto cmd_cms(int argc, char** argv, const GlobalOptions& g) -> CommandResult {
  const CmsOptions co = parse_cms_opts(argc, argv);
  if (co.show_help) {
    print_help();
    return CommandResult::Success;
  }

  auto global_r =
      probkit::cms::sketch::make_by_eps_delta(co.have_eps ? co.eps : 1e-3, co.have_delta ? co.delta : 1e-4, g.hash);
  if (!global_r) {
    std::fputs("error: failed to init cms\n", stderr);
    return CommandResult::ConfigError;
  }

  const int num_workers = decide_num_workers(g.threads);
  const std::size_t ring_capacity = 1U << 14;

  auto rings =
      make_rings(RingConfig{.capacity = ring_capacity, .worker_count = static_cast<unsigned int>(num_workers)});

  // Thread-local sketches
  std::vector<probkit::cms::sketch> locals;
  if (!build_locals(num_workers, co, g, locals)) {
    std::fputs("error: failed to init worker cms\n", stderr);
    return CommandResult::ConfigError;
  }

  std::atomic<bool> done{false};
  std::atomic<std::uint64_t> processed_total{0};

  // Workers
  std::vector<WorkerThread> workers;
  workers.reserve(static_cast<std::size_t>(num_workers));
  for (int wi = 0; wi < num_workers; ++wi) {
    spawn_worker(workers, *rings.views[static_cast<std::size_t>(wi)], locals[static_cast<std::size_t>(wi)], done);
  }

  // Reader
  ReaderThread reader = start_reader(g, rings.views, num_workers, done, processed_total);

  // Optional periodic stats
  StatsThread stats_thr;
  const bool stats_enabled = start_stats_if_enabled(g, done, processed_total, stats_thr);

  // Wait and finalize
  reader.join();
#if PROBKIT_HAS_JTHREAD && PROBKIT_HAS_STOP_TOKEN
  for (auto& w : workers) {
    w.request_stop();
  }
  if (stats_enabled) {
    stats_thr.request_stop();
  }
#else
  for (auto& w : workers) {
    w.join();
  }
#endif
  if (stats_enabled) {
#if !(PROBKIT_HAS_JTHREAD && PROBKIT_HAS_STOP_TOKEN)
    if (stats_thr.joinable()) {
      stats_thr.join();
    }
#endif
  }

  // Reducer: merge locals
  auto global = std::move(global_r.value());
  for (auto& tl : locals) {
    (void)global.merge(tl);
  }

  // Output
  if (co.topk > 0) {
    auto r = global.topk(co.topk);
    if (!r) {
      std::fputs("error: cms topk failed\n", stderr);
      return CommandResult::ConfigError;
    }
    const auto& items = r.value();
    if (g.json) {
      print_topk_json(stdout, items);
    } else {
      for (const auto& it : items) {
        std::fprintf(stdout, "%s\t%llu\n", it.key.c_str(), static_cast<unsigned long long>(it.est));
      }
    }
  } else {
    if (g.json) {
      print_dims(stdout, global);
    } else {
      std::fputs("cms: processed\n", stdout);
    }
  }
  return CommandResult::Success;
}

} // namespace probkit::cli
