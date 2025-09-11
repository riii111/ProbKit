#include "options.hpp"
#include "probkit/hash.hpp"
#include "probkit/hll.hpp"
#include "util/duration.hpp"
#include "util/parse.hpp"
#include "util/spsc_ring.hpp"
#include "util/string_utils.hpp"
#include "util/threads.hpp"
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

// Feature-detect stop_token/jthread
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

using probkit::cli::CommandResult;
using probkit::cli::util::parse_u64;
using probkit::cli::util::sv_starts_with;
using probkit::cli::util::timeutil::format_utc_iso8601;
using probkit::cli::util::timeutil::parse_duration;
using probkit::cli::util::timeutil::Timebase;
using probkit::hashing::hash64;

namespace probkit::cli {

namespace {
using probkit::cli::util::spsc_ring;

struct HllOptions {
  bool show_help{false};
  bool have_precision{false};
  std::uint8_t precision{14};
};

inline void print_help() {
  std::fputs("usage: probkit hll [--precision=<p>]\n", stdout);
}

auto parse_hll_opts(int argc, char** argv) -> HllOptions {
  HllOptions o{};
  // NOLINTBEGIN(cppcoreguidelines-pro-bounds-pointer-arithmetic)
  for (int i = 0; i < argc; ++i) {
    std::string_view a{argv[i]};
    if (a == std::string_view{"--help"}) {
      o.show_help = true;
      break;
    }
    if (sv_starts_with(a, std::string_view{"--precision="})) {
      std::uint64_t v = 0;
      if (!parse_u64(a.substr(std::string_view{"--precision="}.size()), v) || v > 24) {
        std::fputs("error: invalid --precision\n", stderr);
        o.show_help = true;
        break;
      }
      o.have_precision = true;
      o.precision = static_cast<std::uint8_t>(v);
    }
  }
  // NOLINTEND(cppcoreguidelines-pro-bounds-pointer-arithmetic)
  return o;
}
struct LineItem {
  std::string data;
};

struct InputPair;
static auto open_input(const GlobalOptions& g, std::ifstream& file_in, std::istream*& in) -> bool;
static void dispatch_line(spsc_ring<LineItem>& ring, std::string& line);
static auto run_hll_single_non_bucket(std::istream& in, probkit::hll::sketch global, const GlobalOptions& g)
    -> CommandResult;
static auto run_hll_single_bucketed(std::istream& in, std::uint8_t p, const GlobalOptions& g) -> CommandResult;
template <class StopQ>
static void worker_loop(spsc_ring<LineItem>& ring, probkit::hll::sketch& sk, StopQ stopq, std::atomic<bool>* merging,
                        std::atomic<int>* paused);
#if PROBKIT_HAS_JTHREAD
using WorkerThread = std::jthread;
using ReaderThread = std::jthread;
using ReducerThread = std::jthread;
#else
using WorkerThread = std::thread;
using ReaderThread = std::thread;
using ReducerThread = std::thread;
#endif
static void spawn_worker(std::vector<WorkerThread>& ws, spsc_ring<LineItem>& ring, probkit::hll::sketch& sk,
                         std::atomic<bool>& done, std::atomic<bool>* merging, std::atomic<int>* paused);
static auto start_reader(const GlobalOptions& g, const std::vector<spsc_ring<LineItem>*>& rings, int num_workers,
                         std::atomic<bool>& done) -> ReaderThread;
static auto start_reducer_hll(const GlobalOptions& g, std::vector<probkit::hll::sketch>& locals, std::uint8_t p,
                              std::atomic<bool>& done, std::atomic<int>& paused, std::atomic<bool>& merging,
                              int num_workers, std::atomic<bool>& workers_ended) -> ReducerThread;
} // namespace

// Reader → Workers → Reducer minimal pipeline for HLL
auto cmd_hll(int argc, char** argv, const GlobalOptions& g) -> CommandResult {
  const HllOptions ho = parse_hll_opts(argc, argv);
  if (ho.show_help) {
    print_help();
    return CommandResult::Success;
  }

  const std::uint8_t p = ho.have_precision ? ho.precision : 14;
  auto sketch_r = probkit::hll::sketch::make_by_precision(p, g.hash);
  if (!sketch_r) {
    std::fputs("error: failed to init hll\n", stderr);
    return CommandResult::ConfigError;
  }

  const int num_workers = probkit::cli::util::decide_num_workers(g.threads);
  const std::size_t ring_capacity = 1U << 14; // 16384 slots

  std::vector<spsc_ring<LineItem>*> rings;
  rings.reserve(static_cast<std::size_t>(num_workers));
  std::vector<std::unique_ptr<spsc_ring<LineItem>>> ring_storage;
  ring_storage.reserve(static_cast<std::size_t>(num_workers));
  for (int i = 0; i < num_workers; ++i) {
    ring_storage.emplace_back(new spsc_ring<LineItem>(ring_capacity));
    rings.push_back(ring_storage.back().get());
  }

  // thread-local sketches (use identical hash config across workers)
  std::vector<probkit::hll::sketch> locals;
  locals.reserve(static_cast<std::size_t>(num_workers));
  for (int i = 0; i < num_workers; ++i) {
    auto s = probkit::hll::sketch::make_by_precision(p, g.hash);
    if (!s) {
      std::fputs("error: failed to init worker sketch\n", stderr);
      return CommandResult::ConfigError;
    }
    locals.emplace_back(std::move(s.value()));
  }

  std::atomic<bool> done{false};
  std::atomic<bool> merging{false};
  std::atomic<int> paused_workers{0};
  std::atomic<bool> workers_ended{false};

  // Single-thread fallback (stability)
  if (num_workers <= 1) {
    std::ifstream file_in;
    std::istream* in = nullptr;
    if (!open_input(g, file_in, in)) {
      return CommandResult::IOError;
    }
    const bool bucket_mode = !g.bucket.empty();
    if (!bucket_mode) {
      return run_hll_single_non_bucket(*in, std::move(sketch_r.value()), g);
    }
    return run_hll_single_bucketed(*in, p, g);
  }

  // Workers
#if PROBKIT_HAS_JTHREAD
  std::vector<std::jthread> workers;
#else
  std::vector<std::thread> workers;
#endif
  workers.reserve(static_cast<std::size_t>(num_workers));
  for (int wi = 0; wi < num_workers; ++wi) {
    spawn_worker(workers, *rings[static_cast<std::size_t>(wi)], locals[static_cast<std::size_t>(wi)], done, &merging,
                 &paused_workers);
  }

  // Reader
  ReaderThread reader = start_reader(g, rings, num_workers, done);

  // Optional reducer for bucket mode
  const bool bucket_mode = !g.bucket.empty();
  ReducerThread reducer;
  bool reducer_started = false;
  if (bucket_mode) {
    reducer = start_reducer_hll(g, locals, p, done, paused_workers, merging, num_workers, workers_ended);
    reducer_started = true;
  }

  // Wait: reader then workers
  reader.join();
#if PROBKIT_HAS_JTHREAD && PROBKIT_HAS_STOP_TOKEN
  for (auto& w : workers) {
    w.request_stop();
    // jthread joins on destruction; explicit stop to hasten exit if sleeping
  }
  for (auto& w : workers) {
    if (w.joinable()) {
      w.join();
    }
  }
#else
  for (auto& w : workers) {
    w.join();
  }
#endif

  workers_ended.store(true, std::memory_order_release);

  if (reducer_started) {
    if (reducer.joinable()) {
      reducer.join();
    }
    return CommandResult::Success;
  }

  // Reducer: merge locals
  auto global = std::move(sketch_r.value());
  for (auto& tl : locals) {
    (void)global.merge(tl);
  }

  // Final output (non-bucket)
  auto est = global.estimate();
  if (!est) {
    std::fputs("error: hll estimate failed\n", stderr);
    return CommandResult::ConfigError;
  }
  if (g.json) {
    std::printf("{\"uu\":%.0f,\"m\":%zu}\n", est.value(), global.m());
  } else {
    std::printf("uu=%.0f m=%zu\n", est.value(), global.m());
  }
  return CommandResult::Success;
}

} // namespace probkit::cli

// ==================== Details (helper implementations) ====================
namespace probkit::cli {
namespace {
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

inline void dispatch_line(spsc_ring<LineItem>& ring, std::string& line) {
  using namespace std::chrono_literals;
  // two-phase backoff: initial yields to reduce CPU, then sleep
  int spins = 0;
  while (!ring.try_emplace(std::move(line))) {
    if (spins < 16) {
      std::this_thread::yield();
      ++spins;
    } else {
      std::this_thread::sleep_for(50us);
    }
  }
}

template <class StopQ>
inline void worker_loop(spsc_ring<LineItem>& ring, probkit::hll::sketch& sk, StopQ stopq, std::atomic<bool>* merging,
                        std::atomic<int>* paused) {
  LineItem item;
  bool counted_pause = false;
  while (true) {
    if (merging != nullptr && merging->load(std::memory_order_acquire)) {
      if (!counted_pause) {
        if (paused != nullptr) {
          paused->fetch_add(1, std::memory_order_acq_rel);
        }
        counted_pause = true;
      }
      std::this_thread::sleep_for(std::chrono::microseconds(50));
      continue;
    }
    if (counted_pause) {
      counted_pause = false;
    }
    if (ring.pop(item)) {
      (void)sk.add(item.data);
    } else if (stopq()) {
      break;
    } else {
      // lighter backoff under normal idle
      std::this_thread::yield();
    }
  }
}

inline auto run_hll_single_non_bucket(std::istream& in, probkit::hll::sketch global, const GlobalOptions& g)
    -> CommandResult {
  std::string line;
  line.reserve(256);
  std::uint64_t processed = 0;
  while (std::getline(in, line)) {
    (void)global.add(line);
    if ((g.stop_after != 0U) && ++processed >= g.stop_after) {
      break;
    }
  }
  auto est = global.estimate();
  if (!est) {
    std::fputs("error: hll estimate failed\n", stderr);
    return CommandResult::ConfigError;
  }
  if (g.json) {
    std::printf("{\"uu\":%.0f,\"m\":%zu}\n", est.value(), global.m());
  } else {
    std::printf("uu=%.0f m=%zu\n", est.value(), global.m());
  }
  return CommandResult::Success;
}

inline auto run_hll_single_bucketed(std::istream& in, std::uint8_t p, const GlobalOptions& g) -> CommandResult {
  std::chrono::nanoseconds bucket_ns{};
  if (!parse_duration(g.bucket, bucket_ns)) {
    std::fputs("error: invalid --bucket value\n", stderr);
    return CommandResult::ConfigError;
  }
  if (bucket_ns < std::chrono::seconds(1)) {
    bucket_ns = std::chrono::seconds(1);
  }
  Timebase tb{};
  auto bucket_start = std::chrono::steady_clock::now();
  auto bucket_end = bucket_start + bucket_ns;
  auto bucket_sk_r = probkit::hll::sketch::make_by_precision(p, g.hash);
  if (!bucket_sk_r) {
    std::fputs("error: failed to init hll bucket\n", stderr);
    return CommandResult::ConfigError;
  }
  auto bucket_sk = std::move(bucket_sk_r.value());
  std::string line;
  line.reserve(256);
  std::uint64_t processed = 0;
  auto flush_bucket = [&](std::chrono::steady_clock::time_point ts_steady) -> void {
    auto est = bucket_sk.estimate();
    if (est) {
      const auto ts = format_utc_iso8601(tb.to_system(ts_steady));
      if (g.json) {
        std::printf("{\"ts\":\"%s\",\"uu\":%.0f,\"m\":%zu}\n", ts.c_str(), est.value(), bucket_sk.m());
      } else {
        std::printf("%s\tuu=%.0f m=%zu\n", ts.c_str(), est.value(), bucket_sk.m());
      }
    }
    auto r = probkit::hll::sketch::make_by_precision(p, g.hash);
    if (r) {
      bucket_sk = std::move(r.value());
    }
  };
  while (std::getline(in, line)) {
    const auto now = std::chrono::steady_clock::now();
    if (now >= bucket_end) {
      flush_bucket(bucket_start);
      bucket_start = bucket_end;
      bucket_end = bucket_start + bucket_ns;
    }
    (void)bucket_sk.add(line);
    if ((g.stop_after != 0U) && ++processed >= g.stop_after) {
      break;
    }
  }
  flush_bucket(bucket_start);
  return CommandResult::Success;
}
#if PROBKIT_HAS_JTHREAD && PROBKIT_HAS_STOP_TOKEN
static void spawn_worker(std::vector<WorkerThread>& ws, spsc_ring<LineItem>& ring, probkit::hll::sketch& sk,
                         std::atomic<bool>& done, std::atomic<bool>* merging, std::atomic<int>* paused) {
  auto* ring_p = &ring;
  auto* sk_p = &sk;
  auto* done_p = &done;
  auto* merging_p = merging;
  auto* paused_p = paused;
  ws.emplace_back([ring_p, sk_p, done_p, merging_p, paused_p](std::stop_token st) {
    auto stopq = [done_p, &st]() -> bool { return done_p->load(std::memory_order_acquire) || st.stop_requested(); };
    worker_loop(*ring_p, *sk_p, stopq, merging_p, paused_p);
  });
}
static ReaderThread start_reader(const GlobalOptions& g, const std::vector<spsc_ring<LineItem>*>& rings,
                                 int num_workers, std::atomic<bool>& done) {
  return ReaderThread([&](std::stop_token rst) {
    std::ifstream file_in;
    std::istream* in = nullptr;
    if (!open_input(g, file_in, in)) {
      done.store(true, std::memory_order_release);
      return;
    }
    std::string line;
    line.reserve(256);
    std::uint64_t processed = 0;
    while (!rst.stop_requested()) {
      if (!std::getline(*in, line))
        break;
      const std::uint64_t hv = hash64(line, g.hash);
      const int shard = static_cast<int>(hv % static_cast<std::uint64_t>(num_workers));
      dispatch_line(*rings[static_cast<std::size_t>(shard)], line);
      if (g.stop_after && ++processed >= g.stop_after)
        break;
    }
    done.store(true, std::memory_order_release);
  });
}
#else
void spawn_worker(std::vector<WorkerThread>& ws, spsc_ring<LineItem>& ring, probkit::hll::sketch& sk,
                  std::atomic<bool>& done, std::atomic<bool>* merging, std::atomic<int>* paused) {
  auto* ring_p = &ring;
  auto* sk_p = &sk;
  auto* done_p = &done;
  auto* merging_p = merging;
  auto* paused_p = paused;
  ws.emplace_back([ring_p, sk_p, done_p, merging_p, paused_p]() -> void {
    auto stopq = [done_p]() -> bool { return done_p->load(std::memory_order_acquire); };
    worker_loop(*ring_p, *sk_p, stopq, merging_p, paused_p);
  });
}
auto start_reader(const GlobalOptions& g, const std::vector<spsc_ring<LineItem>*>& rings, int num_workers,
                  std::atomic<bool>& done) -> ReaderThread {
  return ReaderThread([&]() -> void {
    std::ifstream file_in;
    std::istream* in = nullptr;
    if (!open_input(g, file_in, in)) {
      done.store(true, std::memory_order_release);
      return;
    }
    std::string line;
    line.reserve(256);
    std::uint64_t processed = 0;
    while (true) {
      if (!std::getline(*in, line)) {
        break;
      }
      const std::uint64_t hv = hash64(line, g.hash);
      const int shard = static_cast<int>(hv % static_cast<std::uint64_t>(num_workers));
      dispatch_line(*rings[static_cast<std::size_t>(shard)], line);
      if (g.stop_after && ++processed >= g.stop_after) {
        break;
      }
    }
    done.store(true, std::memory_order_release);
  });
}
#endif
} // namespace
} // namespace probkit::cli

// ==================== Reducer (bucketed output) ====================
namespace probkit::cli {
namespace {
auto start_reducer_hll(const GlobalOptions& g, std::vector<probkit::hll::sketch>& locals, std::uint8_t p,
                       std::atomic<bool>& done, std::atomic<int>& paused, std::atomic<bool>& merging, int num_workers,
                       std::atomic<bool>& workers_ended) -> ReducerThread {
#if PROBKIT_HAS_JTHREAD && PROBKIT_HAS_STOP_TOKEN
  return ReducerThread([&, p](std::stop_token st) {
#else
  return ReducerThread([&, p]() -> void {
#endif
    std::chrono::nanoseconds bucket_ns{};
    if (!parse_duration(g.bucket, bucket_ns)) {
      std::fputs("error: invalid --bucket value\n", stderr);
      return;
    }
    if (bucket_ns < std::chrono::seconds(1)) {
      bucket_ns = std::chrono::seconds(1);
    }

    Timebase tb{};
    auto bucket_start = std::chrono::steady_clock::now();
    auto bucket_end = bucket_start + bucket_ns;

    auto acc_r = probkit::hll::sketch::make_by_precision(p, locals.empty() ? g.hash : locals.front().hash_config());
    if (!acc_r) {
      std::fputs("error: hll reducer init failed\n", stderr);
      return;
    }
    auto acc = std::move(acc_r.value());

    const auto sleep_quanta = std::chrono::milliseconds(50);
    while (
#if PROBKIT_HAS_JTHREAD && PROBKIT_HAS_STOP_TOKEN
        !st.stop_requested()
#else
        true
#endif
    ) {
      std::this_thread::sleep_for(sleep_quanta);
      const auto now = std::chrono::steady_clock::now();
      const bool finishing = done.load(std::memory_order_acquire) && workers_ended.load(std::memory_order_acquire);
      const bool need_rotate = now >= bucket_end || finishing;
      if (!need_rotate) {
        continue;
      }
      // Pause workers and wait only if not finishing
      if (!finishing) {
        merging.store(true, std::memory_order_release);
        while (paused.load(std::memory_order_acquire) < num_workers) {
          std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
      }
      // Merge locals into accumulator
      for (auto& tl : locals) {
        (void)acc.merge(tl);
      }
      // Emit
      auto est = acc.estimate();
      if (est) {
        const auto ts = format_utc_iso8601(tb.to_system(bucket_start));
        if (g.json) {
          std::printf("{\"ts\":\"%s\",\"uu\":%.0f,\"m\":%zu}\n", ts.c_str(), est.value(), acc.m());
        } else {
          std::printf("%s\tuu=%.0f m=%zu\n", ts.c_str(), est.value(), acc.m());
        }
      } else {
        std::fputs("error: hll estimate failed\n", stderr);
      }
      // Reset
      for (auto& tl : locals) {
        auto s = probkit::hll::sketch::make_by_precision(p, tl.hash_config());
        if (s) {
          tl = std::move(s.value());
        }
      }
      auto new_acc_r = probkit::hll::sketch::make_by_precision(p, acc.hash_config());
      if (new_acc_r) {
        acc = std::move(new_acc_r.value());
      }
      if (!finishing) {
        paused.store(0, std::memory_order_release);
        merging.store(false, std::memory_order_release);
      }

      if (finishing) {
        break;
      }
      bucket_start = bucket_end;
      bucket_end = bucket_start + bucket_ns;
    }
  });
}
} // namespace
} // namespace probkit::cli
