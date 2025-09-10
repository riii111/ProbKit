#include "options.hpp"
#include "probkit/hash.hpp"
#include "probkit/hll.hpp"
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
template <class StopQ> static void worker_loop(spsc_ring<LineItem>& ring, probkit::hll::sketch& sk, StopQ stopq);
#if PROBKIT_HAS_JTHREAD
using WorkerThread = std::jthread;
using ReaderThread = std::jthread;
#else
using WorkerThread = std::thread;
using ReaderThread = std::thread;
#endif
static void spawn_worker(std::vector<WorkerThread>& ws, spsc_ring<LineItem>& ring, probkit::hll::sketch& sk,
                         std::atomic<bool>& done);
static auto start_reader(const GlobalOptions& g, const std::vector<spsc_ring<LineItem>*>& rings, int num_workers,
                         std::atomic<bool>& done) -> ReaderThread;
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

  const int worker_count = (g.threads > 0) ? g.threads : static_cast<int>(std::thread::hardware_concurrency());
  const int num_workers = worker_count > 0 ? worker_count : 1;
  const std::size_t ring_capacity = 1U << 14; // 16384 slots

  std::vector<spsc_ring<LineItem>*> rings;
  rings.reserve(static_cast<std::size_t>(num_workers));
  std::vector<std::unique_ptr<spsc_ring<LineItem>>> ring_storage;
  ring_storage.reserve(static_cast<std::size_t>(num_workers));
  for (int i = 0; i < num_workers; ++i) {
    ring_storage.emplace_back(new spsc_ring<LineItem>(ring_capacity));
    rings.push_back(ring_storage.back().get());
  }

  // thread-local sketches (derive per-thread salt)
  std::vector<probkit::hll::sketch> locals;
  locals.reserve(static_cast<std::size_t>(num_workers));
  for (int i = 0; i < num_workers; ++i) {
    probkit::hashing::HashConfig hc = g.hash;
    const std::uint64_t thread_index = static_cast<std::uint64_t>(i) + 1ULL;
    hc.thread_salt = probkit::hashing::derive_thread_salt(hc.seed, thread_index);
    auto s = probkit::hll::sketch::make_by_precision(p, hc);
    if (!s) {
      std::fputs("error: failed to init worker sketch\n", stderr);
      return CommandResult::ConfigError;
    }
    locals.emplace_back(std::move(s.value()));
  }

  std::atomic<bool> done{false};

  // Workers
#if PROBKIT_HAS_JTHREAD
  std::vector<std::jthread> workers;
#else
  std::vector<std::thread> workers;
#endif
  workers.reserve(static_cast<std::size_t>(num_workers));
  for (int wi = 0; wi < num_workers; ++wi) {
    spawn_worker(workers, *rings[static_cast<std::size_t>(wi)], locals[static_cast<std::size_t>(wi)], done);
  }

  // Reader
  ReaderThread reader = start_reader(g, rings, num_workers, done);

  // Wait: reader then workers
  reader.join();
#if PROBKIT_HAS_JTHREAD && PROBKIT_HAS_STOP_TOKEN
  for (auto& w : workers) {
    w.request_stop();
    // jthread joins on destruction; explicit stop to hasten exit if sleeping
  }
#else
  for (auto& w : workers) {
    w.join();
  }
#endif

  // Reducer: merge locals
  auto global = std::move(sketch_r.value());
  for (auto& tl : locals) {
    (void)global.merge(tl);
  }

  // Final output (non-bucketed for PR-21)
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
  while (!ring.try_emplace(std::move(line))) {
    std::this_thread::sleep_for(50us);
  }
}

template <class StopQ> inline void worker_loop(spsc_ring<LineItem>& ring, probkit::hll::sketch& sk, StopQ stopq) {
  LineItem item;
  while (true) {
    if (ring.pop(item)) {
      (void)sk.add(item.data);
    } else if (stopq()) {
      break;
    } else {
      std::this_thread::sleep_for(std::chrono::microseconds(50));
    }
  }
}
#if PROBKIT_HAS_JTHREAD && PROBKIT_HAS_STOP_TOKEN
static void spawn_worker(std::vector<WorkerThread>& ws, spsc_ring<LineItem>& ring, probkit::hll::sketch& sk,
                         std::atomic<bool>& done) {
  ws.emplace_back([&](std::stop_token st) {
    auto stopq = [&]() -> bool { return done.load(std::memory_order_acquire) || st.stop_requested(); };
    worker_loop(ring, sk, stopq);
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
    int shard = 0;
    while (!rst.stop_requested()) {
      if (!std::getline(*in, line))
        break;
      dispatch_line(*rings[static_cast<std::size_t>(shard)], line);
      shard = (shard + 1) % num_workers;
      if (g.stop_after && ++processed >= g.stop_after)
        break;
    }
    done.store(true, std::memory_order_release);
  });
}
#else
void spawn_worker(std::vector<WorkerThread>& ws, spsc_ring<LineItem>& ring, probkit::hll::sketch& sk,
                  std::atomic<bool>& done) {
  ws.emplace_back([&]() -> void {
    auto stopq = [&]() -> bool { return done.load(std::memory_order_acquire); };
    worker_loop(ring, sk, stopq);
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
    int shard = 0;
    while (true) {
      if (!std::getline(*in, line)) {
        break;
      }
      dispatch_line(*rings[static_cast<std::size_t>(shard)], line);
      shard = (shard + 1) % num_workers;
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
