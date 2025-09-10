#include "options.hpp"
#include "probkit/cms.hpp"
#include "probkit/hash.hpp"
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

template <typename T> class spsc_ring {
public:
  explicit spsc_ring(std::size_t cap) : capacity_(cap), data_(cap) {}

  auto push(const T& v) noexcept -> bool {
    const std::size_t head = head_.load(std::memory_order_relaxed);
    const std::size_t next = (head + 1) % capacity_;
    if (next == tail_.load(std::memory_order_acquire)) {
      return false; // full
    }
    data_[head] = v;
    head_.store(next, std::memory_order_release);
    return true;
  }

  auto pop(T& out) noexcept -> bool {
    const std::size_t tail = tail_.load(std::memory_order_relaxed);
    if (tail == head_.load(std::memory_order_acquire)) {
      return false; // empty
    }
    out = std::move(data_[tail]);
    tail_.store((tail + 1) % capacity_, std::memory_order_release);
    return true;
  }

  auto empty() const noexcept -> bool {
    return tail_.load(std::memory_order_acquire) == head_.load(std::memory_order_acquire);
  }

private:
  std::size_t capacity_;
  std::vector<T> data_;
  std::atomic<std::size_t> head_{0};
  std::atomic<std::size_t> tail_{0};
};

struct CmsOptions {
  bool show_help{false};
  bool have_eps{false};
  bool have_delta{false};
  double eps{1e-3};
  double delta{1e-4};
  std::size_t topk{0};
};

inline void print_help() {
  std::fputs("usage: probkit cms [--eps=<e>] [--delta=<d>] [--topk=<k>]\n", stdout);
}

[[nodiscard]] inline auto parse_u64(std::string_view s, std::uint64_t& out) -> bool {
  char* end = nullptr;
  std::string tmp{s};
  const unsigned long long v = std::strtoull(tmp.c_str(), &end, 10);
  if (end == tmp.c_str() || *end != '\0') {
    return false;
  }
  out = static_cast<std::uint64_t>(v);
  return true;
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
      if (!parse_double(a.substr(std::string_view{"--eps="}.size()), v) || v <= 0.0) {
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

struct LineItem {
  std::string data;
};

inline auto open_input(const GlobalOptions& g, std::ifstream& file_in, std::istream*& in) -> bool {
  if (g.file_path.empty()) {
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
      LineItem item{line};
      auto& ring = *rings[static_cast<std::size_t>(shard)];
      while (!ring.push(item)) {
        std::this_thread::sleep_for(std::chrono::microseconds(50));
      }
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

  const int worker_count = (g.threads > 0) ? g.threads : static_cast<int>(std::thread::hardware_concurrency());
  const int num_workers = worker_count > 0 ? worker_count : 1;
  const std::size_t ring_capacity = 1U << 14;

  std::vector<spsc_ring<LineItem>*> rings;
  rings.reserve(static_cast<std::size_t>(num_workers));
  std::vector<std::unique_ptr<spsc_ring<LineItem>>> ring_storage;
  ring_storage.reserve(static_cast<std::size_t>(num_workers));
  for (int i = 0; i < num_workers; ++i) {
    ring_storage.emplace_back(new spsc_ring<LineItem>(ring_capacity));
    rings.push_back(ring_storage.back().get());
  }

  // Thread-local sketches
  std::vector<probkit::cms::sketch> locals;
  locals.reserve(static_cast<std::size_t>(num_workers));
  for (int i = 0; i < num_workers; ++i) {
    probkit::hashing::HashConfig hc = g.hash;
    const std::uint64_t thread_index = static_cast<std::uint64_t>(i) + 1ULL;
    hc.thread_salt = probkit::hashing::derive_thread_salt(hc.seed, thread_index);
    auto s = probkit::cms::sketch::make_by_eps_delta(co.have_eps ? co.eps : 1e-3, co.have_delta ? co.delta : 1e-4, hc);
    if (!s) {
      std::fputs("error: failed to init worker cms\n", stderr);
      return CommandResult::ConfigError;
    }
    locals.emplace_back(std::move(s.value()));
  }

  std::atomic<bool> done{false};
  std::atomic<std::uint64_t> processed_total{0};

  // Workers
  std::vector<WorkerThread> workers;
  workers.reserve(static_cast<std::size_t>(num_workers));
  for (int wi = 0; wi < num_workers; ++wi) {
    spawn_worker(workers, *rings[static_cast<std::size_t>(wi)], locals[static_cast<std::size_t>(wi)], done);
  }

  // Reader
  ReaderThread reader = start_reader(g, rings, num_workers, done, processed_total);

  // Optional periodic stats
#if PROBKIT_HAS_JTHREAD && PROBKIT_HAS_STOP_TOKEN
  std::jthread stats_thr([&](std::stop_token st) {
#else
  std::thread stats_thr([&]() -> void {
#endif
    using std::chrono_literals::operator""s;
    const auto interval = std::chrono::seconds(g.stats ? g.stats_interval_seconds : 1U);
    while (
#if PROBKIT_HAS_JTHREAD && PROBKIT_HAS_STOP_TOKEN
        !st.stop_requested()
#else
        true
#endif
    ) {
      std::this_thread::sleep_for(interval);
      if (g.stats) {
        const auto proc = processed_total.load(std::memory_order_relaxed);
        std::fprintf(stderr, "processed=%llu\n", static_cast<unsigned long long>(proc));
      }
      if (done.load(std::memory_order_acquire)) {
        break;
      }
    }
  });

  // Wait and finalize
  reader.join();
#if PROBKIT_HAS_JTHREAD && PROBKIT_HAS_STOP_TOKEN
  for (auto& w : workers) {
    w.request_stop();
  }
  stats_thr.request_stop();
#else
  for (auto& w : workers) {
    w.join();
  }
#endif
  stats_thr.join();

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
      std::fputs("{\"topk\":[", stdout);
      for (std::size_t i = 0; i < items.size(); ++i) {
        const auto& it = items[i];
        std::fprintf(stdout, R"(%s{"key":"%s","est":%llu})", ((i != 0U) ? "," : ""), it.key.c_str(),
                     static_cast<unsigned long long>(it.est));
      }
      std::fputs("]}\n", stdout);
    } else {
      for (const auto& it : items) {
        std::fprintf(stdout, "%s\t%llu\n", it.key.c_str(), static_cast<unsigned long long>(it.est));
      }
    }
  } else {
    if (g.json) {
      auto [d, w] = global.dims();
      std::fprintf(stdout, "{\"depth\":%zu,\"width\":%zu}\n", d, w);
    } else {
      std::fputs("cms: processed\n", stdout);
    }
  }
  return CommandResult::Success;
}

} // namespace probkit::cli
