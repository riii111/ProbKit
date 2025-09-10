#include "options.hpp"
#include "probkit/hash.hpp"
#include "probkit/hll.hpp"
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
#if defined(__has_include)
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

using probkit::cli::util::sv_starts_with;

namespace probkit::cli {

// GlobalOptions comes from options.hpp

namespace {

// Minimal fixed-size SPSC ring for pointers to lines (owning storage external)
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

struct HllOptions {
  bool show_help{false};
  bool have_precision{false};
  std::uint8_t precision{14};
};

inline void print_help() {
  std::fputs("usage: probkit hll [--precision=<p>]\n", stdout);
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

} // namespace

// Reader → Workers → Reducer minimal pipeline for HLL
auto cmd_hll(int argc, char** argv, const GlobalOptions& g) -> int {
  const HllOptions ho = parse_hll_opts(argc, argv);
  if (ho.show_help) {
    print_help();
    return 0;
  }

  const std::uint8_t p = ho.have_precision ? ho.precision : 14;
  auto sketch_r = probkit::hll::sketch::make_by_precision(p, g.hash);
  if (!sketch_r) {
    std::fputs("error: failed to init hll\n", stderr);
    return 5;
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
      return 5;
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
#if PROBKIT_HAS_JTHREAD && PROBKIT_HAS_STOP_TOKEN
    workers.emplace_back([&, wi](std::stop_token st) -> void {
      LineItem item;
      auto& ring = *rings[static_cast<std::size_t>(wi)];
      auto& sk = locals[static_cast<std::size_t>(wi)];
      while (true) {
        if (ring.pop(item)) {
          (void)sk.add(item.data);
        } else if (done.load(std::memory_order_acquire) || st.stop_requested()) {
          // Drain complete
          break;
        } else {
          std::this_thread::sleep_for(std::chrono::microseconds(50));
        }
      }
    });
#else
    workers.emplace_back([&, wi]() -> void {
      LineItem item;
      auto& ring = *rings[static_cast<std::size_t>(wi)];
      auto& sk = locals[static_cast<std::size_t>(wi)];
      while (true) {
        if (ring.pop(item)) {
          (void)sk.add(item.data);
        } else if (done.load(std::memory_order_acquire)) {
          // Drain complete
          break;
        } else {
          std::this_thread::sleep_for(std::chrono::microseconds(50));
        }
      }
    });
#endif
  }

// Reader
#if PROBKIT_HAS_JTHREAD && PROBKIT_HAS_STOP_TOKEN
  std::jthread reader([&](std::stop_token rst) -> void {
    // Select input stream
    std::ifstream file_in;
    std::istream* in = &std::cin;
    if (!g.file_path.empty()) {
      file_in.open(g.file_path, std::ios::in);
      if (!file_in.is_open()) {
        std::fputs("error: failed to open --file\n", stderr);
        done.store(true, std::memory_order_release);
        return;
      }
      in = &file_in;
    }

    std::string line;
    line.reserve(256);
    std::uint64_t processed = 0;
    int shard = 0;
    while (!rst.stop_requested()) {
      if (!std::getline(*in, line)) {
        break;
      }
      LineItem item{line};
      // Push to ring with simple round-robin sharding
      auto& ring = *rings[static_cast<std::size_t>(shard)];
      // backpressure
      while (!ring.push(item)) {
        std::this_thread::sleep_for(std::chrono::microseconds(50));
      }
      shard = (shard + 1) % num_workers;
      if (g.stop_after && ++processed >= g.stop_after) {
        break;
      }
    }
    done.store(true, std::memory_order_release);
  });
#else
  std::thread reader([&]() -> void {
    std::ifstream file_in;
    std::istream* in = &std::cin;
    if (!g.file_path.empty()) {
      file_in.open(g.file_path, std::ios::in);
      if (!file_in.is_open()) {
        std::fputs("error: failed to open --file\n", stderr);
        done.store(true, std::memory_order_release);
        return;
      }
      in = &file_in;
    }

    std::string line;
    line.reserve(256);
    std::uint64_t processed = 0;
    int shard = 0;
    while (true) {
      if (!std::getline(*in, line)) {
        break;
      }
      LineItem item{line};
      auto& ring = *rings[static_cast<std::size_t>(shard)];
      while (!ring.push(item)) {
        std::this_thread::sleep_for(std::chrono::microseconds(50));
      }
      shard = (shard + 1) % num_workers;
      if (g.stop_after && ++processed >= g.stop_after) {
        break;
      }
    }
    done.store(true, std::memory_order_release);
  });
#endif

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
    return 5;
  }
  if (g.json) {
    std::printf("{\"uu\":%.0f,\"m\":%zu}\n", est.value(), global.m());
  } else {
    std::printf("uu=%.0f m=%zu\n", est.value(), global.m());
  }
  return 0;
}

} // namespace probkit::cli
