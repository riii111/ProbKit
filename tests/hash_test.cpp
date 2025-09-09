#include <algorithm>
#include <array>
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <ranges>
#include <string>
#include <string_view>
#include <vector>

#include "probkit/hash.hpp"

using probkit::hashing::derive_thread_salt;
using probkit::hashing::hash64;
using probkit::hashing::HashConfig;
using probkit::hashing::HashKind;

namespace tests {

static inline void check(bool ok, const char* msg) {
  if (!ok) {
    std::fputs(msg, stderr);
    std::fputc('\n', stderr);
    std::exit(1);
  }
}

// Generate compact boundary lenses around the algorithm chunk thresholds.
static auto make_boundary_lens() -> std::vector<int> {
  constexpr std::array<int, 6> pivots{0, 4, 8, 16, 32, 64};
  std::vector<int> out;
  out.reserve(static_cast<std::size_t>(3) * pivots.size());
  auto push = [&](int v) -> void {
    if (v >= 0) {
      out.push_back(v);
    }
  };
  for (int p : pivots) {
    if (p == 0) {
      push(0);
      push(1); // special-case 0/-1/+1
    } else {
      push(p - 1);
      push(p);
      push(p + 1);
    }
  }
#if defined(__cpp_lib_ranges) && (__cpp_lib_ranges >= 201911L)
  std::ranges::sort(out);
  const auto r = std::ranges::unique(out);
  out.erase(r.begin(), r.end());
#else
  std::sort(out.begin(), out.end());
  out.erase(std::unique(out.begin(), out.end()), out.end());
#endif
  return out;
}

static void run_boundary_lengths(HashKind kind, const char* label) {
  HashConfig cfg{};
  cfg.kind = kind;
  std::uint64_t prev = 0;
  bool have_prev = false;
  for (int L : make_boundary_lens()) {
    const std::string s(static_cast<std::size_t>(L), 'a');
    const auto h1 = hash64(s, cfg);
    const auto h2 = hash64(s, cfg);
    check(h1 == h2, "reproducibility failed on boundary length");
    if (have_prev) {
      check(prev != h1, "adjacent boundary hashes should differ");
    }
    prev = h1;
    have_prev = true;
  }
  (void)label; // reserved for richer reporting later
}

static void test_reproducible_same_config() {
  const std::string_view input{"probkit-hash"};
  HashConfig cfg{};
  cfg.seed = 123456789ULL;
  cfg.thread_salt = derive_thread_salt(0xABCDEFULL, 5);

  cfg.kind = HashKind::wyhash;
  const auto a1 = hash64(input, cfg);
  const auto a2 = hash64(input, cfg);
  check(a1 == a2, "wyhash reproducibility failed");

  cfg.kind = HashKind::xxhash;
  const auto b1 = hash64(input, cfg);
  const auto b2 = hash64(input, cfg);
  check(b1 == b2, "xxhash reproducibility failed");
}

static void test_kinds_produce_different_values() {
  const std::string_view input{"probkit-hash-kind"};
  HashConfig w{};
  w.kind = HashKind::wyhash;
  w.seed = 777ULL;
  HashConfig x = w;
  x.kind = HashKind::xxhash;
  const auto hw = hash64(input, w);
  const auto hx = hash64(input, x);
  check(hw != hx, "wyhash vs xxhash should differ");
}

static void test_thread_salt_derivation() {
  const auto s1 = derive_thread_salt(0xDEADBEEFULL, 1);
  const auto s2 = derive_thread_salt(0xDEADBEEFULL, 2);
  check(s1 != s2, "thread salt must differ");
  const auto s1_again = derive_thread_salt(0xDEADBEEFULL, 1);
  check(s1 == s1_again, "thread salt must be stable");
}

static void test_empty_and_embedded_zero() {
  HashConfig cfg{};
  cfg.kind = HashKind::wyhash;
  const auto h_empty = hash64(std::string_view{}, cfg);
  (void)h_empty;
  const std::array<char, 3> with_zero{'a', '\0', 'b'};
  const auto h_zero = hash64(std::string_view{with_zero.data(), with_zero.size()}, cfg);
  const auto h_plain = hash64(std::string_view{"ab"}, cfg);
  check(h_zero != h_plain, "embedded zero must affect hash");
}

static void test_seed_effect() {
  const std::string data(64, 'X');
  HashConfig a{};
  a.kind = HashKind::xxhash;
  a.seed = 1ULL;
  HashConfig b = a;
  b.seed = 2ULL;
  check(hash64(data, a) != hash64(data, b), "different seeds must differ");
}

static void test_boundary_lengths() {
  run_boundary_lengths(HashKind::wyhash, "wyhash");
}

static void test_boundary_lengths_xxhash() {
  run_boundary_lengths(HashKind::xxhash, "xxhash");
}

void run_hash_tests() {
  test_reproducible_same_config();
  test_kinds_produce_different_values();
  test_thread_salt_derivation();
  test_empty_and_embedded_zero();
  test_seed_effect();
  test_boundary_lengths();
  test_boundary_lengths_xxhash();
}

} // namespace tests
