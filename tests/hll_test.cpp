#include <cassert>
#include <cmath>
#include <cstdio>
#include <string>

#include "probkit/hll.hpp"

using probkit::hashing::HashConfig;
using probkit::hll::sketch;

namespace tests {

static void test_hll_basic_accuracy_and_merge() {
  const std::uint8_t p = 12; // m=4096
  auto s1r = sketch::make_by_precision(p, HashConfig{});
  auto s2r = sketch::make_by_precision(p, HashConfig{});
  assert(s1r.has_value() && s2r.has_value());
  auto s1 = std::move(s1r.value());
  auto s2 = std::move(s2r.value());

  const int n = 50000;
  for (int i = 0; i < n; ++i) {
    std::string a = std::string("k-") + std::to_string(i);
    std::string b = std::string("k-") + std::to_string(i + n);
    [[maybe_unused]] auto ok1 = s1.add(a);
    [[maybe_unused]] auto ok2 = s2.add(b);
  }
  auto merged = s1.merge(s2);
  assert(merged.has_value());

  const auto estimate_r = s1.estimate();
  assert(estimate_r.has_value());
  const double v = estimate_r.value(); // NOLINT(clang-diagnostic-unused-variable)

  const auto m = static_cast<double>(1U << p);
  const double rel = 1.04 / std::sqrt(m);
  const double lo = static_cast<double>(2 * n) * (1.0 - 5.0 * rel);
  const double hi = static_cast<double>(2 * n) * (1.0 + 5.0 * rel);
  if (v < lo || v > hi) {
    std::fprintf(stderr, "HLL estimate %.2f not in [%.2f, %.2f]\n", v, lo, hi);
  }
  assert(v >= lo && v <= hi);
}

static void test_hll_linear_counting_region() {
  const std::uint8_t p = 12; // m=4096
  auto s1r = sketch::make_by_precision(p, HashConfig{});
  assert(s1r.has_value());
  auto s = std::move(s1r.value());
  const int n = 500; // << m -> linear counting should kick in
  for (int i = 0; i < n; ++i) {
    std::string k = std::string("x-") + std::to_string(i);
    (void)s.add(k);
  }
  auto estimate_r = s.estimate();
  assert(estimate_r.has_value());
  const double v = estimate_r.value();
  const double lo = n * 0.85; // relaxed lower bound
  const double hi = n * 1.15; // relaxed upper bound
  if (v < lo || v > hi) {
    std::fprintf(stderr, "HLL estimate (linear region) %.2f not in [%.2f, %.2f]\n", v, lo, hi);
  }
  assert(v >= lo && v <= hi);
}

void run_hll_tests() {
  test_hll_basic_accuracy_and_merge();
  test_hll_linear_counting_region();
}

} // namespace tests
