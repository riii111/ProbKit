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
  auto mrg = s1.merge(s2);
  assert(mrg.has_value());

  const auto est = s1.estimate();
  assert(est.has_value());
  const double v = est.value(); // NOLINT(clang-diagnostic-unused-variable)

  const auto m = static_cast<double>(1U << p);
  const double rel = 1.04 / std::sqrt(m);
  const double lo = static_cast<double>(2 * n) * (1.0 - 5.0 * rel); // NOLINT
  const double hi = static_cast<double>(2 * n) * (1.0 + 5.0 * rel); // NOLINT
  if (v < lo || v > hi) {
    std::fprintf(stderr, "HLL estimate %.2f not in [%.2f, %.2f]\n", v, lo, hi);
  }
  assert(v >= lo && v <= hi);
}

void run_hll_tests() {
  test_hll_basic_accuracy_and_merge();
}

} // namespace tests
