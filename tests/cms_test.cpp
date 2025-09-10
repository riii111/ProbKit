#include <cassert>
#include <cstdint>
#include <string>
#include <unordered_map>

#include "probkit/cms.hpp"

using probkit::cms::sketch;
using probkit::hashing::HashConfig;

namespace tests {

static void test_cms_basic_bounds_and_merge() {
  const double eps = 1e-3;
  const double delta = 1e-4;
  auto a = sketch::make_by_eps_delta(eps, delta, HashConfig{});
  auto b = sketch::make_by_eps_delta(eps, delta, HashConfig{});
  assert(a.has_value() && b.has_value());
  auto sa = std::move(a.value());
  auto sb = std::move(b.value());

  std::unordered_map<std::string, std::uint64_t> truth;
  const int n = 10000;
  for (int i = 0; i < n; ++i) {
    std::string hot = std::string("key-") + std::to_string(i % 10);
    std::string cold = std::string("cold-") + std::to_string(i);
    [[maybe_unused]] auto ok1 = sa.inc(hot);
    [[maybe_unused]] auto ok2 = sb.inc(cold);
    ++truth[hot];
    ++truth[cold];
  }

  auto m = sa.merge(sb);
  assert(m.has_value());

  // Check a few keys
  for (int i = 0; i < 10; ++i) {
    std::string k = std::string("key-") + std::to_string(i);
    auto est = sa.estimate(k);
    assert(est.has_value());
    const auto v = est.value(); // NOLINT(clang-diagnostic-unused-variable)
    const auto t = truth[k];    // NOLINT(clang-diagnostic-unused-variable)
    assert(v >= t);             // no underestimation
    // Loose upper bound sanity: should be within a small constant for this test size.
    const auto max_over = static_cast<std::uint64_t>(300ULL);
    if (v - t > max_over) {
      std::fprintf(stderr, "CMS est %llu too high over truth %llu\n", static_cast<unsigned long long>(v),
                   static_cast<unsigned long long>(t));
    }
    assert(v - t <= max_over);
  }
}

void run_cms_tests() {
  test_cms_basic_bounds_and_merge();
}

} // namespace tests
