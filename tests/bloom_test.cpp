#include <cassert>
#include <cmath>
#include <cstdio>
#include <string>

#include "probkit/bloom.hpp"

using probkit::bloom::filter;
using probkit::hashing::HashConfig;

namespace tests {

static auto fp_theory(double k, double n, double m_bits) -> double {
  const double t = -k * n / m_bits;
  const double base = 1.0 - std::exp(t);
  return std::pow(base, k);
}

static void test_insert_and_query_no_false_negative() {
  auto f = filter::make_by_mem(16UL * 1024UL, HashConfig{});
  assert(f.has_value());
  filter bf = std::move(f.value());
  const int n = 5000;
  for (int i = 0; i < n; ++i) {
    std::string s = std::string("key-") + std::to_string(i);
    [[maybe_unused]] auto ok = bf.add(s);
    assert(ok.has_value());
  }
  for (int i = 0; i < n; ++i) {
    std::string s = std::string("key-") + std::to_string(i);
    auto q = bf.might_contain(s);
    assert(q.has_value() && q.value());
  }
}

static void test_false_positive_rate_matches_theory() {
  auto f = filter::make_by_mem(16UL * 1024UL, HashConfig{});
  assert(f.has_value());
  filter bf = std::move(f.value());
  const int n = 20000;
  for (int i = 0; i < n; ++i) {
    std::string s = std::string("A-") + std::to_string(i);
    [[maybe_unused]] auto ok = bf.add(s);
    assert(ok.has_value());
  }

  int fp = 0;
  const int trials = 20000;
  for (int i = 0; i < trials; ++i) {
    std::string s = std::string("B-") + std::to_string(i + 1000000);
    auto q = bf.might_contain(s);
    assert(q.has_value());
    fp += q.value() ? 1 : 0;
  }
  const double rate = static_cast<double>(fp) / static_cast<double>(trials);
  const double p_theory =
      fp_theory(static_cast<double>(bf.k()), static_cast<double>(n), static_cast<double>(bf.bit_size()));

  const double abs_tol = 0.01;
  const double rel_tol = 0.5 * p_theory;
  const double tol = abs_tol > rel_tol ? abs_tol : rel_tol;
  const double diff = std::fabs(rate - p_theory);
  if (diff > tol) {
    std::fprintf(stderr, "FP rate off: measured=%.6f theory=%.6f tol=%.6f\n", rate, p_theory, tol);
  }
  assert(diff <= tol);
}

static void test_merge_union_and_no_false_negative() {
  HashConfig h{};
  auto a = filter::make_by_mem(16UL * 1024UL, h);
  auto b = filter::make_by_mem(16UL * 1024UL, h);
  assert(a.has_value() && b.has_value());
  filter fa = std::move(a.value());
  filter fb = std::move(b.value());

  for (int i = 0; i < 3000; ++i) {
    [[maybe_unused]] auto ok1 = fa.add(std::string("L-") + std::to_string(i));
    [[maybe_unused]] auto ok2 = fb.add(std::string("R-") + std::to_string(i));
  }

  auto m = fa.merge(fb);
  assert(m.has_value());

  for (int i = 0; i < 3000; ++i) {
    auto q1 = fa.might_contain(std::string("L-") + std::to_string(i));
    auto q2 = fa.might_contain(std::string("R-") + std::to_string(i));
    assert(q1.has_value() && q2.has_value());
    assert(q1.value());
    assert(q2.value());
  }
}

void run_bloom_tests() {
  test_insert_and_query_no_false_negative();
  test_false_positive_rate_matches_theory();
  test_merge_union_and_no_false_negative();
}

} // namespace tests
