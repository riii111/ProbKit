#include "probkit/cms.hpp"
#include "probkit/error.hpp"
#include <algorithm>
#include <cmath>

using probkit::errc;
using probkit::make_error;
using probkit::result;
using probkit::hashing::hash64;
using probkit::hashing::HashConfig;

namespace probkit::cms {

namespace {
inline auto compute_dims(double eps, double delta) -> std::pair<std::size_t, std::size_t> {
  if (!(eps > 0.0) || !(eps < 1.0) || !(delta > 0.0) || !(delta < 1.0)) {
    return {0, 0};
  }
  const double w_d = std::ceil(std::exp(1.0) / eps);
  const double d_d = std::ceil(std::log(1.0 / delta));
  const auto w = static_cast<std::size_t>(w_d);
  const auto d = static_cast<std::size_t>(d_d);
  return {d, w};
}

// Avoid easily swappable adjacent size_t parameters by returning the hash first
inline auto hash_row(std::string_view x, const HashConfig& base, std::size_t row) -> std::uint64_t {
  HashConfig cfg = base;
  cfg.seed ^= (0x9E3779B97F4A7C15ULL * static_cast<std::uint64_t>(row + 1));
  return hash64(x, cfg);
}
} // namespace

auto sketch::make_by_eps_delta(double eps, double delta, HashConfig h) -> result<sketch> {
  auto [d, w] = compute_dims(eps, delta);
  if (d == 0 || w == 0) {
    return result<sketch>::from_error(make_error(errc::invalid_argument, "eps/delta out of range"));
  }
  std::vector<std::uint64_t> counters(d * w, 0ULL);
  std::vector<std::string> keys;
  std::vector<std::uint64_t> ests;
  keys.reserve(0);
  ests.reserve(0);
  sketch s{d, h, w, std::move(counters), std::move(keys), std::move(ests)};
  return s;
}

auto sketch::inc(std::string_view x, std::uint64_t c) noexcept -> result<void> {
  for (std::size_t r = 0; r < depth_; ++r) {
    const std::uint64_t h = hash_row(x, hash_cfg_, r);
    const auto col = static_cast<std::size_t>(h % static_cast<std::uint64_t>(width_));
    const std::size_t idx = (r * width_) + col;
    table_[idx] += c;
  }
  // Optional: sparse candidate tracking can be added later when CLI needs Top-K streaming.
  return {};
}

auto sketch::estimate(std::string_view x) const noexcept -> result<std::uint64_t> {
  std::uint64_t est = UINT64_MAX;
  for (std::size_t r = 0; r < depth_; ++r) {
    const std::uint64_t h = hash_row(x, hash_cfg_, r);
    const auto col = static_cast<std::size_t>(h % static_cast<std::uint64_t>(width_));
    const std::size_t idx = (r * width_) + col;
    est = std::min(est, table_[idx]);
  }
  if (est == UINT64_MAX) {
    est = 0;
  }
  return est;
}

auto sketch::topk(std::size_t k) const -> result<std::vector<Pair>> {
  (void)k;
  (void)depth_;
  return std::vector<Pair>{};
}

auto sketch::merge(const sketch& other) noexcept -> result<void> {
  if (!same_params(other)) {
    return result<void>::from_error(make_error(errc::invalid_argument, "incompatible cms merge"));
  }
  for (std::size_t i = 0; i < table_.size(); ++i) {
    table_[i] += other.table_[i];
  }
  // Candidate structures intentionally ignored for now.
  return {};
}

} // namespace probkit::cms
