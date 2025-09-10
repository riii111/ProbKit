#include "probkit/hll.hpp"
#include "probkit/error.hpp"
#include <algorithm>
#include <cmath>

using probkit::errc;
using probkit::make_error;
using probkit::result;
using probkit::hashing::hash64;
using probkit::hashing::HashConfig;

namespace probkit::hll {

namespace {
// Empirical alpha_m constant per classical HLL
inline auto alpha(std::size_t m) noexcept -> double {
  switch (m) {
  case 16:
    return 0.673;
  case 32:
    return 0.697;
  case 64:
    return 0.709;
  default:
    break;
  }
  return 0.7213 / (1.0 + 1.079 / static_cast<double>(m));
}

inline auto leading_zeroes_64(std::uint64_t v) noexcept -> int {
  if (v == 0) {
    return 64;
  }
#if defined(__clang__) || defined(__GNUC__)
  return __builtin_clzll(v);
#else
  // Portable fallback
  int n = 0;
  for (int i = 63; i >= 0; --i) {
    if ((v >> i) & 1ULL) {
      break;
    }
    ++n;
  }
  return n;
#endif
}

inline auto rho_from_hash(std::uint64_t h, std::uint8_t p) noexcept -> std::uint8_t {
  const int lz = leading_zeroes_64((h << p) | (1ULL << (p - 1))); // all-zero case gives max
  const int rank = lz + 1;                                        // rho in [1..]
  const int max_rho = 64 - static_cast<int>(p) + 1;
  return static_cast<std::uint8_t>(rank > max_rho ? max_rho : rank);
}
} // namespace

auto sketch::make_by_precision(std::uint8_t p, HashConfig h) -> result<sketch> {
  if (p < 4U || p > 20U) { // guard reasonable bounds
    return result<sketch>::from_error(make_error(errc::invalid_argument, "precision out of range"));
  }
  const std::size_t m = 1ULL << p;
  std::vector<std::uint8_t> regs(m, 0U);
  sketch s{p, h, std::move(regs)};
  return s;
}

auto sketch::add(std::string_view x) noexcept -> result<void> {
  const std::uint64_t h = hash64(x, hash_cfg_);
  const auto mval = static_cast<std::size_t>(1ULL << p_);
  const std::size_t idx = static_cast<std::size_t>(h >> (64U - p_)) & (mval - 1U);
  const std::uint8_t r = rho_from_hash(h, p_);
  std::uint8_t& cell = registers_[idx];
  cell = std::max(r, cell);
  return {};
}

auto sketch::estimate() const noexcept -> result<double> {
  const auto m = static_cast<std::size_t>(1ULL << p_);
  double sum = 0.0;
  std::size_t zeros = 0;
  for (std::uint8_t v : registers_) {
    if (v == 0U) {
      ++zeros;
    }
    sum += std::ldexp(1.0, -static_cast<int>(v)); // 2^{-v}
  }
  const double inv_sum = 1.0 / sum;
  const double am = alpha(m);
  double E = am * static_cast<double>(m) * static_cast<double>(m) * inv_sum;

  // Small range correction (linear counting) per original HLL
  if (E <= 2.5 * static_cast<double>(m) && zeros > 0) {
    E = static_cast<double>(m) * std::log(static_cast<double>(m) / static_cast<double>(zeros));
  } else if (E > (1.0 / 30.0) * 18446744073709551616.0) { // 2^64 / 30
    // Large range correction using 64-bit hash space saturation
    const double two64 = 18446744073709551616.0; // 2^64
    E = -two64 * std::log(1.0 - (E / two64));
  }
  return E;
}

auto sketch::merge(const sketch& other) noexcept -> result<void> {
  if (!same_params(other)) {
    return result<void>::from_error(make_error(errc::invalid_argument, "incompatible hll merge"));
  }
  for (std::size_t i = 0; i < registers_.size(); ++i) {
    registers_[i] = std::max(other.registers_[i], registers_[i]);
  }
  return {};
}

} // namespace probkit::hll
