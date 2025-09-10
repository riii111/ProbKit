#include "probkit/bloom.hpp"
#include <cmath>
#include <cstring>
#if __has_include(<numbers>)
#include <numbers>
#endif
#include <vector>

using probkit::errc;
using probkit::make_error;
using probkit::result;
using probkit::hashing::hash64;
using probkit::hashing::HashConfig;

namespace probkit::bloom {

namespace {
constexpr std::size_t kMinBytes = 8;          // at least one 64-bit word
constexpr std::size_t kCapacityHint = 100000; // default n for make_by_fp
#if defined(__cpp_lib_math_constants) && (__cpp_lib_math_constants >= 201907L)
constexpr double kLn2 = std::numbers::ln2; // prefer standard constant when available
#else
// Fallback when <numbers> is unavailable in the toolchain
constexpr double kLn2 = 0.693147180559945309;
#endif

inline auto round_up_bits_to_words(std::size_t bits) -> std::size_t {
  return (bits + 63U) / 64U; // number of 64-bit words
}
} // namespace

auto filter::make_by_mem(std::size_t bytes, HashConfig h) -> result<filter> {
  if (bytes < kMinBytes) {
    return result<filter>::from_error(make_error(errc::invalid_argument, "mem too small"));
  }
  const std::size_t words = bytes / 8U;
  if (words == 0U) {
    return result<filter>::from_error(make_error(errc::invalid_argument, "mem not aligned"));
  }
  const std::size_t m_bits = words * 64U;
  std::vector<std::uint64_t> storage(words, 0ULL);
  filter f{std::move(storage), filter::layout{.bit_count = m_bits, .k = kDefaultK}, h};
  return f;
}

auto filter::make_by_fp(double p, HashConfig h) -> result<filter> {
  return make_by_fp(p, h, kCapacityHint);
}

auto filter::make_by_fp(double p, HashConfig h, std::size_t capacity_hint) -> result<filter> {
  if (!(p > 0.0) || !(p < 1.0)) {
    return result<filter>::from_error(make_error(errc::invalid_argument, "fp out of range"));
  }
  const double k_real = -std::log(p) / kLn2;
  std::uint8_t k = static_cast<std::uint8_t>(std::lround(std::max(1.0, std::min(32.0, k_real))));
  const double m_per_n = -std::log(p) / (kLn2 * kLn2);
  const double m_bits_d = std::ceil(m_per_n * static_cast<double>(capacity_hint));
  const auto m_bits = static_cast<std::size_t>(m_bits_d);
  const std::size_t words = round_up_bits_to_words(m_bits);
  std::vector<std::uint64_t> storage(words, 0ULL);
  filter f{std::move(storage), filter::layout{.bit_count = words * 64U, .k = k}, h};
  return f;
}

auto filter::add(std::string_view x) noexcept -> result<void> {
  // Double-hashing: h(i) = h1 + i*h2 (mod m)
  const std::uint64_t h1 = hash64(x, hash_cfg_);
  HashConfig cfg2 = hash_cfg_;
  cfg2.seed = second_seed();
  const std::uint64_t h2 = hash64(x, cfg2) | 1ULL; // make it odd to reduce cycles
  for (std::uint8_t i = 0; i < k_; ++i) {
    const std::size_t bit = mod_bit(h1 + (static_cast<std::uint64_t>(i) * h2));
    bits_[index_word(bit)] |= index_mask(bit);
  }
  return {};
}

auto filter::might_contain(std::string_view x) const noexcept -> result<bool> {
  const std::uint64_t h1 = hash64(x, hash_cfg_);
  HashConfig cfg2 = hash_cfg_;
  cfg2.seed = second_seed();
  const std::uint64_t h2 = hash64(x, cfg2) | 1ULL;
  for (std::uint8_t i = 0; i < k_; ++i) {
    const std::size_t bit = mod_bit(h1 + (static_cast<std::uint64_t>(i) * h2));
    if ((bits_[index_word(bit)] & index_mask(bit)) == 0ULL) {
      return false;
    }
  }
  return true;
}

auto filter::merge(const filter& other) noexcept -> result<void> {
  const bool hash_same = (hash_cfg_.kind == other.hash_cfg_.kind) && (hash_cfg_.seed == other.hash_cfg_.seed) &&
                         (hash_cfg_.thread_salt == other.hash_cfg_.thread_salt);
  if (m_bits_ != other.m_bits_ || k_ != other.k_ || !hash_same) {
    return result<void>::from_error(make_error(errc::invalid_argument, "incompatible bloom merge"));
  }
  for (std::size_t i = 0; i < bits_.size(); ++i) {
    bits_[i] |= other.bits_[i];
  }
  return {};
}

} // namespace probkit::bloom
