#include "probkit/hash.hpp"
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>

namespace probkit::hashing {

namespace {
constexpr int kShift30 = 30;
constexpr int kShift27 = 27;
constexpr int kShift31 = 31;
constexpr std::uint64_t kMul1 = 0xBF58476D1CE4E5B9ULL;
constexpr std::uint64_t kMul2 = 0x94D049BB133111EBULL;
constexpr std::uint64_t kGolden = 0x9E3779B97F4A7C15ULL;

constexpr std::uint64_t kWyP0 = 0xA0761D6478BD642FULL;
constexpr std::uint64_t kWyP1 = 0xE7037ED1A0B428DBULL;
constexpr std::uint64_t kWyP2 = 0x8EBC6AF09C88C6E3ULL;
constexpr std::uint64_t kWyP3 = 0x589965CC75374CC3ULL;
constexpr std::uint64_t kWyP4 = 0x1D8E4E27C47D124FULL;

constexpr std::uint64_t kXxPrime1 = 11400714785074694791ULL;
constexpr std::uint64_t kXxPrime2 = 14029467366897019727ULL;
constexpr std::uint64_t kXxPrime3 = 1609587929392839161ULL;
constexpr std::uint64_t kXxPrime4 = 9650029242287828579ULL;
constexpr std::uint64_t kXxPrime5 = 2870177450012600261ULL;

template <unsigned Rot> inline auto rotl_const(std::uint64_t value) noexcept -> std::uint64_t {
  constexpr unsigned mask = 63U;
  constexpr unsigned r = (Rot & mask);
  return (value << r) | (value >> ((64U - r) & mask));
}

inline auto splitmix64(std::uint64_t value) noexcept -> std::uint64_t {
  value += kGolden;
  value = (value ^ (value >> kShift30)) * kMul1;
  value = (value ^ (value >> kShift27)) * kMul2;
  value ^= (value >> kShift31);
  return value;
}

inline auto load_u64_le(std::string_view s, std::size_t off) noexcept -> std::uint64_t {
  std::uint64_t v = 0;
  const std::size_t n = s.size();
  std::size_t rem = 0;
  if (off < n) {
    rem = n - off;
  }
  std::size_t lim = rem;
  lim = std::min<std::size_t>(lim, 8);
  for (std::size_t i = 0; i < lim; ++i) {
    v |= (std::uint64_t)(unsigned char)s[off + i] << (8 * i);
  }
  return v;
}

inline auto load_u32_le(std::string_view s, std::size_t off) noexcept -> std::uint32_t {
  std::uint32_t v = 0;
  const std::size_t n = s.size();
  std::size_t rem = 0;
  if (off < n) {
    rem = n - off;
  }
  std::size_t lim = rem;
  lim = std::min<std::size_t>(lim, 4);
  for (std::size_t i = 0; i < lim; ++i) {
    v |= (std::uint32_t)(unsigned char)s[off + i] << (8 * i);
  }
  return v;
}

inline auto wymum(std::uint64_t a, std::uint64_t b) noexcept -> std::uint64_t {
  __uint128_t r = static_cast<__uint128_t>(a) * static_cast<__uint128_t>(b);
  return static_cast<std::uint64_t>(r) ^ static_cast<std::uint64_t>(r >> 64);
}

inline auto wyhash_impl(std::string_view s, std::uint64_t seed) noexcept -> std::uint64_t {
  const std::size_t n = s.size();
  const std::uint64_t secret = kWyP0 ^ kWyP1;
  std::uint64_t h = seed ^ (secret + static_cast<std::uint64_t>(n));
  std::size_t i = 0;
  while (i + 16 <= n) {
    const std::uint64_t a = load_u64_le(s, i) ^ kWyP1;
    const std::uint64_t b = load_u64_le(s, i + 8) ^ kWyP2;
    h = wymum(h ^ a, kWyP0) ^ wymum(b, kWyP3);
    i += 16;
  }
  if (i + 8 <= n) {
    const std::uint64_t a = load_u64_le(s, i) ^ kWyP1;
    h = wymum(h ^ a, kWyP4);
    i += 8;
  }
  if (i < n) {
    const std::uint64_t a = load_u32_le(s, i) ^ kWyP2;
    const std::uint64_t b = load_u32_le(s, n - 4) ^ kWyP3;
    h = wymum(h ^ a, kWyP0) ^ b;
  }
  h = wymum(h ^ kWyP1, kWyP4);
  return h;
}

inline auto xxhash64_impl(std::string_view s, std::uint64_t seed) noexcept -> std::uint64_t {
  const std::size_t n = s.size();
  std::size_t i = 0;
  std::uint64_t h = 0;
  if (n >= 32) {
    std::uint64_t v1 = seed + kXxPrime1 + kXxPrime2;
    std::uint64_t v2 = seed + kXxPrime2;
    std::uint64_t v3 = seed + 0;
    std::uint64_t v4 = seed - kXxPrime1;
    const std::size_t limit = n - 32;
    while (i <= limit) {
      v1 = rotl_const<31U>(v1 + (load_u64_le(s, i) * kXxPrime2)) * kXxPrime1;
      v2 = rotl_const<31U>(v2 + (load_u64_le(s, i + 8) * kXxPrime2)) * kXxPrime1;
      v3 = rotl_const<31U>(v3 + (load_u64_le(s, i + 16) * kXxPrime2)) * kXxPrime1;
      v4 = rotl_const<31U>(v4 + (load_u64_le(s, i + 24) * kXxPrime2)) * kXxPrime1;
      i += 32;
    }
    h = rotl_const<1U>(v1) + rotl_const<7U>(v2) + rotl_const<12U>(v3) + rotl_const<18U>(v4);
    v1 = (v1 * kXxPrime2);
    v1 = rotl_const<31U>(v1);
    v1 *= kXxPrime1;
    h ^= v1;
    h = (h * kXxPrime1) + kXxPrime4;
    v2 = (v2 * kXxPrime2);
    v2 = rotl_const<31U>(v2);
    v2 *= kXxPrime1;
    h ^= v2;
    h = (h * kXxPrime1) + kXxPrime4;
    v3 = (v3 * kXxPrime2);
    v3 = rotl_const<31U>(v3);
    v3 *= kXxPrime1;
    h ^= v3;
    h = (h * kXxPrime1) + kXxPrime4;
    v4 = (v4 * kXxPrime2);
    v4 = rotl_const<31U>(v4);
    v4 *= kXxPrime1;
    h ^= v4;
    h = (h * kXxPrime1) + kXxPrime4;
  } else {
    h = seed + kXxPrime5;
  }
  h += n;
  while (i + 8 <= n) {
    const std::uint64_t k = load_u64_le(s, i) * kXxPrime2;
    i += 8;
    h ^= rotl_const<31U>(k) * kXxPrime1;
    h = (rotl_const<27U>(h) * kXxPrime1) + kXxPrime4;
  }
  if (i + 4 <= n) {
    h ^= (std::uint64_t)load_u32_le(s, i) * kXxPrime1;
    i += 4;
    h = (rotl_const<23U>(h) * kXxPrime2) + kXxPrime3;
  }
  while (i < n) {
    h ^= (std::uint64_t)(unsigned char)s[i] * kXxPrime5;
    ++i;
    h = rotl_const<11U>(h) * kXxPrime1;
  }
  h ^= h >> 33;
  h *= kXxPrime2;
  h ^= h >> 29;
  h *= kXxPrime3;
  h ^= h >> 32;
  return h;
}
} // namespace

auto derive_thread_salt(std::uint64_t base, std::uint64_t thread_index) noexcept -> std::uint64_t {
  // Reduce cross-thread collision correlation by spacing seeds
  return splitmix64(base ^ (thread_index * kGolden));
}

auto hash64(std::string_view input, const HashConfig& cfg) noexcept -> std::uint64_t {
  const std::uint64_t seed = cfg.seed ^ cfg.thread_salt;
  switch (cfg.kind) {
  case HashKind::wyhash:
    return wyhash_impl(input, seed);
  case HashKind::xxhash:
    return xxhash64_impl(input, seed);
  }
  return wyhash_impl(input, seed);
}

} // namespace probkit::hashing
