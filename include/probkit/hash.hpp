#pragma once

#include <cstdint>
#include <string_view>

namespace probkit::hashing {

enum class HashKind : std::uint8_t { wyhash, xxhash };

struct HashConfig {
  HashKind kind{HashKind::wyhash};
  std::uint64_t seed{0};
  std::uint64_t thread_salt{0};
};

[[nodiscard]] auto hash64(std::string_view input, const HashConfig& cfg) noexcept -> std::uint64_t;
[[nodiscard]] auto derive_thread_salt(std::uint64_t base, std::uint64_t thread_index) noexcept -> std::uint64_t;

// ------------------------------------------------------------------
// Helpers for converting HashKind <-> string (shared by CLI/tests)
// ------------------------------------------------------------------
[[nodiscard]] constexpr auto to_string(HashKind k) noexcept -> std::string_view {
  switch (k) {
  case HashKind::wyhash:
    return std::string_view{"wyhash"};
  case HashKind::xxhash:
    return std::string_view{"xxhash"};
  }
  return std::string_view{"wyhash"};
}

// Accepts: "wyhash", "xxhash", and the common shorthand "xxh"
constexpr auto parse_hash_kind(std::string_view s, HashKind& out) noexcept -> bool {
  if (s == std::string_view{"wyhash"}) {
    out = HashKind::wyhash;
    return true;
  }
  if (s == std::string_view{"xxhash"} || s == std::string_view{"xxh"}) {
    out = HashKind::xxhash;
    return true;
  }
  return false;
}

} // namespace probkit::hashing
