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

} // namespace probkit::hashing
