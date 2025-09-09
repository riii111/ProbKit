#pragma once

#include <cstddef>
#include <cstdint>
#include <string_view>
#include <vector>

#include "probkit/expected.hpp"
#include "probkit/hash.hpp"

namespace probkit::bloom {

struct Config {
  double fp = 0.01;
  std::size_t mem_budget_bytes{};
};

class filter {
public:
  filter() = default;
  filter(filter&&) noexcept = default;
  auto operator=(filter&&) noexcept -> filter& = default;
  filter(const filter&) = delete;
  auto operator=(const filter&) -> filter& = delete;

  [[nodiscard]] static auto make_by_fp(double p, hashing::HashConfig h = {}) -> result<filter>;
  [[nodiscard]] static auto make_by_fp(double p, std::size_t capacity_hint, hashing::HashConfig h) -> result<filter>;
  [[nodiscard]] static auto make_by_mem(std::size_t bytes, hashing::HashConfig h = {}) -> result<filter>;

  // The view is not retained beyond the call
  [[nodiscard]] auto add(std::string_view x) noexcept -> result<void>;
  // May return false positives; must not return false negatives if constructed successfully
  [[nodiscard]] auto might_contain(std::string_view x) const noexcept -> result<bool>;
  // Requires identical parameterization (size/k/hash config)
  [[nodiscard]] auto merge(const filter& other) noexcept -> result<void>;

  // Accessors for tests/observability
  [[nodiscard]] auto bit_size() const noexcept -> std::size_t {
    return m_bits_;
  }
  [[nodiscard]] auto k() const noexcept -> std::uint8_t {
    return k_;
  }

private:
  static constexpr std::uint8_t kDefaultK = 7;
  static constexpr std::uint64_t kSalt2 = 0x9E3779B97F4A7C15ULL;

  struct layout {
    std::size_t bit_count{};
    std::uint8_t k{};
  };

  filter(std::vector<std::uint64_t>&& words, layout s, hashing::HashConfig cfg) noexcept
      : bits_(std::move(words)), m_bits_(s.bit_count), k_(s.k), hash_cfg_(cfg) {}

  static auto index_word(std::size_t bit) noexcept -> std::size_t {
    return bit >> 6;
  }
  static auto index_mask(std::size_t bit) noexcept -> std::uint64_t {
    return 1ULL << (bit & 63U);
  }
  [[nodiscard]] auto mod_bit(std::uint64_t v) const noexcept -> std::size_t {
    return static_cast<std::size_t>(v % static_cast<std::uint64_t>(m_bits_));
  }

  [[nodiscard]] auto second_seed() const noexcept -> std::uint64_t {
    return hash_cfg_.seed ^ kSalt2;
  }

  std::vector<std::uint64_t> bits_;
  std::size_t m_bits_{};
  std::uint8_t k_{};
  hashing::HashConfig hash_cfg_{};
};

} // namespace probkit::bloom
