#pragma once

#include <cstddef>
#include <cstdint>
#include <string_view>
#include <vector>

#include "probkit/expected.hpp"
#include "probkit/hash.hpp"

namespace probkit::hll {

struct Config {
  std::uint8_t precision = 14; // m = 1 << p
};

class sketch {
public:
  sketch() = default;
  sketch(sketch&&) noexcept = default;
  auto operator=(sketch&&) noexcept -> sketch& = default;
  sketch(const sketch&) = delete;
  auto operator=(const sketch&) -> sketch& = delete;

  [[nodiscard]] static auto make_by_precision(std::uint8_t p, hashing::HashConfig h = {}) -> result<sketch>;

  [[nodiscard]] auto add(std::string_view x) noexcept -> result<void>;
  [[nodiscard]] auto estimate() const noexcept -> result<double>;
  [[nodiscard]] auto merge(const sketch& other) noexcept -> result<void>;

  [[nodiscard]] auto precision() const noexcept -> std::uint8_t {
    return p_;
  }
  [[nodiscard]] auto m() const noexcept -> std::size_t {
    return static_cast<std::size_t>(1ULL << p_);
  }
  [[nodiscard]] auto hash_config() const noexcept -> hashing::HashConfig {
    return hash_cfg_;
  }
  [[nodiscard]] auto same_params(const sketch& other) const noexcept -> bool {
    return p_ == other.p_ && hash_cfg_.kind == other.hash_cfg_.kind && hash_cfg_.seed == other.hash_cfg_.seed &&
           hash_cfg_.thread_salt == other.hash_cfg_.thread_salt;
  }

private:
  explicit sketch(std::uint8_t p, hashing::HashConfig cfg, std::vector<std::uint8_t>&& regs) noexcept
      : p_(p), hash_cfg_(cfg), registers_(std::move(regs)) {}

  std::uint8_t p_{14};
  hashing::HashConfig hash_cfg_{};
  std::vector<std::uint8_t> registers_; // rank values per register
};

} // namespace probkit::hll
