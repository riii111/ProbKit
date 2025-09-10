#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "probkit/expected.hpp"
#include "probkit/hash.hpp"

namespace probkit::cms {

struct Config {
  double eps = 1e-3;
  double delta = 1e-4;
  std::size_t topk = 0; // optional, used for sizing candidate structure
};

struct Pair {
  std::string key;
  std::uint64_t est{};
};

class sketch {
public:
  sketch() = default;
  sketch(sketch&&) noexcept = default;
  auto operator=(sketch&&) noexcept -> sketch& = default;
  sketch(const sketch&) = delete;
  auto operator=(const sketch&) -> sketch& = delete;

  [[nodiscard]] static auto make_by_eps_delta(double eps, double delta, hashing::HashConfig h = {}) -> result<sketch>;

  [[nodiscard]] auto inc(std::string_view x, std::uint64_t c = 1) noexcept -> result<void>;
  [[nodiscard]] auto estimate(std::string_view x) const noexcept -> result<std::uint64_t>;
  [[nodiscard]] auto topk(std::size_t k) const -> result<std::vector<Pair>>;
  [[nodiscard]] auto merge(const sketch& other) noexcept -> result<void>;

  [[nodiscard]] auto dims() const noexcept -> std::pair<std::size_t, std::size_t> {
    return {depth_, width_};
  }
  [[nodiscard]] auto hash_config() const noexcept -> hashing::HashConfig {
    return hash_cfg_;
  }
  [[nodiscard]] auto same_params(const sketch& other) const noexcept -> bool {
    return depth_ == other.depth_ && width_ == other.width_ && hash_cfg_.kind == other.hash_cfg_.kind &&
           hash_cfg_.seed == other.hash_cfg_.seed && hash_cfg_.thread_salt == other.hash_cfg_.thread_salt;
  }

private:
  // Order parameters to avoid adjacent easily-swappable size_t parameters
  explicit sketch(std::size_t d, hashing::HashConfig cfg, std::size_t w, std::vector<std::uint64_t>&& counters,
                  std::vector<std::string>&& keys, std::vector<std::uint64_t>&& ests) noexcept
      : depth_(d), width_(w), hash_cfg_(cfg), table_(std::move(counters)), cand_keys_(std::move(keys)),
        cand_ests_(std::move(ests)) {}

  std::size_t depth_{};
  std::size_t width_{};
  hashing::HashConfig hash_cfg_{};
  std::vector<std::uint64_t> table_; // size depth_*width_

  // Optional candidate tracking for Top-K (simple reservoir with over-provision factor)
  std::vector<std::string> cand_keys_;
  std::vector<std::uint64_t> cand_ests_;
};

} // namespace probkit::cms
