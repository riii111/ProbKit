#pragma once

#include <atomic>
#include <cstddef>
#include <type_traits>
#include <utility>
#include <vector>

namespace probkit::cli::util {

// Minimal single-producer single-consumer ring buffer.
// T should be movable and reasonably small.
template <typename T> class spsc_ring {
public:
  explicit spsc_ring(std::size_t capacity) : capacity_(capacity), data_(capacity) {}

  static_assert(std::is_move_constructible_v<T>, "spsc_ring requires T to be move-constructible");

  // Copy-push (kept for API compatibility)
  auto push(const T& value) noexcept -> bool {
    const std::size_t head = head_.load(std::memory_order_relaxed);
    const std::size_t next = (head + 1) % capacity_;
    if (next == tail_.load(std::memory_order_acquire)) {
      return false; // full
    }
    data_[head] = value;
    head_.store(next, std::memory_order_release);
    return true;
  }

  // Move-push to avoid extra string allocations/copies on hot path
  auto push(T&& value) noexcept -> bool {
    const std::size_t head = head_.load(std::memory_order_relaxed);
    const std::size_t next = (head + 1) % capacity_;
    if (next == tail_.load(std::memory_order_acquire)) {
      return false; // full
    }
    data_[head] = std::move(value);
    head_.store(next, std::memory_order_release);
    return true;
  }

  // Construct-in-queue only when space is available.
  // Important: Passing std::move(x) here is safe in a retry loop; x is consumed only on success.
  template <class... Args> auto try_emplace(Args&&... args) noexcept -> bool {
    const std::size_t head = head_.load(std::memory_order_relaxed);
    const std::size_t next = (head + 1) % capacity_;
    if (next == tail_.load(std::memory_order_acquire)) {
      return false; // full
    }
    data_[head] = T{std::forward<Args>(args)...};
    head_.store(next, std::memory_order_release);
    return true;
  }

  auto pop(T& out) noexcept -> bool {
    const std::size_t tail = tail_.load(std::memory_order_relaxed);
    if (tail == head_.load(std::memory_order_acquire)) {
      return false; // empty
    }
    out = std::move(data_[tail]);
    tail_.store((tail + 1) % capacity_, std::memory_order_release);
    return true;
  }

  auto empty() const noexcept -> bool {
    return tail_.load(std::memory_order_acquire) == head_.load(std::memory_order_acquire);
  }

  // Optional observability helpers (unused OK; inline so cost=0 when not used)
  auto capacity() const noexcept -> std::size_t {
    return capacity_;
  }
  auto approx_size() const noexcept -> std::size_t {
    const auto h = head_.load(std::memory_order_acquire);
    const auto t = tail_.load(std::memory_order_acquire);
    return (h >= t) ? (h - t) : (capacity_ - t + h);
  }

private:
  std::size_t capacity_;
  std::vector<T> data_;
  std::atomic<std::size_t> head_{0};
  std::atomic<std::size_t> tail_{0};
};

} // namespace probkit::cli::util
