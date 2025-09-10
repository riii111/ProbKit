#pragma once

#include <atomic>
#include <cstddef>
#include <utility>
#include <vector>

namespace probkit::cli::util {

// Minimal single-producer single-consumer ring buffer.
// T should be movable and reasonably small.
template <typename T> class spsc_ring {
public:
  explicit spsc_ring(std::size_t capacity) : capacity_(capacity), data_(capacity) {}

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

private:
  std::size_t capacity_;
  std::vector<T> data_;
  std::atomic<std::size_t> head_{0};
  std::atomic<std::size_t> tail_{0};
};

} // namespace probkit::cli::util
