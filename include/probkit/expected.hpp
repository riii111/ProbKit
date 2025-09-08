#pragma once

#include <new>
#include <type_traits>
#include <utility>

#include "probkit/error.hpp"

namespace probkit {

// Minimal expected-like type for C++20 (no exceptions required).
template <class T, class E> class expected {
public:
  using value_type = T;
  using error_type = E;

  expected(const expected& other) {
    if (other.has_) {
      ::new (&storage_.val) T(other.storage_.val);
      has_ = true;
    } else {
      ::new (&storage_.err) E(other.storage_.err);
      has_ = false;
    }
  }

  expected(expected&& other) noexcept(std::is_nothrow_move_constructible_v<T> &&
                                      std::is_nothrow_move_constructible_v<E>) {
    if (other.has_) {
      ::new (&storage_.val) T(std::move(other.storage_.val));
      has_ = true;
    } else {
      ::new (&storage_.err) E(std::move(other.storage_.err));
      has_ = false;
    }
  }

  auto operator=(const expected& other) -> expected& {
    if (this == &other) {
      return *this;
    }
    this->~expected();
    new (this) expected(other);
    return *this;
  }

  auto operator=(expected&& other) noexcept(std::is_nothrow_move_constructible_v<T> &&
                                            std::is_nothrow_move_constructible_v<E> &&
                                            std::is_nothrow_move_assignable_v<T> &&
                                            std::is_nothrow_move_assignable_v<E>) -> expected& {
    if (this == &other) {
      return *this;
    }
    this->~expected();
    new (this) expected(std::move(other));
    return *this;
  }

  ~expected() {
    if (has_) {
      storage_.val.~T();
    } else {
      storage_.err.~E();
    }
  }

  // Construct value
  expected(const T& v) : has_(true) {
    ::new (&storage_.val) T(v);
  }
  expected(T&& v) : has_(true) {
    ::new (&storage_.val) T(std::move(v));
  }
  template <class... Args> explicit expected(std::in_place_t /*unused*/, Args&&... args) : has_(true) {
    ::new (&storage_.val) T(std::forward<Args>(args)...);
  }

  // Construct error
  expected(const E& e) {
    ::new (&storage_.err) E(e);
  }
  expected(E&& e) {
    ::new (&storage_.err) E(std::move(e));
  }
  template <class... Args> static auto from_error(Args&&... args) -> expected {
    expected r{std::forward<Args>(args)...};
    return r;
  }

  // Observers
  [[nodiscard]] auto has_value() const noexcept -> bool {
    return has_;
  }
  explicit operator bool() const noexcept {
    return has_;
  }

  auto value() & -> T& {
    return storage_.val;
  }
  [[nodiscard]] auto value() const& -> const T& {
    return storage_.val;
  }
  auto value() && -> T&& {
    return std::move(storage_.val);
  }

  auto error() & -> E& {
    return storage_.err;
  }
  [[nodiscard]] auto error() const& -> const E& {
    return storage_.err;
  }
  auto error() && -> E&& {
    return std::move(storage_.err);
  }

private:
  bool has_{false};
  union Storage {
    T val;
    E err;
    Storage() {}
    ~Storage() {}
  } storage_;
};

// void specialization
template <class E> class expected<void, E> {
public:
  using value_type = void;
  using error_type = E;

  expected() : has_(true) {}
  expected(const expected& other) : has_(other.has_) {
    if (!has_) {
      ::new (&storage_.err) E(other.storage_.err);
    }
  }
  expected(expected&& other) noexcept(std::is_nothrow_move_constructible_v<E>) : has_(other.has_) {
    if (!has_) {
      ::new (&storage_.err) E(std::move(other.storage_.err));
    }
  }
  ~expected() {
    if (!has_) {
      storage_.err.~E();
    }
  }

  expected(const E& e) {
    ::new (&storage_.err) E(e);
  }
  expected(E&& e) {
    ::new (&storage_.err) E(std::move(e));
  }
  template <class... Args> static auto from_error(Args&&... args) -> expected {
    expected r{E(std::forward<Args>(args)...)};
    return r;
  }

  [[nodiscard]] auto has_value() const noexcept -> bool {
    return has_;
  }
  explicit operator bool() const noexcept {
    return has_;
  }

  void value() const noexcept {}
  auto error() & -> E& {
    return storage_.err;
  }
  [[nodiscard]] auto error() const& -> const E& {
    return storage_.err;
  }
  auto error() && -> E&& {
    return std::move(storage_.err);
  }

private:
  bool has_{false};
  union Storage {
    E err;
    Storage() {}
    ~Storage() {}
  } storage_;
};

template <class T> using result = expected<T, error>;

} // namespace probkit
