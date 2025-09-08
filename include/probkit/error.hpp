#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <system_error>

namespace probkit {

enum class errc : std::uint8_t {
  invalid_argument = 1,
  parse_error = 2,
  io_error = 3,
  out_of_memory = 4,
  timeout = 5,
  canceled = 6,
  overflow = 7,
  internal_error = 8,
  not_supported = 9,
};

// Forward declaration for ADL before usage in constructors.
auto make_error_code(errc err) noexcept -> std::error_code;

namespace detail {
class probkit_category_impl final : public std::error_category {
public:
  [[nodiscard]] auto name() const noexcept -> const char* override {
    return "probkit";
  }
  [[nodiscard]] auto message(int code_value) const -> std::string override {
    switch (static_cast<errc>(code_value)) {
    case errc::invalid_argument:
      return "invalid argument";
    case errc::parse_error:
      return "parse error";
    case errc::io_error:
      return "I/O error";
    case errc::out_of_memory:
      return "out of memory";
    case errc::timeout:
      return "timeout";
    case errc::canceled:
      return "canceled";
    case errc::overflow:
      return "overflow";
    case errc::internal_error:
      return "internal error";
    case errc::not_supported:
      return "not supported";
    default:
      return "unknown error";
    }
  }
};

inline auto category_singleton() -> const probkit_category_impl& {
  static const probkit_category_impl cat{};
  return cat;
}
} // namespace detail

inline auto error_category() noexcept -> const std::error_category& {
  return detail::category_singleton();
}

struct error {
  std::error_code code;
  std::string context;

  error() = default;
  /* implicit */ error(std::error_code err_code) noexcept : code(err_code) {}
  /* implicit */ error(errc err) noexcept : code(make_error_code(err)) {}
  error(std::error_code err_code, std::string_view ctx) : code(err_code), context(ctx) {}
  error(errc err, std::string_view ctx) : code(make_error_code(err)), context(ctx) {}

  explicit operator bool() const noexcept {
    return static_cast<bool>(code);
  }
  [[nodiscard]] auto category() const noexcept -> const char* {
    return code.category().name();
  }
  [[nodiscard]] auto message() const -> std::string {
    return context.empty() ? code.message() : (std::string(code.message()) + ": " + context);
  }
};

inline auto make_error(errc err, std::string_view ctx = {}) -> error {
  return error{make_error_code(err), ctx};
}
inline auto make_error(std::error_code err_code, std::string_view ctx = {}) -> error {
  return error{err_code, ctx};
}

} // namespace probkit

// Integrate with <system_error>
namespace std {
template <> struct is_error_code_enum<probkit::errc> : true_type {};
} // namespace std

namespace probkit {
inline auto make_error_code(errc err) noexcept -> std::error_code {
  return {static_cast<int>(err), error_category()};
}
} // namespace probkit
