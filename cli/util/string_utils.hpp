#pragma once

#include <string_view>

namespace probkit::cli::util {

// C++20 portability: prefer std::string_view::starts_with when available.
// Fallback to compare() to support libstdc++/libc++ variants lacking it.
constexpr auto sv_starts_with(std::string_view s, std::string_view prefix) noexcept -> bool {
#if defined(__cpp_lib_starts_ends_with) && (__cpp_lib_starts_ends_with >= 201711L)
  return s.starts_with(prefix);
#else
  return s.size() >= prefix.size() && s.compare(0, prefix.size(), prefix) == 0;
#endif
}

} // namespace probkit::cli::util