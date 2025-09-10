#pragma once

#include <cstdint>
#include <cstdlib>
#include <limits>
#include <string>
#include <string_view>

namespace probkit::cli::util {

inline bool parse_u64(std::string_view s, std::uint64_t& out) noexcept {
  if (s.empty())
    return false;
  std::uint64_t value = 0;
  for (char ch_raw : s) {
    const unsigned char ch = static_cast<unsigned char>(ch_raw);
    if (ch < '0' || ch > '9')
      return false;
    const std::uint64_t digit = static_cast<std::uint64_t>(ch - '0');
    if (value > (std::numeric_limits<std::uint64_t>::max() - digit) / 10ULL)
      return false; // overflow
    value = value * 10ULL + digit;
  }
  out = value;
  return true;
}

inline bool parse_double(std::string_view s, double& out) {
  char* end = nullptr;
  std::string tmp{s};
  const double v = std::strtod(tmp.c_str(), &end);
  if (end == tmp.c_str() || *end != '\0')
    return false;
  out = v;
  return true;
}

} // namespace probkit::cli::util
