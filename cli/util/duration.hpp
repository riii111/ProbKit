#pragma once

#include <array>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <ctime>
#include <limits>
#include <string>
#include <string_view>

namespace probkit::cli::util::timeutil {

[[nodiscard]] inline auto parse_duration(std::string_view s, std::chrono::nanoseconds& out) noexcept -> bool {
  if (s.empty()) {
    return false;
  }
  std::uint64_t value = 0;
  std::size_t i = 0;
  for (; i < s.size(); ++i) {
    const auto ch = static_cast<unsigned char>(s[i]);
    if (ch < '0' || ch > '9') {
      break;
    }
    const auto digit = static_cast<std::uint64_t>(ch - '0');
    const std::uint64_t limit = (std::numeric_limits<std::uint64_t>::max() - digit) / 10ULL;
    if (value > limit) {
      return false;
    }
    value = (value * 10ULL) + digit;
  }
  if (i == 0 || i >= s.size()) {
    return false;
  }
  const std::string_view unit = s.substr(i);
  if (unit == std::string_view{"ms"}) {
    out = std::chrono::milliseconds(value);
    return true;
  }
  if (unit == std::string_view{"s"}) {
    out = std::chrono::seconds(value);
    return true;
  }
  if (unit == std::string_view{"m"}) {
    out = std::chrono::minutes(value);
    return true;
  }
  if (unit == std::string_view{"h"}) {
    out = std::chrono::hours(value);
    return true;
  }
  return false;
}

struct Timebase {
  std::chrono::system_clock::time_point sys0{std::chrono::system_clock::now()};
  std::chrono::steady_clock::time_point steady0{std::chrono::steady_clock::now()};

  [[nodiscard]] auto to_system(std::chrono::steady_clock::time_point t) const -> std::chrono::system_clock::time_point {
    const auto delta = t - steady0;
    return sys0 + std::chrono::duration_cast<std::chrono::system_clock::duration>(delta);
  }
};

[[nodiscard]] inline auto format_utc_iso8601(std::chrono::system_clock::time_point tp) -> std::string {
  const std::time_t t = std::chrono::system_clock::to_time_t(tp);
  std::tm tm_utc{};
#if defined(_WIN32)
  gmtime_s(&tm_utc, &t);
#else
  #if defined(__unix__) || defined(__APPLE__)
    // thread-safe variant on POSIX
    gmtime_r(&t, &tm_utc);
  #else
    // fallback (not thread-safe)
    std::tm* ptm = std::gmtime(&t);
    if (ptm != nullptr) {
      tm_utc = *ptm;
    }
  #endif
#endif
  std::array<char, 32> buf{};
  if (std::snprintf(buf.data(), buf.size(), "%04d-%02d-%02dT%02d:%02d:%02dZ", tm_utc.tm_year + 1900, tm_utc.tm_mon + 1,
                    tm_utc.tm_mday, tm_utc.tm_hour, tm_utc.tm_min, tm_utc.tm_sec) < 0) {
    return std::string{"1970-01-01T00:00:00Z"};
  }
  return std::string{buf.data()};
}

} // namespace probkit::cli::util::timeutil
