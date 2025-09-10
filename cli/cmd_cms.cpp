#include "options.hpp"
#include "probkit/cms.hpp"
#include "util/string_utils.hpp"
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <string_view>

using probkit::cli::util::sv_starts_with;

namespace probkit::cli {

namespace {

struct CmsOptions {
  bool show_help{false};
  bool have_eps{false};
  bool have_delta{false};
  double eps{1e-3};
  double delta{1e-4};
  std::size_t topk{0};
};

inline void print_help() {
  std::fputs("usage: probkit cms [--eps=<e>] [--delta=<d>] [--topk=<k>]\n", stdout);
}

[[nodiscard]] inline auto parse_u64(std::string_view s, std::uint64_t& out) -> bool {
  char* end = nullptr;
  std::string tmp{s};
  const unsigned long long v = std::strtoull(tmp.c_str(), &end, 10);
  if (end == tmp.c_str() || *end != '\0') {
    return false;
  }
  out = static_cast<std::uint64_t>(v);
  return true;
}

[[nodiscard]] inline auto parse_double(std::string_view s, double& out) -> bool {
  char* end = nullptr;
  std::string tmp{s};
  const double v = std::strtod(tmp.c_str(), &end);
  if (end == tmp.c_str() || *end != '\0') {
    return false;
  }
  out = v;
  return true;
}

auto parse_cms_opts(int argc, char** argv) -> CmsOptions {
  CmsOptions o{};
  // NOLINTBEGIN(cppcoreguidelines-pro-bounds-pointer-arithmetic)
  for (int i = 0; i < argc; ++i) {
    std::string_view a{argv[i]};
    if (a == std::string_view{"--help"}) {
      o.show_help = true;
      break;
    }
    if (sv_starts_with(a, std::string_view{"--eps="})) {
      double v = 0.0;
      if (!parse_double(a.substr(std::string_view{"--eps="}.size()), v) || v <= 0.0) {
        std::fputs("error: invalid --eps\n", stderr);
        o.show_help = true;
        break;
      }
      o.have_eps = true;
      o.eps = v;
    } else if (sv_starts_with(a, std::string_view{"--delta="})) {
      double v = 0.0;
      if (!parse_double(a.substr(std::string_view{"--delta="}.size()), v) || v <= 0.0 || v >= 1.0) {
        std::fputs("error: invalid --delta\n", stderr);
        o.show_help = true;
        break;
      }
      o.have_delta = true;
      o.delta = v;
    } else if (sv_starts_with(a, std::string_view{"--topk="})) {
      std::uint64_t v = 0;
      if (!parse_u64(a.substr(std::string_view{"--topk="}.size()), v)) {
        std::fputs("error: invalid --topk\n", stderr);
        o.show_help = true;
        break;
      }
      o.topk = static_cast<std::size_t>(v);
    }
  }
  // NOLINTEND(cppcoreguidelines-pro-bounds-pointer-arithmetic)
  return o;
}

} // namespace

auto cmd_cms(int argc, char** argv, const GlobalOptions& g) -> int {
  const CmsOptions co = parse_cms_opts(argc, argv);
  if (co.show_help) {
    print_help();
    return 0;
  }
  auto s =
      probkit::cms::sketch::make_by_eps_delta(co.have_eps ? co.eps : 1e-3, co.have_delta ? co.delta : 1e-4, g.hash);
  if (!s) {
    std::fputs("error: failed to init cms\n", stderr);
    return 5;
  }
  if (g.json) {
    std::fputs("{\"cms\":\"ok\"}\n", stdout);
  } else {
    std::fputs("cms: initialized\n", stdout);
  }
  return 0;
}

} // namespace probkit::cli
