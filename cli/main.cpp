#include "probkit/hash.hpp"
#include <cstdio>
#include <cstring>
#include <string_view>

using probkit::hashing::parse_hash_kind;
namespace probkit::cli {
auto cmd_bloom(int, char**, const probkit::hashing::HashConfig&) -> int; // legacy wrapper
} // namespace probkit::cli

auto main(int argc, char** argv) -> int {
  using probkit::hashing::HashConfig;
  using probkit::hashing::HashKind;

  HashConfig hash_cfg{};
  constexpr const char* kHashEq = "--hash=";
  const std::size_t kHashEqLen = std::strlen(kHashEq);
  // NOLINTBEGIN(cppcoreguidelines-pro-bounds-pointer-arithmetic)
  for (int i = 1; i < argc; ++i) {
    std::string_view arg{argv[i]};
    if (arg.size() >= kHashEqLen && arg.compare(0, kHashEqLen, kHashEq) == 0) {
      const std::size_t val_len = arg.size() - kHashEqLen;
      if (val_len == 0) {
        std::fputs("error: empty --hash value\n", stderr);
        return 2;
      }
      auto algo = arg;
      algo.remove_prefix(kHashEqLen);
      HashKind k{};
      if (!parse_hash_kind(algo, k)) {
        std::fputs("error: unknown --hash value\n", stderr);
        return 2;
      }
      hash_cfg.kind = k;
    } else if (arg == std::string_view{"--hash"} && i + 1 < argc) {
      ++i;
      std::string_view algo{argv[i]};
      HashKind k{};
      if (!parse_hash_kind(algo, k)) {
        std::fputs("error: unknown --hash value\n", stderr);
        return 2;
      }
      hash_cfg.kind = k;
    } else if (arg == std::string_view{"--hash"} && i + 1 >= argc) {
      std::fputs("error: --hash requires a value\n", stderr);
      return 2;
    }
  }
  if (argc >= 2) {
    std::string_view cmd{argv[1]};
    if (cmd == std::string_view{"bloom"}) {
      return probkit::cli::cmd_bloom(argc - 2, argv + 2, hash_cfg);
    }
  }
  // NOLINTEND(cppcoreguidelines-pro-bounds-pointer-arithmetic)
  std::fputs("probkit: CLI skeleton\n", stdout);
  std::fputs("subcommands: hll | bloom | cms\n", stdout);
  return 0;
}
