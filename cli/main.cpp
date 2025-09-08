#include <cstdio>
#include <string_view>

auto main(int argc, char** argv) -> int {
  (void)argc;
  (void)argv;
  std::fputs("probkit: CLI skeleton (PR-01)\n", stdout);
  std::fputs("subcommands: hll | bloom | cms\n", stdout);
  return 0;
}
