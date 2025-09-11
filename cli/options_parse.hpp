#pragma once

#include "options.hpp"

namespace probkit::cli {

struct ParseResult {
  ExitCode status;
  int next_index;
};

[[nodiscard]] auto parse_global_options(int argc, char** argv, GlobalOptions& g) -> ParseResult;

} // namespace probkit::cli
