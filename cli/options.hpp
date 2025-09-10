#pragma once

#include <cstdint>
#include <string>

#include "probkit/hash.hpp"

namespace probkit::cli {

enum class OptionResult : std::uint8_t {
  HelpShown = 0,       // ヘルプ表示済み
  Handled = 1,         // 正常処理完了
  NotHandled = 2,      // このハンドラーでは処理できない
  SubcommandStart = 3, // サブコマンド開始
  Error = 255          // エラー（-1の代わりに255を使用）
};

enum class CommandResult : std::uint8_t {
  Success = 0,      // 正常終了
  GeneralError = 2, // 一般的なエラー
  IOError = 3,      // I/Oエラー
  ConfigError = 5   // 設定エラー
};

enum class ExitCode : std::uint8_t {
  Success = 0,      // 正常終了
  GeneralError = 1, // 一般的なエラー
  ArgumentError = 2 // 引数エラー
};

constexpr auto to_int(OptionResult r) -> int {
  return static_cast<int>(r);
}
constexpr auto to_int(CommandResult r) -> int {
  return static_cast<int>(r);
}
constexpr auto to_int(ExitCode r) -> int {
  return static_cast<int>(r);
}

struct GlobalOptions {
  int threads{0};        // 0 => use hardware_concurrency
  std::string file_path; // empty => stdin
  bool json{false};
  std::uint64_t stop_after{0}; // lines; 0 => unlimited
  probkit::hashing::HashConfig hash{};
  // Observability / rotation flags (parsed; functionality may be enabled in later PRs)
  bool stats{false};
  unsigned stats_interval_seconds{5}; // default interval when --stats is present without value
  std::string bucket;                 // e.g., "30s", "1m"; empty => no rotation
  bool prom{false};
  std::string prom_path; // empty => stdout
  // Memory upper bound hint (global). Subcommands may override their own sizing.
  std::uint64_t mem_budget_bytes{0};
};

auto cmd_bloom(int argc, char** argv, const GlobalOptions& g) -> CommandResult;
auto cmd_hll(int argc, char** argv, const GlobalOptions& g) -> CommandResult;
auto cmd_cms(int argc, char** argv, const GlobalOptions& g) -> CommandResult;

} // namespace probkit::cli
