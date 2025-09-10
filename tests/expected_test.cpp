#include <cassert>
#include <string>

#include "probkit/error.hpp"
#include "probkit/expected.hpp"

using probkit::errc;
using probkit::error;
using probkit::result;

namespace tests {
void run_hash_tests();
void run_bloom_tests();
void run_hll_tests();
void run_cms_tests();
} // namespace tests

static void copy_move_value() {
  result<std::string> a{std::string("ok")};
  auto b = a;            // copy
  auto c = std::move(a); // move
  assert(b.has_value());
  assert(c.has_value());
  assert(b.value() == "ok");
  assert(c.value() == "ok");
}

static void copy_move_error() {
  error e = probkit::make_error(errc::invalid_argument, "x");
  result<std::string> a{e};
  auto b = a;            // copy
  auto c = std::move(a); // move
  assert(!b.has_value());
  assert(!c.has_value());
  assert(b.error() == e);
  assert(c.error() == e);
}

static void error_context() {
  error e = probkit::make_error(errc::timeout, "phase1");
  e.append_context("phase2");
  assert(e.message().find("phase1") != std::string::npos);
  assert(e.message().find("phase2") != std::string::npos);
}

auto main() -> int {
  try {
    copy_move_value();
    copy_move_error();
    error_context();
    tests::run_hash_tests();
    tests::run_bloom_tests();
    tests::run_hll_tests();
    tests::run_cms_tests();
    return 0;
  } catch (const std::exception& e) {
    std::fprintf(stderr, "unexpected exception: %s\n", e.what());
    return 2;
  } catch (...) {
    std::fprintf(stderr, "unexpected non-std exception\n");
    return 2;
  }
}
