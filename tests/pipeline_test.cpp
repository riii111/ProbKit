#include <cassert>
#include <string>

#include "probkit/error.hpp"
#include "probkit/expected.hpp"

using probkit::errc;
using probkit::error;
using probkit::expected;
using probkit::result;

static void test_expected_value() {
  constexpr int kTestValue = 42;
  probkit::result<int> result_value{kTestValue};
  assert(result_value.has_value());
  assert(static_cast<bool>(result_value));
  assert(result_value.value() == kTestValue);
}

static void test_expected_error() {
  probkit::result<int> result_error{error{probkit::make_error_code(errc::invalid_argument), "x"}};
  assert(!result_error.has_value());
  assert(!static_cast<bool>(result_error));
  assert(result_error.error().code == probkit::make_error_code(errc::invalid_argument));
}

static void test_expected_void() {
  probkit::result<void> ok_result{}; // success
  assert(ok_result.has_value());

  probkit::result<void> err_result{error{errc::timeout}};
  assert(!err_result.has_value());
  assert(err_result.error().code == probkit::make_error_code(errc::timeout));
}

auto main() -> int {
  test_expected_value();
  test_expected_error();
  test_expected_void();
  return 0;
}
