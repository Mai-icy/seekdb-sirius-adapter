/*
 * Copyright (c) 2026 OceanBase.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <sirius/ffi.hpp>
#include <cstdio>
#include <exception>
#include <memory>
#include <stdexcept>
#include <string>

// Python/C++ boundary owned by seekdb. The engine interface is Sirius's
// unmodified public C++ FFI. A compute process owns exactly one Context and
// releases each Arrow stream/result before the next query, and destroys the
// Context on that same thread when the process retires.
namespace {
void error_text(char *output, size_t capacity, const char *message) noexcept
{
  if (output && capacity) std::snprintf(output, capacity, "%s", message);
}
}

extern "C" void *compute_open(const char *config, char *error, size_t capacity) noexcept
{
  try {
    if (!config || !*config) throw std::invalid_argument("An explicit Sirius configuration is required");
    return sirius::ffi::make_context_from_config(config).release();
  } catch (const std::exception &e) {
    error_text(error, capacity, e.what());
  } catch (...) {
    error_text(error, capacity, "Unknown Sirius initialization failure");
  }
  return nullptr;
}

extern "C" int compute_execute(void *context, const char *plan, size_t size,
                                void *arrow_stream, char *error, size_t capacity) noexcept
{
  try {
    if (!context || !plan || !size || size > 16 * 1024 * 1024 || !arrow_stream)
      throw std::invalid_argument("Invalid compute request");
    static_cast<sirius::ffi::Context *>(context)->execute_substrait(
        std::string(plan, size), reinterpret_cast<std::uintptr_t>(arrow_stream));
    return 0;
  } catch (const std::exception &e) {
    error_text(error, capacity, e.what());
  } catch (...) {
    error_text(error, capacity, "Unknown Sirius execution failure");
  }
  // execute_substrait's caller owns any initialized stream, even on failure.
  return -1;
}

extern "C" void compute_close(void *context) noexcept
{
  delete static_cast<sirius::ffi::Context *>(context);
}
