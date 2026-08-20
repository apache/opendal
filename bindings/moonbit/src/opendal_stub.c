/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <limits.h>
#include <moonbit.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

typedef struct opendal_moonbit_operator opendal_moonbit_operator_t;

typedef struct {
  uint8_t *data;
  size_t len;
} opendal_moonbit_bytes_t;

typedef struct {
  int32_t kind;
  opendal_moonbit_bytes_t message;
} opendal_moonbit_error_t;

typedef struct {
  opendal_moonbit_error_t error;
  opendal_moonbit_operator_t *operator_;
} opendal_moonbit_new_result_t;

typedef struct {
  opendal_moonbit_error_t error;
  opendal_moonbit_bytes_t data;
} opendal_moonbit_read_result_t;

_Static_assert(offsetof(opendal_moonbit_new_result_t, error) == 0, "error");
_Static_assert(offsetof(opendal_moonbit_read_result_t, error) == 0, "error");

extern opendal_moonbit_new_result_t
opendal_moonbit_operator_new(const uint8_t *scheme, uint32_t scheme_len);
extern opendal_moonbit_read_result_t opendal_moonbit_operator_read(
    opendal_moonbit_operator_t *operator_, const uint8_t *path,
    uint32_t path_len);
extern opendal_moonbit_error_t opendal_moonbit_operator_write(
    opendal_moonbit_operator_t *operator_, const uint8_t *path,
    uint32_t path_len, const uint8_t *data, uint32_t data_len);
extern void opendal_moonbit_operator_close(opendal_moonbit_operator_t *operator_);
extern void opendal_moonbit_operator_free(opendal_moonbit_operator_t *operator_);
extern void opendal_moonbit_bytes_free(opendal_moonbit_bytes_t *bytes);

typedef struct {
  opendal_moonbit_operator_t *inner;
} moonbit_opendal_operator_t;

static uint32_t moonbit_bytes_len(moonbit_bytes_t bytes) {
  return (uint32_t)Moonbit_array_length(bytes);
}

static moonbit_bytes_t copy_bytes(const uint8_t *data, size_t len) {
  if (len > INT32_MAX || (len != 0 && data == NULL)) {
    static const uint8_t fallback[] = "native result is unavailable";
    data = fallback;
    len = sizeof(fallback) - 1;
  }
  moonbit_bytes_t output = moonbit_make_bytes((int32_t)len, 0);
  if (len != 0) {
    memcpy(output, data, len);
  }
  return output;
}

static void operator_finalize(void *payload) {
  moonbit_opendal_operator_t *operator_ = payload;
  if (operator_->inner != NULL) {
    opendal_moonbit_operator_free(operator_->inner);
    operator_->inner = NULL;
  }
}

static moonbit_opendal_operator_t *
operator_external(opendal_moonbit_operator_t *inner) {
  moonbit_opendal_operator_t *operator_ = moonbit_make_external_object(
      operator_finalize, (uint32_t)sizeof(moonbit_opendal_operator_t));
  operator_->inner = inner;
  return operator_;
}

static void new_result_finalize(void *payload) {
  opendal_moonbit_new_result_t *result = payload;
  if (result->operator_ != NULL) {
    opendal_moonbit_operator_free(result->operator_);
  }
  opendal_moonbit_bytes_free(&result->error.message);
}

static void read_result_finalize(void *payload) {
  opendal_moonbit_read_result_t *result = payload;
  opendal_moonbit_bytes_free(&result->data);
  opendal_moonbit_bytes_free(&result->error.message);
}

static void write_result_finalize(void *payload) {
  opendal_moonbit_error_t *error = payload;
  opendal_moonbit_bytes_free(&error->message);
}

MOONBIT_FFI_EXPORT opendal_moonbit_new_result_t *
moonbit_opendal_operator_new(moonbit_bytes_t scheme) {
  opendal_moonbit_new_result_t *result = moonbit_make_external_object(
      new_result_finalize, (uint32_t)sizeof(opendal_moonbit_new_result_t));
  *result = opendal_moonbit_operator_new(scheme, moonbit_bytes_len(scheme));
  return result;
}

MOONBIT_FFI_EXPORT opendal_moonbit_read_result_t *
moonbit_opendal_operator_read(moonbit_opendal_operator_t *operator_,
                              moonbit_bytes_t path) {
  opendal_moonbit_read_result_t *result = moonbit_make_external_object(
      read_result_finalize, (uint32_t)sizeof(opendal_moonbit_read_result_t));
  *result = opendal_moonbit_operator_read(
      operator_ == NULL ? NULL : operator_->inner, path,
      moonbit_bytes_len(path));
  return result;
}

MOONBIT_FFI_EXPORT opendal_moonbit_error_t *
moonbit_opendal_operator_write(moonbit_opendal_operator_t *operator_,
                               moonbit_bytes_t path, moonbit_bytes_t data) {
  opendal_moonbit_error_t *result = moonbit_make_external_object(
      write_result_finalize, (uint32_t)sizeof(opendal_moonbit_error_t));
  *result = opendal_moonbit_operator_write(
      operator_ == NULL ? NULL : operator_->inner, path,
      moonbit_bytes_len(path), data, moonbit_bytes_len(data));
  return result;
}

MOONBIT_FFI_EXPORT void
moonbit_opendal_operator_close(moonbit_opendal_operator_t *operator_) {
  if (operator_ != NULL) {
    opendal_moonbit_operator_close(operator_->inner);
  }
}

MOONBIT_FFI_EXPORT int32_t
moonbit_opendal_result_error_kind(void *result) {
  opendal_moonbit_error_t *error = result;
  return error == NULL ? 0 : error->kind;
}

MOONBIT_FFI_EXPORT moonbit_bytes_t
moonbit_opendal_result_take_error_message(void *result) {
  opendal_moonbit_error_t *error = result;
  if (error == NULL || error->kind == -1) {
    static const uint8_t message[] = "native result is unavailable";
    return copy_bytes(message, sizeof(message) - 1);
  }
  moonbit_bytes_t message =
      copy_bytes(error->message.data, error->message.len);
  opendal_moonbit_bytes_free(&error->message);
  error->kind = -1;
  return message;
}

MOONBIT_FFI_EXPORT moonbit_opendal_operator_t *
moonbit_opendal_new_result_take_operator(
    opendal_moonbit_new_result_t *result) {
  opendal_moonbit_operator_t *inner = result == NULL ? NULL : result->operator_;
  if (result != NULL) {
    result->operator_ = NULL;
  }
  return operator_external(inner);
}

MOONBIT_FFI_EXPORT moonbit_bytes_t
moonbit_opendal_read_result_take_bytes(
    opendal_moonbit_read_result_t *result) {
  if (result == NULL) {
    return moonbit_make_bytes(0, 0);
  }
  moonbit_bytes_t data = copy_bytes(result->data.data, result->data.len);
  opendal_moonbit_bytes_free(&result->data);
  return data;
}
