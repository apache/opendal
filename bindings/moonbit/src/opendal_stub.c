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

#define MOONBIT_OPENDAL_MAX_WHOLE_READ_BYTES (64u * 1024u * 1024u)

typedef struct opendal_moonbit_operator opendal_moonbit_operator_t;

typedef struct {
  const uint8_t *data;
  size_t len;
} opendal_moonbit_bytes_t;

typedef struct {
  int32_t status;
  int32_t error_kind;
  opendal_moonbit_bytes_t message;
  opendal_moonbit_operator_t *operator_;
  opendal_moonbit_bytes_t data;
  uint8_t has_data;
} opendal_moonbit_result_t;

extern opendal_moonbit_result_t *
opendal_moonbit_operator_new(const uint8_t *scheme, size_t scheme_len);
extern void opendal_moonbit_operator_close(opendal_moonbit_operator_t *operator_);
extern void opendal_moonbit_operator_free(opendal_moonbit_operator_t *operator_);
extern opendal_moonbit_result_t *opendal_moonbit_operator_read(
    opendal_moonbit_operator_t *operator_, const uint8_t *path,
    size_t path_len, size_t max_read_bytes);
extern opendal_moonbit_result_t *opendal_moonbit_operator_write(
    opendal_moonbit_operator_t *operator_, const uint8_t *path,
    size_t path_len, const uint8_t *data, size_t data_len);
extern void opendal_moonbit_result_free(opendal_moonbit_result_t *result);
extern uint32_t opendal_moonbit_live_operator_count(void);

typedef struct {
  opendal_moonbit_operator_t *inner;
} moonbit_opendal_operator_t;

typedef struct {
  opendal_moonbit_result_t *inner;
} moonbit_opendal_result_t;

static size_t moonbit_bytes_len(moonbit_bytes_t bytes) {
  return (size_t)(uint32_t)Moonbit_array_length(bytes);
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
  moonbit_opendal_operator_t *operator_ =
      (moonbit_opendal_operator_t *)payload;
  if (operator_->inner != NULL) {
    opendal_moonbit_operator_free(operator_->inner);
    operator_->inner = NULL;
  }
}

static moonbit_opendal_operator_t *
operator_external(opendal_moonbit_operator_t *inner) {
  moonbit_opendal_operator_t *operator_ =
      (moonbit_opendal_operator_t *)moonbit_make_external_object(
          operator_finalize, (uint32_t)sizeof(moonbit_opendal_operator_t));
  operator_->inner = inner;
  return operator_;
}

static void result_finalize(void *payload) {
  moonbit_opendal_result_t *result = (moonbit_opendal_result_t *)payload;
  if (result->inner != NULL) {
    opendal_moonbit_result_free(result->inner);
    result->inner = NULL;
  }
}

static moonbit_opendal_result_t *
result_external(opendal_moonbit_result_t *inner) {
  moonbit_opendal_result_t *result =
      (moonbit_opendal_result_t *)moonbit_make_external_object(
          result_finalize, (uint32_t)sizeof(moonbit_opendal_result_t));
  result->inner = inner;
  return result;
}

MOONBIT_FFI_EXPORT moonbit_opendal_result_t *
moonbit_opendal_operator_new(moonbit_bytes_t scheme) {
  return result_external(
      opendal_moonbit_operator_new(scheme, moonbit_bytes_len(scheme)));
}

MOONBIT_FFI_EXPORT void
moonbit_opendal_operator_close(moonbit_opendal_operator_t *operator_) {
  if (operator_ != NULL) {
    opendal_moonbit_operator_close(operator_->inner);
  }
}

static moonbit_opendal_result_t *
operator_read_with_limit(moonbit_opendal_operator_t *operator_,
                         moonbit_bytes_t path, size_t max_read_bytes) {
  opendal_moonbit_operator_t *inner = operator_ == NULL ? NULL : operator_->inner;
  return result_external(opendal_moonbit_operator_read(
      inner, path, moonbit_bytes_len(path), max_read_bytes));
}

MOONBIT_FFI_EXPORT moonbit_opendal_result_t *
moonbit_opendal_operator_read(moonbit_opendal_operator_t *operator_,
                              moonbit_bytes_t path) {
  return operator_read_with_limit(operator_, path,
                                  MOONBIT_OPENDAL_MAX_WHOLE_READ_BYTES);
}

MOONBIT_FFI_EXPORT moonbit_opendal_result_t *
moonbit_opendal_operator_read_with_limit_for_test(
    moonbit_opendal_operator_t *operator_, moonbit_bytes_t path,
    uint32_t max_read_bytes) {
  return operator_read_with_limit(operator_, path, max_read_bytes);
}

MOONBIT_FFI_EXPORT moonbit_opendal_result_t *
moonbit_opendal_operator_write(moonbit_opendal_operator_t *operator_,
                               moonbit_bytes_t path, moonbit_bytes_t data) {
  opendal_moonbit_operator_t *inner = operator_ == NULL ? NULL : operator_->inner;
  return result_external(opendal_moonbit_operator_write(
      inner, path, moonbit_bytes_len(path), data, moonbit_bytes_len(data)));
}

MOONBIT_FFI_EXPORT int32_t
moonbit_opendal_result_status(moonbit_opendal_result_t *result) {
  return result == NULL || result->inner == NULL
             ? 1
             : result->inner->status;
}

MOONBIT_FFI_EXPORT int32_t
moonbit_opendal_result_error_kind(moonbit_opendal_result_t *result) {
  return result == NULL || result->inner == NULL
             ? 0
             : result->inner->error_kind;
}

MOONBIT_FFI_EXPORT moonbit_bytes_t
moonbit_opendal_result_error_message(moonbit_opendal_result_t *result) {
  if (result == NULL || result->inner == NULL) {
    static const uint8_t message[] = "native result is unavailable";
    return copy_bytes(message, sizeof(message) - 1);
  }
  return copy_bytes(result->inner->message.data, result->inner->message.len);
}

MOONBIT_FFI_EXPORT moonbit_opendal_operator_t *
moonbit_opendal_result_take_operator(moonbit_opendal_result_t *result) {
  opendal_moonbit_operator_t *inner =
      result == NULL || result->inner == NULL ? NULL
                                              : result->inner->operator_;
  if (result != NULL && result->inner != NULL) {
    result->inner->operator_ = NULL;
  }
  return operator_external(inner);
}

MOONBIT_FFI_EXPORT moonbit_bytes_t
moonbit_opendal_result_take_bytes(moonbit_opendal_result_t *result) {
  if (result == NULL || result->inner == NULL || !result->inner->has_data) {
    return moonbit_make_bytes(0, 0);
  }
  return copy_bytes(result->inner->data.data, result->inner->data.len);
}

MOONBIT_FFI_EXPORT void
moonbit_opendal_result_release(moonbit_opendal_result_t *result) {
  if (result != NULL && result->inner != NULL) {
    opendal_moonbit_result_free(result->inner);
    result->inner = NULL;
  }
}

MOONBIT_FFI_EXPORT uint32_t moonbit_opendal_live_operator_count(void) {
  return opendal_moonbit_live_operator_count();
}
