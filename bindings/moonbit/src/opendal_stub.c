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

#include "../../c/include/opendal.h"
#include <limits.h>
#include <moonbit.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

enum {
  MOONBIT_OPENDAL_OK = 0,
  MOONBIT_OPENDAL_ERROR = 1,
  MOONBIT_OPENDAL_RESOURCE_CLOSED = 0x1001,
  MOONBIT_OPENDAL_BUFFER_TOO_LARGE = 0x1002,
  MOONBIT_OPENDAL_INVALID_ARGUMENT = 0x1003,
};

#define MOONBIT_OPENDAL_MAX_WHOLE_READ_BYTES (64u * 1024u * 1024u)

typedef struct {
  atomic_flag gate;
  opendal_operator *inner;
  bool counted;
} moonbit_opendal_operator_t;

typedef struct {
  int32_t status;
  int32_t local_kind;
  const char *local_message;
  opendal_operator *operator_;
  opendal_error *error;
  uint8_t *data;
  size_t data_len;
  size_t data_capacity;
  bool released;
} moonbit_opendal_result_t;

static _Atomic uint32_t live_operator_count = 0;

static void operator_lock(moonbit_opendal_operator_t *operator_) {
  while (atomic_flag_test_and_set_explicit(&operator_->gate,
                                           memory_order_acquire)) {
  }
}

static void operator_unlock(moonbit_opendal_operator_t *operator_) {
  atomic_flag_clear_explicit(&operator_->gate, memory_order_release);
}

static void operator_release(moonbit_opendal_operator_t *operator_) {
  if (operator_ == NULL) {
    return;
  }
  operator_lock(operator_);
  opendal_operator *inner = operator_->inner;
  operator_->inner = NULL;
  bool counted = operator_->counted;
  operator_->counted = false;
  operator_unlock(operator_);
  if (inner != NULL) {
    opendal_operator_free(inner);
  }
  if (counted) {
    atomic_fetch_sub_explicit(&live_operator_count, 1, memory_order_relaxed);
  }
}

static void operator_finalize(void *payload) {
  operator_release((moonbit_opendal_operator_t *)payload);
}

static moonbit_opendal_operator_t *operator_new_external(void) {
  moonbit_opendal_operator_t *operator_ =
      (moonbit_opendal_operator_t *)moonbit_make_external_object(
          operator_finalize, (uint32_t)sizeof(moonbit_opendal_operator_t));
  operator_->gate = (atomic_flag)ATOMIC_FLAG_INIT;
  operator_->inner = NULL;
  operator_->counted = false;
  return operator_;
}

static void result_release_payload(moonbit_opendal_result_t *result) {
  if (result == NULL || result->released) {
    return;
  }
  if (result->operator_ != NULL) {
    opendal_operator_free(result->operator_);
    result->operator_ = NULL;
  }
  if (result->error != NULL) {
    opendal_error_free(result->error);
    result->error = NULL;
  }
  free(result->data);
  result->data = NULL;
  result->data_len = 0;
  result->data_capacity = 0;
  result->released = true;
}

static void result_finalize(void *payload) {
  result_release_payload((moonbit_opendal_result_t *)payload);
}

static moonbit_opendal_result_t *result_new(void) {
  moonbit_opendal_result_t *result =
      (moonbit_opendal_result_t *)moonbit_make_external_object(
          result_finalize, (uint32_t)sizeof(moonbit_opendal_result_t));
  memset(result, 0, sizeof(*result));
  result->status = MOONBIT_OPENDAL_ERROR;
  result->local_kind = -1;
  return result;
}

static void result_set_local_error(moonbit_opendal_result_t *result,
                                   int32_t kind, const char *message) {
  result->status = MOONBIT_OPENDAL_ERROR;
  result->local_kind = kind;
  result->local_message = message;
}

static char *copy_c_text(moonbit_opendal_result_t *result, moonbit_bytes_t text,
                         const char *embedded_nul_message) {
  int32_t len;
  if (text == NULL || (len = Moonbit_array_length(text)) < 0) {
    result_set_local_error(result, MOONBIT_OPENDAL_INVALID_ARGUMENT,
                           "text input is invalid");
    return NULL;
  }
  if (memchr(text, 0, (size_t)len) != NULL) {
    result_set_local_error(result, MOONBIT_OPENDAL_INVALID_ARGUMENT,
                           embedded_nul_message);
    return NULL;
  }
  char *copy = (char *)malloc((size_t)len + 1);
  if (copy == NULL) {
    result_set_local_error(result, OPENDAL_UNEXPECTED,
                           "unable to allocate a text input");
    return NULL;
  }
  memcpy(copy, text, (size_t)len);
  copy[len] = '\0';
  return copy;
}

static moonbit_bytes_t copy_bytes(const uint8_t *data, size_t len) {
  if (len > INT32_MAX || (len != 0 && data == NULL)) {
    static const char message[] = "native result cannot fit in MoonBit Bytes";
    data = (const uint8_t *)message;
    len = sizeof(message) - 1;
  }
  moonbit_bytes_t output = moonbit_make_bytes((int32_t)len, 0);
  if (len != 0) {
    memcpy(output, data, len);
  }
  return output;
}

static bool append_read_data(moonbit_opendal_result_t *result,
                             const uint8_t *chunk, size_t chunk_len,
                             size_t max_read_bytes) {
  if (result->data_len > max_read_bytes ||
      chunk_len > max_read_bytes - result->data_len) {
    result_set_local_error(result, MOONBIT_OPENDAL_BUFFER_TOO_LARGE,
                           "read result exceeds the whole-object read limit");
    return false;
  }
  size_t required = result->data_len + chunk_len;
  if (required > result->data_capacity) {
    size_t initial_capacity =
        max_read_bytes < 64 * 1024 ? max_read_bytes : 64 * 1024;
    size_t capacity =
        result->data_capacity == 0 ? initial_capacity : result->data_capacity;
    while (capacity < required) {
      capacity = capacity > max_read_bytes / 2 ? max_read_bytes : capacity * 2;
    }
    uint8_t *resized = (uint8_t *)realloc(result->data, capacity);
    if (resized == NULL) {
      result_set_local_error(result, OPENDAL_UNEXPECTED,
                             "unable to allocate the read result");
      return false;
    }
    result->data = resized;
    result->data_capacity = capacity;
  }
  memcpy(result->data + result->data_len, chunk, chunk_len);
  result->data_len = required;
  return true;
}

MOONBIT_FFI_EXPORT moonbit_opendal_result_t *
moonbit_opendal_operator_new(moonbit_bytes_t scheme) {
  moonbit_opendal_result_t *result = result_new();
  char *scheme_text = copy_c_text(
      result, scheme, "service scheme contains an embedded NUL byte");
  if (scheme_text == NULL) {
    return result;
  }
  opendal_result_operator_new created = opendal_operator_new(scheme_text, NULL);
  free(scheme_text);
  result->operator_ = created.op;
  result->error = created.error;
  if (created.op != NULL && created.error == NULL) {
    result->status = MOONBIT_OPENDAL_OK;
  } else if (created.op == NULL && created.error != NULL) {
    result->status = MOONBIT_OPENDAL_ERROR;
  } else {
    result_release_payload(result);
    result->released = false;
    result_set_local_error(result, OPENDAL_UNEXPECTED,
                           "OpenDAL returned an invalid constructor result");
  }
  return result;
}

MOONBIT_FFI_EXPORT void
moonbit_opendal_operator_close(moonbit_opendal_operator_t *operator_) {
  operator_release(operator_);
}

static moonbit_opendal_result_t *
operator_read_with_limit(moonbit_opendal_operator_t *operator_,
                         moonbit_bytes_t path, size_t max_read_bytes) {
  static const size_t chunk_capacity = 64 * 1024;
  moonbit_opendal_result_t *result = result_new();
  opendal_reader *reader = NULL;
  opendal_metadata *metadata = NULL;
  uint8_t *chunk = NULL;
  char *path_text = NULL;

  if (max_read_bytes > INT32_MAX) {
    max_read_bytes = INT32_MAX;
  }
  if (operator_ == NULL) {
    result_set_local_error(result, MOONBIT_OPENDAL_RESOURCE_CLOSED,
                           "operator is closed");
    return result;
  }
  operator_lock(operator_);
  if (operator_->inner == NULL) {
    result_set_local_error(result, MOONBIT_OPENDAL_RESOURCE_CLOSED,
                           "operator is closed");
    goto cleanup;
  }
  path_text = copy_c_text(result, path, "path contains an embedded NUL byte");
  if (path_text == NULL) {
    goto cleanup;
  }

  opendal_result_stat stat_result =
      opendal_operator_stat(operator_->inner, path_text);
  metadata = stat_result.meta;
  if (stat_result.error != NULL) {
    result->error = stat_result.error;
    goto cleanup;
  }
  if (metadata == NULL) {
    result_set_local_error(result, OPENDAL_UNEXPECTED,
                           "OpenDAL returned no metadata for the read");
    goto cleanup;
  }
  uint64_t content_length = opendal_metadata_content_length(metadata);
  if (content_length > max_read_bytes) {
    result_set_local_error(result, MOONBIT_OPENDAL_BUFFER_TOO_LARGE,
                           "read result exceeds the whole-object read limit");
    goto cleanup;
  }
  if (content_length != 0) {
    result->data = (uint8_t *)malloc((size_t)content_length);
    if (result->data == NULL) {
      result_set_local_error(result, OPENDAL_UNEXPECTED,
                             "unable to allocate the read result");
      goto cleanup;
    }
    result->data_capacity = (size_t)content_length;
  }
  opendal_metadata_free(metadata);
  metadata = NULL;

  opendal_result_operator_reader opened =
      opendal_operator_reader(operator_->inner, path_text);
  reader = opened.reader;
  if (opened.error != NULL) {
    result->error = opened.error;
    goto cleanup;
  }
  if (reader == NULL) {
    result_set_local_error(result, OPENDAL_UNEXPECTED,
                           "OpenDAL returned no reader");
    goto cleanup;
  }
  chunk = (uint8_t *)malloc(chunk_capacity);
  if (chunk == NULL) {
    result_set_local_error(result, OPENDAL_UNEXPECTED,
                           "unable to allocate a read buffer");
    goto cleanup;
  }

  for (;;) {
    opendal_result_reader_read read_result =
        opendal_reader_read(reader, chunk, chunk_capacity);
    if (read_result.error != NULL) {
      result->error = read_result.error;
      goto cleanup;
    }
    if (read_result.size == 0) {
      result->status = MOONBIT_OPENDAL_OK;
      break;
    }
    if (read_result.size > chunk_capacity) {
      result_set_local_error(result, OPENDAL_UNEXPECTED,
                             "OpenDAL returned an invalid read size");
      goto cleanup;
    }
    if (!append_read_data(result, chunk, read_result.size, max_read_bytes)) {
      goto cleanup;
    }
  }

cleanup:
  free(path_text);
  free(chunk);
  if (reader != NULL) {
    opendal_reader_free(reader);
  }
  if (metadata != NULL) {
    opendal_metadata_free(metadata);
  }
  operator_unlock(operator_);
  return result;
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
  moonbit_opendal_result_t *result = result_new();
  int32_t data_len;
  char *path_text = NULL;
  if (operator_ == NULL) {
    result_set_local_error(result, MOONBIT_OPENDAL_RESOURCE_CLOSED,
                           "operator is closed");
    return result;
  }
  operator_lock(operator_);
  if (operator_->inner == NULL) {
    result_set_local_error(result, MOONBIT_OPENDAL_RESOURCE_CLOSED,
                           "operator is closed");
    goto cleanup;
  }
  if (data == NULL || (data_len = Moonbit_array_length(data)) < 0) {
    result_set_local_error(result, MOONBIT_OPENDAL_INVALID_ARGUMENT,
                           "data is invalid");
    goto cleanup;
  }
  path_text = copy_c_text(result, path, "path contains an embedded NUL byte");
  if (path_text == NULL) {
    goto cleanup;
  }
  opendal_bytes input = {
      .data = data_len == 0 ? NULL : data,
      .len = (uintptr_t)(uint32_t)data_len,
      .capacity = 0,
  };
  result->error = opendal_operator_write(operator_->inner, path_text, &input);
  result->status =
      result->error == NULL ? MOONBIT_OPENDAL_OK : MOONBIT_OPENDAL_ERROR;
cleanup:
  free(path_text);
  operator_unlock(operator_);
  return result;
}

MOONBIT_FFI_EXPORT int32_t
moonbit_opendal_result_status(moonbit_opendal_result_t *result) {
  return result == NULL || result->released ? MOONBIT_OPENDAL_ERROR
                                            : result->status;
}

MOONBIT_FFI_EXPORT int32_t
moonbit_opendal_result_error_kind(moonbit_opendal_result_t *result) {
  if (result == NULL || result->released) {
    return OPENDAL_UNEXPECTED;
  }
  if (result->local_kind >= 0) {
    return result->local_kind;
  }
  return result->error == NULL ? OPENDAL_UNEXPECTED : result->error->code;
}

MOONBIT_FFI_EXPORT moonbit_bytes_t
moonbit_opendal_result_error_message(moonbit_opendal_result_t *result) {
  if (result == NULL || result->released) {
    static const char message[] = "native result is unavailable";
    return copy_bytes((const uint8_t *)message, sizeof(message) - 1);
  }
  if (result->local_message != NULL) {
    return copy_bytes((const uint8_t *)result->local_message,
                      strlen(result->local_message));
  }
  if (result->error != NULL) {
    return copy_bytes(result->error->message.data, result->error->message.len);
  }
  static const char message[] = "OpenDAL returned an unspecified error";
  return copy_bytes((const uint8_t *)message, sizeof(message) - 1);
}

MOONBIT_FFI_EXPORT moonbit_opendal_operator_t *
moonbit_opendal_result_take_operator(moonbit_opendal_result_t *result) {
  moonbit_opendal_operator_t *operator_ = operator_new_external();
  if (result == NULL || result->released ||
      result->status != MOONBIT_OPENDAL_OK || result->operator_ == NULL) {
    if (result != NULL && !result->released) {
      result_set_local_error(result, OPENDAL_UNEXPECTED,
                             "native result has no operator");
    }
    return operator_;
  }
  operator_->inner = result->operator_;
  result->operator_ = NULL;
  operator_->counted = true;
  atomic_fetch_add_explicit(&live_operator_count, 1, memory_order_relaxed);
  return operator_;
}

MOONBIT_FFI_EXPORT moonbit_bytes_t
moonbit_opendal_result_take_bytes(moonbit_opendal_result_t *result) {
  if (result == NULL || result->released ||
      result->status != MOONBIT_OPENDAL_OK || result->data_len > INT32_MAX) {
    if (result != NULL && !result->released) {
      result_set_local_error(result, OPENDAL_UNEXPECTED,
                             "native result has no readable bytes");
    }
    return moonbit_make_bytes(0, 0);
  }
  moonbit_bytes_t output = copy_bytes(result->data, result->data_len);
  free(result->data);
  result->data = NULL;
  result->data_len = 0;
  result->data_capacity = 0;
  return output;
}

MOONBIT_FFI_EXPORT void
moonbit_opendal_result_release(moonbit_opendal_result_t *result) {
  result_release_payload(result);
}

MOONBIT_FFI_EXPORT uint32_t moonbit_opendal_live_operator_count(void) {
  return atomic_load_explicit(&live_operator_count, memory_order_relaxed);
}
