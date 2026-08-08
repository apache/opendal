# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to you under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

import ctypes
import json
import threading
from collections.abc import Mapping
from pathlib import Path
from typing import Any

RUNTIME_PROTOCOL = 1
STATUS_OK = 0
STATUS_BUFFER_TOO_SMALL = 6


class RuntimeProtocolError(RuntimeError):
    pass


class ByteSlice(ctypes.Structure):
    _fields_ = [("data", ctypes.c_void_p), ("len", ctypes.c_size_t)]


class KeyValue(ctypes.Structure):
    _fields_ = [("key", ByteSlice), ("value", ByteSlice)]


class OutputBuffer(ctypes.Structure):
    _fields_ = [
        ("data", ctypes.c_void_p),
        ("capacity", ctypes.c_size_t),
        ("len", ctypes.c_size_t),
    ]


class ServiceRegistrationV1(ctypes.Structure):
    _fields_ = [
        ("struct_size", ctypes.c_size_t),
        ("required_runtime_protocol", ctypes.c_uint32),
        ("package_id", ByteSlice),
        ("component_id", ByteSlice),
        ("entry_symbol", ByteSlice),
        ("library_path", ByteSlice),
    ]


class RuntimeProtocolInfoV1(ctypes.Structure):
    _fields_ = [
        ("struct_size", ctypes.c_size_t),
        ("minimum_runtime_protocol", ctypes.c_uint32),
        ("runtime_protocol", ctypes.c_uint32),
    ]


RegisterServiceFn = ctypes.CFUNCTYPE(
    ctypes.c_int32,
    ctypes.POINTER(ServiceRegistrationV1),
    ctypes.POINTER(OutputBuffer),
)
CreateOperatorFn = ctypes.CFUNCTYPE(
    ctypes.c_int32,
    ByteSlice,
    ctypes.POINTER(KeyValue),
    ctypes.c_size_t,
    ctypes.POINTER(ctypes.c_void_p),
    ctypes.POINTER(OutputBuffer),
)
OperatorInfoFn = ctypes.CFUNCTYPE(
    ctypes.c_int32,
    ctypes.c_void_p,
    ctypes.POINTER(OutputBuffer),
    ctypes.POINTER(OutputBuffer),
)
OperatorWriteFn = ctypes.CFUNCTYPE(
    ctypes.c_int32,
    ctypes.c_void_p,
    ByteSlice,
    ByteSlice,
    ctypes.POINTER(OutputBuffer),
)
OperatorReadFn = ctypes.CFUNCTYPE(
    ctypes.c_int32,
    ctypes.c_void_p,
    ByteSlice,
    ctypes.POINTER(OutputBuffer),
    ctypes.POINTER(OutputBuffer),
)
OperatorDestroyFn = ctypes.CFUNCTYPE(None, ctypes.c_void_p)


class RuntimeApiV1(ctypes.Structure):
    _fields_ = [
        ("struct_size", ctypes.c_size_t),
        ("register_service", RegisterServiceFn),
        ("create_operator", CreateOperatorFn),
        ("operator_info", OperatorInfoFn),
        ("operator_write", OperatorWriteFn),
        ("operator_read", OperatorReadFn),
        ("operator_destroy", OperatorDestroyFn),
    ]


def _bytes(value: str | bytes) -> tuple[ByteSlice, ctypes.Array[ctypes.c_char]]:
    raw = value.encode() if isinstance(value, str) else value
    storage = ctypes.create_string_buffer(raw)
    return ByteSlice(ctypes.cast(storage, ctypes.c_void_p), len(raw)), storage


def _output(capacity: int = 4096) -> tuple[OutputBuffer, ctypes.Array[ctypes.c_char]]:
    storage = ctypes.create_string_buffer(capacity)
    return OutputBuffer(ctypes.cast(storage, ctypes.c_void_p), capacity, 0), storage


def _message(output: OutputBuffer, storage: ctypes.Array[ctypes.c_char]) -> str:
    length = min(output.len, len(storage))
    return bytes(storage[:length]).decode(errors="replace")


_native_path = Path(__file__).parent / "_native" / "libopendal_runtime_poc.so"
_native = ctypes.CDLL(str(_native_path))
_native.opendal_runtime_get_api_v1.argtypes = [
    ctypes.c_uint32,
    ctypes.POINTER(RuntimeProtocolInfoV1),
    ctypes.POINTER(ctypes.POINTER(RuntimeApiV1)),
]
_native.opendal_runtime_get_api_v1.restype = ctypes.c_int32

_protocol = RuntimeProtocolInfoV1(
    struct_size=ctypes.sizeof(RuntimeProtocolInfoV1),
    minimum_runtime_protocol=0,
    runtime_protocol=0,
)
_api_pointer = ctypes.POINTER(RuntimeApiV1)()
_status = _native.opendal_runtime_get_api_v1(
    RUNTIME_PROTOCOL, ctypes.byref(_protocol), ctypes.byref(_api_pointer)
)
if _status != STATUS_OK or not _api_pointer:
    raise RuntimeProtocolError(
        "runtime protocol negotiation failed: "
        f"required={RUNTIME_PROTOCOL}, "
        f"supported={_protocol.minimum_runtime_protocol}..{_protocol.runtime_protocol}"
    )
_api = _api_pointer.contents


def _register_service(manifest: Mapping[str, Any], library_path: Path) -> None:
    component = manifest.get("component")
    if not isinstance(component, Mapping) or component.get("kind") != "service":
        raise RuntimeProtocolError("manifest must describe one service")
    package_id = manifest.get("package_id")
    component_id = component.get("id")
    entry_symbol = manifest.get("native_entry_symbol")
    required_protocol = manifest.get("required_runtime_protocol")
    if not all(isinstance(value, str) for value in (package_id, component_id, entry_symbol)):
        raise RuntimeProtocolError(
            "manifest package ID, component ID, and entry symbol must be strings"
        )
    if not isinstance(required_protocol, int):
        raise RuntimeProtocolError("manifest runtime protocol must be an integer")
    if not library_path.is_file():
        raise RuntimeProtocolError(f"native service artifact does not exist: {library_path}")

    package, package_storage = _bytes(package_id)
    service, service_storage = _bytes(component_id)
    entry, entry_storage = _bytes(entry_symbol)
    path, path_storage = _bytes(str(library_path.resolve()))
    registration = ServiceRegistrationV1(
        struct_size=ctypes.sizeof(ServiceRegistrationV1),
        required_runtime_protocol=required_protocol,
        package_id=package,
        component_id=service,
        entry_symbol=entry,
        library_path=path,
    )
    error, error_storage = _output()
    status = _api.register_service(ctypes.byref(registration), ctypes.byref(error))
    _ = (package_storage, service_storage, entry_storage, path_storage)
    if status != STATUS_OK:
        raise RuntimeProtocolError(_message(error, error_storage))


def _stringify(value: object) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (str, int, float, Path)):
        return str(value)
    raise TypeError(f"unsupported configuration value: {type(value).__name__}")


def _options(values: Mapping[str, object]) -> tuple[Any, list[Any]]:
    pairs: list[KeyValue] = []
    keepalive: list[Any] = []
    for key, value in values.items():
        encoded_key, key_storage = _bytes(key)
        encoded_value, value_storage = _bytes(_stringify(value))
        pairs.append(KeyValue(encoded_key, encoded_value))
        keepalive.extend((key_storage, value_storage))
    array_type = KeyValue * len(pairs)
    return array_type(*pairs), keepalive


class Operator:
    def __init__(self, scheme: str, **config: object) -> None:
        self._lock = threading.RLock()
        self._handle = ctypes.c_void_p()
        scheme = scheme.strip().lower().replace("_", "-")
        encoded_scheme, scheme_storage = _bytes(scheme)
        options, option_storage = _options(config)
        handle = ctypes.c_void_p()
        error, error_storage = _output()
        status = _api.create_operator(
            encoded_scheme,
            options,
            len(options),
            ctypes.byref(handle),
            ctypes.byref(error),
        )
        _ = (scheme_storage, option_storage)
        if status != STATUS_OK:
            raise RuntimeProtocolError(_message(error, error_storage))
        self._handle = handle

    def close(self) -> None:
        with self._lock:
            handle = self._handle
            if handle:
                _api.operator_destroy(handle)
                self._handle = ctypes.c_void_p()

    def __enter__(self) -> Operator:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def __del__(self) -> None:
        self.close()

    def _require_handle(self) -> ctypes.c_void_p:
        if not self._handle:
            raise RuntimeProtocolError("operator is closed")
        return self._handle

    @property
    def info(self) -> dict[str, str]:
        with self._lock:
            output, output_storage = _output()
            error, error_storage = _output()
            status = _api.operator_info(
                self._require_handle(), ctypes.byref(output), ctypes.byref(error)
            )
            if status != STATUS_OK:
                raise RuntimeProtocolError(_message(error, error_storage))
            return json.loads(_message(output, output_storage))

    def write(self, path: str, data: bytes) -> None:
        with self._lock:
            encoded_path, path_storage = _bytes(path)
            encoded_data, data_storage = _bytes(data)
            error, error_storage = _output()
            status = _api.operator_write(
                self._require_handle(), encoded_path, encoded_data, ctypes.byref(error)
            )
            _ = (path_storage, data_storage)
            if status != STATUS_OK:
                raise RuntimeProtocolError(_message(error, error_storage))

    def read(self, path: str) -> bytes:
        with self._lock:
            encoded_path, path_storage = _bytes(path)
            output, output_storage = _output()
            error, error_storage = _output()
            status = _api.operator_read(
                self._require_handle(),
                encoded_path,
                ctypes.byref(output),
                ctypes.byref(error),
            )
            if status == STATUS_BUFFER_TOO_SMALL:
                output, output_storage = _output(output.len)
                status = _api.operator_read(
                    self._require_handle(),
                    encoded_path,
                    ctypes.byref(output),
                    ctypes.byref(error),
                )
            _ = path_storage
            if status != STATUS_OK:
                raise RuntimeProtocolError(_message(error, error_storage))
            return bytes(output_storage[: output.len])
