# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import opendal
import pytest

from opendal.layers import HttpClientLayer


def test_http_client_creation():
    """Test that HttpClient can be created with default settings."""
    client = opendal.HttpClient()
    assert client is not None


def test_http_client_with_invalid_certs():
    """Test that HttpClient can be created with danger_accept_invalid_certs."""
    client = opendal.HttpClient(danger_accept_invalid_certs=True)
    assert client is not None


def test_http_client_with_timeout():
    """Test that HttpClient can be created with a custom timeout."""
    client = opendal.HttpClient(timeout=30.0)
    assert client is not None


def test_http_client_with_all_options():
    """Test that HttpClient can be created with all options."""
    client = opendal.HttpClient(danger_accept_invalid_certs=True, timeout=60.0)
    assert client is not None


def test_http_client_layer_creation():
    """Test that HttpClientLayer can be created."""
    client = opendal.HttpClient()
    layer = HttpClientLayer(client)
    assert layer is not None


def test_http_client_layer_with_operator():
    """Test that HttpClientLayer can be applied to an operator."""
    # Create a custom HTTP client that accepts invalid certificates
    client = opendal.HttpClient(danger_accept_invalid_certs=True)
    layer = HttpClientLayer(client)

    # Create an operator and apply the layer
    # Using memory service for testing since it doesn't require external setup
    op = opendal.Operator("memory")
    op = op.layer(layer)

    # Verify the operator still works
    assert op is not None
    # Basic functionality test
    op.write("test_file", b"test content")
    assert op.read("test_file") == b"test content"


@pytest.mark.asyncio
async def test_http_client_layer_with_async_operator():
    """Test that HttpClientLayer can be applied to an async operator."""
    # Create a custom HTTP client with timeout
    client = opendal.HttpClient(timeout=30.0)
    layer = HttpClientLayer(client)

    # Create an async operator and apply the layer
    op = opendal.AsyncOperator("memory")
    op = op.layer(layer)

    # Verify the operator still works
    assert op is not None
    # Basic functionality test
    await op.write("test_file_async", b"test content async")
    assert await op.read("test_file_async") == b"test content async"
