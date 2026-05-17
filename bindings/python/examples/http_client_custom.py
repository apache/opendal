#!/usr/bin/env python3
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

"""Example: Using Custom HTTP Client with Self-Signed Certificates.

This example demonstrates how to configure OpenDAL to work with S3-compatible
services that use self-signed SSL/TLS certificates, such as a local MinIO
instance with HTTPS enabled.

WARNING: danger_accept_invalid_certs=True disables certificate verification
and should ONLY be used in testing/development environments. Never use this
in production!
"""

import opendal
from opendal.layers import HttpClientLayer


def example_with_self_signed_cert() -> None:
    """Connect to MinIO with self-signed certificate."""
    # Create a custom HTTP client that accepts invalid certificates
    # WARNING: Only use this for testing/development!
    client = opendal.HttpClient(danger_accept_invalid_certs=True)

    # Create the HTTP client layer
    http_layer = HttpClientLayer(client)

    # Create an S3 operator for a local MinIO instance with HTTPS
    op = opendal.Operator(
        "s3",
        bucket="my-bucket",
        endpoint="https://localhost:9000",
        access_key_id="minioadmin",
        secret_access_key="minioadmin",
        region="us-east-1"
    )

    # Apply the custom HTTP client layer
    op = op.layer(http_layer)

    # Now you can use the operator normally
    # Write data
    op.write("test.txt", b"Hello, OpenDAL with custom HTTP client!")

    # Read data
    data = op.read("test.txt")
    print(f"Read data: {data}")

    # Delete data
    op.delete("test.txt")
    print("Data deleted successfully")


def example_with_timeout() -> opendal.Operator:
    """Configure HTTP client with custom timeout."""
    # Create HTTP client with 30 second timeout
    client = opendal.HttpClient(timeout=30.0)

    # Create the layer
    http_layer = HttpClientLayer(client)

    # Apply to operator
    op = opendal.Operator("s3", bucket="my-bucket")
    op = op.layer(http_layer)

    return op


def example_with_both_options() -> opendal.Operator:
    """Create custom client with both invalid certs and timeout."""
    # Create HTTP client with both options
    # This is useful for development/testing against local services
    client = opendal.HttpClient(
        danger_accept_invalid_certs=True,
        timeout=60.0
    )

    http_layer = HttpClientLayer(client)

    op = opendal.Operator(
        "s3",
        bucket="test-bucket",
        endpoint="https://localhost:9000",
        access_key_id="testuser",
        secret_access_key="testpass",
        region="us-east-1"
    )

    op = op.layer(http_layer)

    return op


async def example_async_with_custom_client() -> None:
    """Use custom HTTP client with async operator."""
    # Custom client also works with AsyncOperator
    client = opendal.HttpClient(danger_accept_invalid_certs=True)
    http_layer = HttpClientLayer(client)

    op = opendal.AsyncOperator("memory")
    op = op.layer(http_layer)

    # Use async operations
    await op.write("async_test.txt", b"Async write with custom client")
    data = await op.read("async_test.txt")
    print(f"Async read data: {data}")

    await op.delete("async_test.txt")


if __name__ == "__main__":
    print("Example 1: Using custom HTTP client with self-signed certificates")
    print("=" * 70)
    print("This example requires a running MinIO instance with HTTPS.")
    print("To run this example, uncomment the line below:")
    print("# example_with_self_signed_cert()")
    print()

    print("Example 2: Using custom timeout")
    print("=" * 70)
    op = example_with_timeout()
    print(f"Created operator with 30s timeout: {op}")
    print()

    print("Example 3: Using both options")
    print("=" * 70)
    op = example_with_both_options()
    print(f"Created operator with invalid certs + timeout: {op}")
    print()

    print("For async example, run:")
    print(">>> import asyncio")
    print(">>> asyncio.run(example_async_with_custom_client())")
