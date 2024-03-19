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

from .config import Config, ConfigField, FieldType, RefinedFieldType

S3 = Config(
    name="s3",
    desc="AWS S3 and compatible services support (MinIO, DigitalOcean Spaces, Tencent Cloud Object Storage, etc.)",

    rust_cfg='#[cfg(feature = "services-s3")]',

    fields=[
        ConfigField(
            name="root",
            ty=FieldType.String,
            desc="""
            root of this backend.

            All operations will happen under this root.
            """,
            default="/",
        ),
        ConfigField(
            name="bucket",
            ty=FieldType.String,
            desc="bucket name of this backend.",
            required=True,
        ),
        ConfigField(
            name="endpoint",
            ty=FieldType.String,
            desc="""
            endpoint of this backend.

            Endpoint must be full uri, e.g.

            - AWS S3: `https://s3.amazonaws.com` or `https://s3.{region}.amazonaws.com`
            - Cloudflare R2: `https://<ACCOUNT_ID>.r2.cloudflarestorage.com`
            - Aliyun OSS: `https://{region}.aliyuncs.com`
            - Tencent COS: `https://cos.{region}.myqcloud.com`
            - Minio: `http://127.0.0.1:9000`

            If user inputs endpoint without scheme like "s3.amazonaws.com", we
            will prepend "https://" before it.
            """,
            default="https://s3.amazonaws.com",
        ),
        ConfigField(
            name="region",
            ty=FieldType.String,
            desc="""
            Region represent the signing region of this endpoint. This is required
            if you are using the default AWS S3 endpoint.

            If using a custom endpoint,
            - If region is set, we will take user's input first.
            - If not, we will try to load it from environment.
            """,
        ),
        ConfigField(
            name="access_key_id",
            ty=FieldType.String,
            sensitive=True,
            desc="""
            access_key_id of this backend.

            - If access_key_id is set, we will take user's input first.
            - If not, we will try to load it from environment.
            """,
        ),
        ConfigField(
            name="secret_access_key",
            ty=FieldType.String,
            sensitive=True,
            desc="""
            secret_access_key of this backend.

            - If secret_access_key is set, we will take user's input first.
            - If not, we will try to load it from environment.
            """,
        ),
        ConfigField(
            name="security_token",
            ty=FieldType.String,
            sensitive=True,
            desc="""
            security_token (aka, session token) of this backend.

            This token will expire after sometime, it's recommended to set security_token by hand.
            """,
        ),
        ConfigField(
            name="role_arn",
            ty=FieldType.String,
            desc="""
            role_arn for this backend.

            If `role_arn` is set, we will use already known config as source
            credential to assume role with `role_arn`.
            """,
        ),
        ConfigField(
            name="external_id",
            ty=FieldType.String,
            desc="external_id for this backend.",
        ),
        ConfigField(
            name="disable_config_load",
            ty=FieldType.Bool,
            required=True,
            desc="""
            Disable config load so that opendal will not load config from environment,
            e.g, envs like `AWS_ACCESS_KEY_ID` or files like `~/.aws/config`
            """,
        ),
        ConfigField(
            name="disable_ec2_metadata",
            ty=FieldType.Bool,
            required=True,
            desc="""
            Disable load credential from ec2 metadata.

            This option is used to disable the default behavior of opendal
            to load credential from ec2 metadata, a.k.a, IMDSv2
            """,
        ),
        ConfigField(
            name="allow_anonymous",
            ty=FieldType.Bool,
            required=True,
            desc="""
            Allow anonymous will allow opendal to send request without signing
            when credential is not loaded.
            """,
        ),
        ConfigField(
            name="server_side_encryption",
            ty=FieldType.String,
            desc="server_side_encryption for this backend.",
            available=["`AES256`", "`aws:kms`"],
        ),
        ConfigField(
            name="server_side_encryption_aws_kms_key_id",
            ty=FieldType.String,
            desc="""
            server_side_encryption_aws_kms_key_id for this backend

            - If `server_side_encryption` set to `aws:kms`, and `server_side_encryption_aws_kms_key_id`
            is not set, S3 will use aws managed kms key to encrypt data.
            - If `server_side_encryption` set to `aws:kms`, and `server_side_encryption_aws_kms_key_id`
            is a valid kms key id, S3 will use the provided kms key to encrypt data.
            - If the `server_side_encryption_aws_kms_key_id` is invalid or not found, an error will be
            returned.
            - If `server_side_encryption` is not `aws:kms`, setting `server_side_encryption_aws_kms_key_id`
            is a noop.
            """,
        ),
        ConfigField(
            name="server_side_encryption_customer_algorithm",
            ty=FieldType.String,
            desc="server_side_encryption_customer_algorithm for this backend.",
            available=["`AES256`"]
        ),
        ConfigField(
            name="server_side_encryption_customer_key",
            ty=FieldType.String,
            sensitive=True,
            desc="""
            server_side_encryption_customer_key for this backend.

            # Value

            base64 encoded key that matches algorithm specified in
            `server_side_encryption_customer_algorithm`.
            """,
        ),
        ConfigField(
            name="server_side_encryption_customer_key_md5",
            ty=FieldType.String,
            sensitive=True,
            desc="""
            Set server_side_encryption_customer_key_md5 for this backend.

            # Value

            MD5 digest of key specified in `server_side_encryption_customer_key`.
            """,
        ),
        ConfigField(
            name="default_storage_class",
            ty=FieldType.String,
            desc="""
            default_storage_class for this backend.

            S3 compatible services don't support all of available values.
            """,
            available=[
                "`DEEP_ARCHIVE`",
                "`GLACIER`",
                "`GLACIER_IR`",
                "`INTELLIGENT_TIERING`",
                "`ONEZONE_IA`",
                "`OUTPOSTS`",
                "`REDUCED_REDUNDANCY`",
                "`STANDARD`",
                "`STANDARD_IA`",
            ]
        ),
        ConfigField(
            name="enable_virtual_host_style",
            ty=FieldType.Bool,
            required=True,
            desc="""
            Enable virtual host style so that opendal will send API requests
            in virtual host style instead of path style.

            - By default, opendal will send API to `https://s3.us-east-1.amazonaws.com/bucket_name`
            - Enabled, opendal will send API to `https://bucket_name.s3.us-east-1.amazonaws.com`
            """,
        ),
        ConfigField(
            name="batch_max_operations",
            ty=FieldType.Int,
            refined_ty=RefinedFieldType.RustUsize,
            desc="""
            Set maximum batch operations of this backend.

            Some compatible services have a limit on the number of operations in a batch request.
            For example, R2 could return `Internal Error` while batch delete 1000 files.

            Please tune this value based on services' document.
            """,
        ),
        ConfigField(
            name="disable_stat_with_override",
            ty=FieldType.Bool,
            required=True,
            desc="""
            Disable stat with override so that opendal will not send stat request with override queries.

            For example, R2 doesn't support stat with `response_content_type` query.
            """,
        )
    ]
)
