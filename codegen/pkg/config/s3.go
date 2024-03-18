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

package config

var S3 = Config{
	ModCfg: `#[cfg(feature = "services-s3")]`,
	Name:   "s3",
	Desc:   "AWS S3 and compatible services (including minio, digitalocean space, Tencent Cloud Object Storage(COS) and so on) support.",
	Fields: []Field{
		{
			Name: "root",
			Type: fileTypeString,
			Desc: "root of this backend.\n\n" +
				"All operations will happen under this root.",
			Default: "/",
		},
		{
			Name:     "bucket",
			Type:     fileTypeString,
			Desc:     "bucket name of this backend.",
			Required: true,
		},
		{
			Name: "endpoint",
			Type: fileTypeString,
			Desc: "endpoint of this backend.\n\n" +
				"Endpoint must be full uri, e.g.\n\n" +
				"- AWS S3: `https://s3.amazonaws.com` or `https://s3.{region}.amazonaws.com`\n" +
				"- Cloudflare R2: `https://<ACCOUNT_ID>.r2.cloudflarestorage.com`\n" +
				"- Aliyun OSS: `https://{region}.aliyuncs.com`\n" +
				"- Tencent COS: `https://cos.{region}.myqcloud.com`\n" +
				"- Minio: `http://127.0.0.1:9000`\n\n" +
				`If user inputs endpoint without scheme like "s3.amazonaws.com", we
will prepend "https://" before it.`,
			Default: "https://s3.amazonaws.com",
		},
		{
			Name: "region",
			Type: fileTypeString,
			Desc: `Region represent the signing region of this endpoint. This is required
if you are using the default AWS S3 endpoint.

If using a custom endpoint,
- If region is set, we will take user's input first.
- If not, we will try to load it from environment.
`,
		},
		{
			Name:      "access_key_id",
			Type:      fileTypeString,
			Sensitive: true,
			Desc: `access_key_id of this backend.

- If access_key_id is set, we will take user's input first.
- If not, we will try to load it from environment.
`,
		},
		{
			Name:      "secret_access_key",
			Type:      fileTypeString,
			Sensitive: true,
			Desc: `secret_access_key of this backend.

- If secret_access_key is set, we will take user's input first.
- If not, we will try to load it from environment.
`,
		},
		{
			Name:      "security_token",
			Type:      fileTypeString,
			Sensitive: true,
			Desc: `security_token (aka, session token) of this backend.

This token will expire after sometime, it's recommended to set security_token
by hand.`,
		},
		{
			Name: "role_arn",
			Type: fileTypeString,
			Desc: "role_arn for this backend.\n\n" +
				"If `role_arn` is set, we will use already known config as source\n" +
				"credential to assume role with `role_arn`.",
		},
		{
			Name: "external_id",
			Type: fileTypeString,
			Desc: "external_id for this backend.",
		},
		{
			Name:     "disable_config_load",
			Type:     fileTypeBool,
			Required: true,
			Desc: "Disable config load so that opendal will not load config from environment,\n" +
				"e.g, envs like `AWS_ACCESS_KEY_ID` or files like `~/.aws/config`",
		},
		{
			Name:     "disable_ec2_metadata",
			Type:     fileTypeBool,
			Required: true,
			Desc: `Disable load credential from ec2 metadata.

This option is used to disable the default behavior of opendal
to load credential from ec2 metadata, a.k.a, IMDSv2`,
		},
		{
			Name:     "allow_anonymous",
			Type:     fileTypeBool,
			Required: true,
			Desc: `Allow anonymous will allow opendal to send request without signing
when credential is not loaded.`,
		},
		{
			Name:      "server_side_encryption",
			Type:      fileTypeString,
			Desc:      "server_side_encryption for this backend.",
			Available: []string{"`AES256`", "`aws:kms`"},
		},
		{
			Name: "server_side_encryption_aws_kms_key_id",
			Type: fileTypeString,
			Desc: "server_side_encryption_aws_kms_key_id for this backend\n\n" +
				"- If `server_side_encryption` set to `aws:kms`, and `server_side_encryption_aws_kms_key_id`\n" +
				"is not set, S3 will use aws managed kms key to encrypt data.\n" +
				"- If `server_side_encryption` set to `aws:kms`, and `server_side_encryption_aws_kms_key_id`\n" +
				"is a valid kms key id, S3 will use the provided kms key to encrypt data.\n" +
				"- If the `server_side_encryption_aws_kms_key_id` is invalid or not found, an error will be\n" +
				"returned.\n" +
				"- If `server_side_encryption` is not `aws:kms`, setting `server_side_encryption_aws_kms_key_id`\n" +
				"is a noop.",
		},
		{
			Name:      "server_side_encryption_customer_algorithm",
			Type:      fileTypeString,
			Desc:      "server_side_encryption_customer_algorithm for this backend.",
			Available: []string{"`AES256`"},
		},
		{
			Name:      "server_side_encryption_customer_key",
			Type:      fileTypeString,
			Sensitive: true,
			Desc: "server_side_encryption_customer_key for this backend.\n\n" +
				"# Value\n\n" +
				"base64 encoded key that matches algorithm specified in\n" +
				"`server_side_encryption_customer_algorithm`.",
		},
		{
			Name:      "server_side_encryption_customer_key_md5",
			Type:      fileTypeString,
			Sensitive: true,
			Desc: "Set server_side_encryption_customer_key_md5 for this backend.\n\n" +
				"# Value\n\n" +
				"MD5 digest of key specified in `server_side_encryption_customer_key`.",
		},
		{
			Name: "default_storage_class",
			Type: fileTypeString,
			Desc: "default_storage_class for this backend.\n\n" +
				"S3 compatible services don't support all of available values.",
			Available: []string{
				"`DEEP_ARCHIVE`",
				"`GLACIER`",
				"`GLACIER_IR`",
				"`INTELLIGENT_TIERING`",
				"`ONEZONE_IA`",
				"`OUTPOSTS`",
				"`REDUCED_REDUNDANCY`",
				"`STANDARD`",
				"`STANDARD_IA`",
			},
		},
		{
			Name:     "enable_virtual_host_style",
			Type:     fileTypeBool,
			Required: true,
			Desc: "Enable virtual host style so that opendal will send API requests\n" +
				"in virtual host style instead of path style.\n\n" +
				"- By default, opendal will send API to `https://s3.us-east-1.amazonaws.com/bucket_name`\n" +
				"- Enabled, opendal will send API to `https://bucket_name.s3.us-east-1.amazonaws.com`",
		},
		{
			Name:        "batch_max_operations",
			Type:        fileTypeInt,
			RefinedType: refinedTypeRustUsize,
			Desc: "Set maximum batch operations of this backend.\n\n" +
				"Some compatible services have a limit on the number of operations in a batch request.\n" +
				"For example, R2 could return `Internal Error` while batch delete 1000 files.\n\n" +
				"Please tune this value based on services' document.",
		},
		{
			Name:     "disable_stat_with_override",
			Type:     fileTypeBool,
			Required: true,
			Desc: "Disable stat with override so that opendal will not send stat request with override queries.\n\n" +
				"For example, R2 doesn't support stat with `response_content_type` query.",
		},
	},
}
