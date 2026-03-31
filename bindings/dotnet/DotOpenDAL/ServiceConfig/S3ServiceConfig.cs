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

// Generated from bindings/java ServiceConfig definitions.

using DotOpenDAL.ServiceConfig.Abstractions;

namespace DotOpenDAL.ServiceConfig
{
    /// <summary>
    /// Configuration for service s3.
    /// </summary>
    public sealed class S3ServiceConfig : IServiceConfig
    {
        /// <summary>
        /// access_key_id of this backend. If access_key_id is set, we will take user's input first. If not, we will try to load it from environment.
        /// </summary>
        public string? AccessKeyId { get; init; }
        /// <summary>
        /// Allow anonymous will allow opendal to send request without signing when credential is not loaded.
        /// </summary>
        public bool? AllowAnonymous { get; init; }
        /// <summary>
        /// Set maximum batch operations of this backend. Some compatible services have a limit on the number of operations in a batch request. For example, R2 could return Internal Error while batch delete 1000 files. Please tune this value based on services' document.
        /// </summary>
        public long? BatchMaxOperations { get; init; }
        /// <summary>
        /// bucket name of this backend. required.
        /// </summary>
        public string? Bucket { get; init; }
        /// <summary>
        /// Checksum Algorithm to use when sending checksums in HTTP headers. This is necessary when writing to AWS S3 Buckets with Object Lock enabled for example. Available options: "crc32c"
        /// </summary>
        public string? ChecksumAlgorithm { get; init; }
        /// <summary>
        /// default storage_class for this backend. Available values: DEEP_ARCHIVE GLACIER GLACIER_IR INTELLIGENT_TIERING ONEZONE_IA EXPRESS_ONEZONE OUTPOSTS REDUCED_REDUNDANCY STANDARD STANDARD_IA S3 compatible services don't support all of them
        /// </summary>
        public string? DefaultStorageClass { get; init; }
        /// <summary>
        /// Set the maximum delete size of this backend. Some compatible services have a limit on the number of operations in a batch request. For example, R2 could return Internal Error while batch delete 1000 files. Please tune this value based on services' document.
        /// </summary>
        public long? DeleteMaxSize { get; init; }
        /// <summary>
        /// Disable config load so that opendal will not load config from environment. For examples: envs like AWS_ACCESS_KEY_ID files like ~/.aws/config
        /// </summary>
        public bool? DisableConfigLoad { get; init; }
        /// <summary>
        /// Disable load credential from ec2 metadata. This option is used to disable the default behavior of opendal to load credential from ec2 metadata, a.k.a, IMDSv2
        /// </summary>
        public bool? DisableEc2Metadata { get; init; }
        /// <summary>
        /// OpenDAL uses List Objects V2 by default to list objects. However, some legacy services do not yet support V2. This option allows users to switch back to the older List Objects V1.
        /// </summary>
        public bool? DisableListObjectsV2 { get; init; }
        /// <summary>
        /// Disable stat with override so that opendal will not send stat request with override queries. For example, R2 doesn't support stat with response_content_type query.
        /// </summary>
        public bool? DisableStatWithOverride { get; init; }
        /// <summary>
        /// Disable write with if match so that opendal will not send write request with if match headers. For example, Ceph RADOS S3 doesn't support write with if match.
        /// </summary>
        public bool? DisableWriteWithIfMatch { get; init; }
        /// <summary>
        /// Indicates whether the client agrees to pay for the requests made to the S3 bucket.
        /// </summary>
        public bool? EnableRequestPayer { get; init; }
        /// <summary>
        /// is bucket versioning enabled for this bucket
        /// </summary>
        public bool? EnableVersioning { get; init; }
        /// <summary>
        /// Enable virtual host style so that opendal will send API requests in virtual host style instead of path style. By default, opendal will send API to https://s3.us-east-1.amazonaws.com/bucket_name Enabled, opendal will send API to https://bucket_name.s3.us-east-1.amazonaws.com
        /// </summary>
        public bool? EnableVirtualHostStyle { get; init; }
        /// <summary>
        /// Enable write with append so that opendal will send write request with append headers.
        /// </summary>
        public bool? EnableWriteWithAppend { get; init; }
        /// <summary>
        /// endpoint of this backend. Endpoint must be full uri, e.g. AWS S3: https://s3.amazonaws.com or https://s3.{region}.amazonaws.com Cloudflare R2: https://&lt;ACCOUNT_ID&gt;.r2.cloudflarestorage.com Aliyun OSS: https://{region}.aliyuncs.com Tencent COS: https://cos.{region}.myqcloud.com Minio: http://127.0.0.1:9000 If user inputs endpoint without scheme like "s3.amazonaws.com", we will prepend "https://" before it. If endpoint is set, we will take user's input first. If not, we will try to load it from environment. If still not set, default to https://s3.amazonaws.com.
        /// </summary>
        public string? Endpoint { get; init; }
        /// <summary>
        /// external_id for this backend.
        /// </summary>
        public string? ExternalId { get; init; }
        /// <summary>
        /// Region represent the signing region of this endpoint. This is required if you are using the default AWS S3 endpoint. If using a custom endpoint, If region is set, we will take user's input first. If not, we will try to load it from environment.
        /// </summary>
        public string? Region { get; init; }
        /// <summary>
        /// role_arn for this backend. If role_arn is set, we will use already known config as source credential to assume role with role_arn.
        /// </summary>
        public string? RoleArn { get; init; }
        /// <summary>
        /// role_session_name for this backend.
        /// </summary>
        public string? RoleSessionName { get; init; }
        /// <summary>
        /// root of this backend. All operations will happen under this root. default to / if not set.
        /// </summary>
        public string? Root { get; init; }
        /// <summary>
        /// secret_access_key of this backend. If secret_access_key is set, we will take user's input first. If not, we will try to load it from environment.
        /// </summary>
        public string? SecretAccessKey { get; init; }
        /// <summary>
        /// server_side_encryption for this backend. Available values: AES256, aws:kms.
        /// </summary>
        public string? ServerSideEncryption { get; init; }
        /// <summary>
        /// server_side_encryption_aws_kms_key_id for this backend If server_side_encryption set to aws:kms, and server_side_encryption_aws_kms_key_id is not set, S3 will use aws managed kms key to encrypt data. If server_side_encryption set to aws:kms, and server_side_encryption_aws_kms_key_id is a valid kms key id, S3 will use the provided kms key to encrypt data. If the server_side_encryption_aws_kms_key_id is invalid or not found, an error will be returned. If server_side_encryption is not aws:kms, setting server_side_encryption_aws_kms_key_id is a noop.
        /// </summary>
        public string? ServerSideEncryptionAwsKmsKeyId { get; init; }
        /// <summary>
        /// server_side_encryption_customer_algorithm for this backend. Available values: AES256.
        /// </summary>
        public string? ServerSideEncryptionCustomerAlgorithm { get; init; }
        /// <summary>
        /// server_side_encryption_customer_key for this backend. Value: BASE64-encoded key that matches algorithm specified in server_side_encryption_customer_algorithm.
        /// </summary>
        public string? ServerSideEncryptionCustomerKey { get; init; }
        /// <summary>
        /// Set server_side_encryption_customer_key_md5 for this backend. Value: MD5 digest of key specified in server_side_encryption_customer_key.
        /// </summary>
        public string? ServerSideEncryptionCustomerKeyMd5 { get; init; }
        /// <summary>
        /// session_token (aka, security token) of this backend. This token will expire after sometime, it's recommended to set session_token by hand.
        /// </summary>
        public string? SessionToken { get; init; }

        public string Scheme => "s3";

        public IReadOnlyDictionary<string, string> ToOptions()
        {
            var map = new Dictionary<string, string>();
            if (AccessKeyId is not null)
            {
                map["access_key_id"] = Utilities.ToOptionString(AccessKeyId);
            }
            if (AllowAnonymous is not null)
            {
                map["allow_anonymous"] = Utilities.ToOptionString(AllowAnonymous);
            }
            if (BatchMaxOperations is not null)
            {
                map["batch_max_operations"] = Utilities.ToOptionString(BatchMaxOperations);
            }
            if (Bucket is not null)
            {
                map["bucket"] = Utilities.ToOptionString(Bucket);
            }
            if (ChecksumAlgorithm is not null)
            {
                map["checksum_algorithm"] = Utilities.ToOptionString(ChecksumAlgorithm);
            }
            if (DefaultStorageClass is not null)
            {
                map["default_storage_class"] = Utilities.ToOptionString(DefaultStorageClass);
            }
            if (DeleteMaxSize is not null)
            {
                map["delete_max_size"] = Utilities.ToOptionString(DeleteMaxSize);
            }
            if (DisableConfigLoad is not null)
            {
                map["disable_config_load"] = Utilities.ToOptionString(DisableConfigLoad);
            }
            if (DisableEc2Metadata is not null)
            {
                map["disable_ec2_metadata"] = Utilities.ToOptionString(DisableEc2Metadata);
            }
            if (DisableListObjectsV2 is not null)
            {
                map["disable_list_objects_v2"] = Utilities.ToOptionString(DisableListObjectsV2);
            }
            if (DisableStatWithOverride is not null)
            {
                map["disable_stat_with_override"] = Utilities.ToOptionString(DisableStatWithOverride);
            }
            if (DisableWriteWithIfMatch is not null)
            {
                map["disable_write_with_if_match"] = Utilities.ToOptionString(DisableWriteWithIfMatch);
            }
            if (EnableRequestPayer is not null)
            {
                map["enable_request_payer"] = Utilities.ToOptionString(EnableRequestPayer);
            }
            if (EnableVersioning is not null)
            {
                map["enable_versioning"] = Utilities.ToOptionString(EnableVersioning);
            }
            if (EnableVirtualHostStyle is not null)
            {
                map["enable_virtual_host_style"] = Utilities.ToOptionString(EnableVirtualHostStyle);
            }
            if (EnableWriteWithAppend is not null)
            {
                map["enable_write_with_append"] = Utilities.ToOptionString(EnableWriteWithAppend);
            }
            if (Endpoint is not null)
            {
                map["endpoint"] = Utilities.ToOptionString(Endpoint);
            }
            if (ExternalId is not null)
            {
                map["external_id"] = Utilities.ToOptionString(ExternalId);
            }
            if (Region is not null)
            {
                map["region"] = Utilities.ToOptionString(Region);
            }
            if (RoleArn is not null)
            {
                map["role_arn"] = Utilities.ToOptionString(RoleArn);
            }
            if (RoleSessionName is not null)
            {
                map["role_session_name"] = Utilities.ToOptionString(RoleSessionName);
            }
            if (Root is not null)
            {
                map["root"] = Utilities.ToOptionString(Root);
            }
            if (SecretAccessKey is not null)
            {
                map["secret_access_key"] = Utilities.ToOptionString(SecretAccessKey);
            }
            if (ServerSideEncryption is not null)
            {
                map["server_side_encryption"] = Utilities.ToOptionString(ServerSideEncryption);
            }
            if (ServerSideEncryptionAwsKmsKeyId is not null)
            {
                map["server_side_encryption_aws_kms_key_id"] = Utilities.ToOptionString(ServerSideEncryptionAwsKmsKeyId);
            }
            if (ServerSideEncryptionCustomerAlgorithm is not null)
            {
                map["server_side_encryption_customer_algorithm"] = Utilities.ToOptionString(ServerSideEncryptionCustomerAlgorithm);
            }
            if (ServerSideEncryptionCustomerKey is not null)
            {
                map["server_side_encryption_customer_key"] = Utilities.ToOptionString(ServerSideEncryptionCustomerKey);
            }
            if (ServerSideEncryptionCustomerKeyMd5 is not null)
            {
                map["server_side_encryption_customer_key_md5"] = Utilities.ToOptionString(ServerSideEncryptionCustomerKeyMd5);
            }
            if (SessionToken is not null)
            {
                map["session_token"] = Utilities.ToOptionString(SessionToken);
            }
            return map;
        }
    }

}
