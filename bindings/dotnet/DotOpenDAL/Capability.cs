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

using DotOpenDAL.Interop.NativeObject;

namespace DotOpenDAL;

/// <summary>
/// Capability is used to describe what operations are supported by current operator.
/// </summary>
/// <remarks>
/// This model maps from native capability payload returned by OpenDAL.
/// For write multi size fields, nullable values are represented by a native sentinel value.
/// </remarks>
public struct Capability
{
    internal Capability(OpenDALCapability native)
    {
        Stat = native.stat != 0;
        StatWithIfMatch = native.statWithIfMatch != 0;
        StatWithIfNoneMatch = native.statWithIfNoneMatch != 0;
        StatWithIfModifiedSince = native.statWithIfModifiedSince != 0;
        StatWithIfUnmodifiedSince = native.statWithIfUnmodifiedSince != 0;
        StatWithOverrideCacheControl = native.statWithOverrideCacheControl != 0;
        StatWithOverrideContentDisposition = native.statWithOverrideContentDisposition != 0;
        StatWithOverrideContentType = native.statWithOverrideContentType != 0;
        StatWithVersion = native.statWithVersion != 0;

        Read = native.read != 0;
        ReadWithIfMatch = native.readWithIfMatch != 0;
        ReadWithIfNoneMatch = native.readWithIfNoneMatch != 0;
        ReadWithIfModifiedSince = native.readWithIfModifiedSince != 0;
        ReadWithIfUnmodifiedSince = native.readWithIfUnmodifiedSince != 0;
        ReadWithOverrideCacheControl = native.readWithOverrideCacheControl != 0;
        ReadWithOverrideContentDisposition = native.readWithOverrideContentDisposition != 0;
        ReadWithOverrideContentType = native.readWithOverrideContentType != 0;
        ReadWithVersion = native.readWithVersion != 0;

        Write = native.write != 0;
        WriteCanMulti = native.writeCanMulti != 0;
        WriteCanEmpty = native.writeCanEmpty != 0;
        WriteCanAppend = native.writeCanAppend != 0;
        WriteWithContentType = native.writeWithContentType != 0;
        WriteWithContentDisposition = native.writeWithContentDisposition != 0;
        WriteWithContentEncoding = native.writeWithContentEncoding != 0;
        WriteWithCacheControl = native.writeWithCacheControl != 0;
        WriteWithIfMatch = native.writeWithIfMatch != 0;
        WriteWithIfNoneMatch = native.writeWithIfNoneMatch != 0;
        WriteWithIfNotExists = native.writeWithIfNotExists != 0;
        WriteWithUserMetadata = native.writeWithUserMetadata != 0;

        WriteMultiMaxSize = native.writeMultiMaxSize == nuint.MaxValue ? null : native.writeMultiMaxSize;
        WriteMultiMinSize = native.writeMultiMinSize == nuint.MaxValue ? null : native.writeMultiMinSize;
        WriteTotalMaxSize = native.writeTotalMaxSize == nuint.MaxValue ? null : native.writeTotalMaxSize;

        CreateDir = native.createDir != 0;
        Delete = native.delete != 0;
        DeleteWithVersion = native.deleteWithVersion != 0;
        DeleteWithRecursive = native.deleteWithRecursive != 0;
        DeleteMaxSize = native.deleteMaxSize == nuint.MaxValue ? null : native.deleteMaxSize;

        Copy = native.copy != 0;
        CopyWithIfNotExists = native.copyWithIfNotExists != 0;
        Rename = native.rename != 0;

        List = native.list != 0;
        ListWithLimit = native.listWithLimit != 0;
        ListWithStartAfter = native.listWithStartAfter != 0;
        ListWithRecursive = native.listWithRecursive != 0;
        ListWithVersions = native.listWithVersions != 0;
        ListWithDeleted = native.listWithDeleted != 0;

        Presign = native.presign != 0;
        PresignRead = native.presignRead != 0;
        PresignStat = native.presignStat != 0;
        PresignWrite = native.presignWrite != 0;
        PresignDelete = native.presignDelete != 0;

        Shared = native.shared != 0;
    }

    /// <summary>
    /// If operator supports stat.
    /// </summary>
    public bool Stat { get; private set; }

    /// <summary>
    /// If operator supports stat with if match.
    /// </summary>
    public bool StatWithIfMatch { get; private set; }

    /// <summary>
    /// If operator supports stat with if none match.
    /// </summary>
    public bool StatWithIfNoneMatch { get; private set; }

    /// <summary>
    /// If operator supports stat with if modified since.
    /// </summary>
    public bool StatWithIfModifiedSince { get; private set; }

    /// <summary>
    /// If operator supports stat with if unmodified since.
    /// </summary>
    public bool StatWithIfUnmodifiedSince { get; private set; }

    /// <summary>
    /// If operator supports stat with override cache control.
    /// </summary>
    public bool StatWithOverrideCacheControl { get; private set; }

    /// <summary>
    /// If operator supports stat with override content disposition.
    /// </summary>
    public bool StatWithOverrideContentDisposition { get; private set; }

    /// <summary>
    /// If operator supports stat with override content type.
    /// </summary>
    public bool StatWithOverrideContentType { get; private set; }

    /// <summary>
    /// If operator supports stat with version.
    /// </summary>
    public bool StatWithVersion { get; private set; }

    /// <summary>
    /// If operator supports read.
    /// </summary>
    public bool Read { get; private set; }

    /// <summary>
    /// If operator supports read with if match.
    /// </summary>
    public bool ReadWithIfMatch { get; private set; }

    /// <summary>
    /// If operator supports read with if none match.
    /// </summary>
    public bool ReadWithIfNoneMatch { get; private set; }

    /// <summary>
    /// If operator supports read with if modified since.
    /// </summary>
    public bool ReadWithIfModifiedSince { get; private set; }

    /// <summary>
    /// If operator supports read with if unmodified since.
    /// </summary>
    public bool ReadWithIfUnmodifiedSince { get; private set; }

    /// <summary>
    /// If operator supports read with override cache control.
    /// </summary>
    public bool ReadWithOverrideCacheControl { get; private set; }

    /// <summary>
    /// If operator supports read with override content disposition.
    /// </summary>
    public bool ReadWithOverrideContentDisposition { get; private set; }

    /// <summary>
    /// If operator supports read with override content type.
    /// </summary>
    public bool ReadWithOverrideContentType { get; private set; }

    /// <summary>
    /// If operator supports read with version.
    /// </summary>
    public bool ReadWithVersion { get; private set; }

    /// <summary>
    /// If operator supports write.
    /// </summary>
    public bool Write { get; private set; }

    /// <summary>
    /// If operator supports write can be called in multi times.
    /// </summary>
    public bool WriteCanMulti { get; private set; }

    /// <summary>
    /// If operator supports write with empty content.
    /// </summary>
    public bool WriteCanEmpty { get; private set; }

    /// <summary>
    /// If operator supports write by append.
    /// </summary>
    public bool WriteCanAppend { get; private set; }

    /// <summary>
    /// If operator supports write with content type.
    /// </summary>
    public bool WriteWithContentType { get; private set; }

    /// <summary>
    /// If operator supports write with content disposition.
    /// </summary>
    public bool WriteWithContentDisposition { get; private set; }

    /// <summary>
    /// If operator supports write with content encoding.
    /// </summary>
    public bool WriteWithContentEncoding { get; private set; }

    /// <summary>
    /// If operator supports write with cache control.
    /// </summary>
    public bool WriteWithCacheControl { get; private set; }

    /// <summary>
    /// If operator supports write with if match.
    /// </summary>
    public bool WriteWithIfMatch { get; private set; }

    /// <summary>
    /// If operator supports write with if none match.
    /// </summary>
    public bool WriteWithIfNoneMatch { get; private set; }

    /// <summary>
    /// If operator supports write with if not exists.
    /// </summary>
    public bool WriteWithIfNotExists { get; private set; }

    /// <summary>
    /// If operator supports write with user metadata.
    /// </summary>
    public bool WriteWithUserMetadata { get; private set; }

    /// <summary>
    /// write_multi_max_size is the max size that services support in write_multi.
    /// </summary>
    public ulong? WriteMultiMaxSize { get; private set; }

    /// <summary>
    /// write_multi_min_size is the min size that services support in write_multi.
    /// </summary>
    public ulong? WriteMultiMinSize { get; private set; }

    /// <summary>
    /// write_total_max_size is the max total size that services support in write.
    /// </summary>
    public ulong? WriteTotalMaxSize { get; private set; }

    /// <summary>
    /// If operator supports create dir.
    /// </summary>
    public bool CreateDir { get; private set; }

    /// <summary>
    /// If operator supports delete.
    /// </summary>
    public bool Delete { get; private set; }

    /// <summary>
    /// If operator supports delete with version.
    /// </summary>
    public bool DeleteWithVersion { get; private set; }

    /// <summary>
    /// If operator supports delete with recursive.
    /// </summary>
    public bool DeleteWithRecursive { get; private set; }

    /// <summary>
    /// delete_max_size is the max size that services support in delete.
    /// </summary>
    public ulong? DeleteMaxSize { get; private set; }

    /// <summary>
    /// If operator supports copy.
    /// </summary>
    public bool Copy { get; private set; }

    /// <summary>
    /// If operator supports copy with if not exists.
    /// </summary>
    public bool CopyWithIfNotExists { get; private set; }

    /// <summary>
    /// If operator supports rename.
    /// </summary>
    public bool Rename { get; private set; }

    /// <summary>
    /// If operator supports list.
    /// </summary>
    public bool List { get; private set; }

    /// <summary>
    /// If backend supports list with limit.
    /// </summary>
    public bool ListWithLimit { get; private set; }

    /// <summary>
    /// If backend supports list with start after.
    /// </summary>
    public bool ListWithStartAfter { get; private set; }

    /// <summary>
    /// If backend supports list with recursive.
    /// </summary>
    public bool ListWithRecursive { get; private set; }

    /// <summary>
    /// If backend supports list with versions.
    /// </summary>
    public bool ListWithVersions { get; private set; }

    /// <summary>
    /// If backend supports list with deleted.
    /// </summary>
    public bool ListWithDeleted { get; private set; }

    /// <summary>
    /// If operator supports presign.
    /// </summary>
    public bool Presign { get; private set; }

    /// <summary>
    /// If operator supports presign read.
    /// </summary>
    public bool PresignRead { get; private set; }

    /// <summary>
    /// If operator supports presign stat.
    /// </summary>
    public bool PresignStat { get; private set; }

    /// <summary>
    /// If operator supports presign write.
    /// </summary>
    public bool PresignWrite { get; private set; }

    /// <summary>
    /// If operator supports presign delete.
    /// </summary>
    public bool PresignDelete { get; private set; }

    /// <summary>
    /// If operator supports shared.
    /// </summary>
    public bool Shared { get; private set; }
}