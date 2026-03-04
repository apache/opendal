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

using System.Runtime.InteropServices;

namespace DotOpenDAL.Interop.NativeObject;

[StructLayout(LayoutKind.Sequential)]
internal struct OpenDALCapability
{
    [MarshalAs(UnmanagedType.U1)] internal byte stat;
    [MarshalAs(UnmanagedType.U1)] internal byte statWithIfMatch;
    [MarshalAs(UnmanagedType.U1)] internal byte statWithIfNoneMatch;
    [MarshalAs(UnmanagedType.U1)] internal byte statWithIfModifiedSince;
    [MarshalAs(UnmanagedType.U1)] internal byte statWithIfUnmodifiedSince;
    [MarshalAs(UnmanagedType.U1)] internal byte statWithOverrideCacheControl;
    [MarshalAs(UnmanagedType.U1)] internal byte statWithOverrideContentDisposition;
    [MarshalAs(UnmanagedType.U1)] internal byte statWithOverrideContentType;
    [MarshalAs(UnmanagedType.U1)] internal byte statWithVersion;

    [MarshalAs(UnmanagedType.U1)] internal byte read;
    [MarshalAs(UnmanagedType.U1)] internal byte readWithIfMatch;
    [MarshalAs(UnmanagedType.U1)] internal byte readWithIfNoneMatch;
    [MarshalAs(UnmanagedType.U1)] internal byte readWithIfModifiedSince;
    [MarshalAs(UnmanagedType.U1)] internal byte readWithIfUnmodifiedSince;
    [MarshalAs(UnmanagedType.U1)] internal byte readWithOverrideCacheControl;
    [MarshalAs(UnmanagedType.U1)] internal byte readWithOverrideContentDisposition;
    [MarshalAs(UnmanagedType.U1)] internal byte readWithOverrideContentType;
    [MarshalAs(UnmanagedType.U1)] internal byte readWithVersion;

    [MarshalAs(UnmanagedType.U1)] internal byte write;
    [MarshalAs(UnmanagedType.U1)] internal byte writeCanMulti;
    [MarshalAs(UnmanagedType.U1)] internal byte writeCanEmpty;
    [MarshalAs(UnmanagedType.U1)] internal byte writeCanAppend;
    [MarshalAs(UnmanagedType.U1)] internal byte writeWithContentType;
    [MarshalAs(UnmanagedType.U1)] internal byte writeWithContentDisposition;
    [MarshalAs(UnmanagedType.U1)] internal byte writeWithContentEncoding;
    [MarshalAs(UnmanagedType.U1)] internal byte writeWithCacheControl;
    [MarshalAs(UnmanagedType.U1)] internal byte writeWithIfMatch;
    [MarshalAs(UnmanagedType.U1)] internal byte writeWithIfNoneMatch;
    [MarshalAs(UnmanagedType.U1)] internal byte writeWithIfNotExists;
    [MarshalAs(UnmanagedType.U1)] internal byte writeWithUserMetadata;

    internal nuint writeMultiMaxSize;
    internal nuint writeMultiMinSize;
    internal nuint writeTotalMaxSize;

    [MarshalAs(UnmanagedType.U1)] internal byte createDir;
    [MarshalAs(UnmanagedType.U1)] internal byte delete;
    [MarshalAs(UnmanagedType.U1)] internal byte deleteWithVersion;
    [MarshalAs(UnmanagedType.U1)] internal byte deleteWithRecursive;
    internal nuint deleteMaxSize;

    [MarshalAs(UnmanagedType.U1)] internal byte copy;
    [MarshalAs(UnmanagedType.U1)] internal byte copyWithIfNotExists;
    [MarshalAs(UnmanagedType.U1)] internal byte rename;

    [MarshalAs(UnmanagedType.U1)] internal byte list;
    [MarshalAs(UnmanagedType.U1)] internal byte listWithLimit;
    [MarshalAs(UnmanagedType.U1)] internal byte listWithStartAfter;
    [MarshalAs(UnmanagedType.U1)] internal byte listWithRecursive;
    [MarshalAs(UnmanagedType.U1)] internal byte listWithVersions;
    [MarshalAs(UnmanagedType.U1)] internal byte listWithDeleted;

    [MarshalAs(UnmanagedType.U1)] internal byte presign;
    [MarshalAs(UnmanagedType.U1)] internal byte presignRead;
    [MarshalAs(UnmanagedType.U1)] internal byte presignStat;
    [MarshalAs(UnmanagedType.U1)] internal byte presignWrite;
    [MarshalAs(UnmanagedType.U1)] internal byte presignDelete;

    [MarshalAs(UnmanagedType.U1)] internal byte shared;
}
