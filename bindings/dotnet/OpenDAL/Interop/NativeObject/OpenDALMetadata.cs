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

namespace OpenDAL.Interop.NativeObject;

/// <summary>
/// Mirror of the native <c>OpendalMetadata</c> struct. Fields follow the
/// accessor order of <c>opendal::Metadata</c>.
/// </summary>
[StructLayout(LayoutKind.Sequential)]
internal struct OpenDALMetadata
{
    public int Mode;

    public byte IsCurrentHasValue;

    public byte IsCurrent;

    public byte IsDeleted;

    public IntPtr CacheControl;

    public ulong ContentLength;

    public IntPtr ContentMd5;

    public IntPtr ContentType;

    public IntPtr ContentEncoding;

    public byte LastModifiedHasValue;

    public long LastModifiedSecond;

    public int LastModifiedNanosecond;

    public IntPtr ETag;

    public IntPtr ContentDisposition;

    public IntPtr Version;

    public byte UserMetadataHasValue;

    public IntPtr UserMetadataKeys;

    public IntPtr UserMetadataValues;

    public nuint UserMetadataLen;
}
