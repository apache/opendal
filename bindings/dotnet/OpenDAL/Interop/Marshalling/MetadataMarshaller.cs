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

using System.Runtime.CompilerServices;
using OpenDAL.Interop.NativeObject;

namespace OpenDAL.Interop.Marshalling;

/// <summary>
/// Converts native metadata payloads into managed <see cref="Metadata"/> instances.
/// </summary>
internal static class MetadataMarshaller
{
    /// <summary>
    /// Reads a native metadata pointer and converts it to managed metadata.
    /// </summary>
    /// <param name="ptr">Pointer to a native <c>opendal_metadata</c> payload.</param>
    /// <returns>A managed <see cref="Metadata"/> instance.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the pointer is null.</exception>
    internal static unsafe Metadata ToMetadata(IntPtr ptr)
    {
        if (ptr == IntPtr.Zero)
        {
            throw new InvalidOperationException("native call returned null metadata pointer");
        }

        var payload = Unsafe.Read<OpenDALMetadata>((void*)ptr);
        return ToMetadata(payload);
    }

    /// <summary>
    /// Converts a native metadata payload structure into managed metadata.
    /// </summary>
    /// <param name="payload">Native metadata payload copied from unmanaged memory.</param>
    /// <returns>A managed <see cref="Metadata"/> instance.</returns>
    internal static Metadata ToMetadata(OpenDALMetadata payload)
    {
        var mode = payload.Mode switch
        {
            0 => EntryMode.File,
            1 => EntryMode.Dir,
            _ => EntryMode.Unknown,
        };

        bool? isCurrent = payload.IsCurrentHasValue != 0 ? payload.IsCurrent != 0 : null;

        DateTimeOffset? lastModified = null;
        if (payload.LastModifiedHasValue != 0)
        {
            lastModified = DateTimeOffset.FromUnixTimeSeconds(payload.LastModifiedSecond)
                .AddTicks(payload.LastModifiedNanosecond / 100);
        }

        IReadOnlyDictionary<string, string>? userMetadata = null;
        if (payload.UserMetadataHasValue != 0)
        {
            userMetadata = Utilities.ReadStringPairs(
                payload.UserMetadataKeys,
                payload.UserMetadataValues,
                payload.UserMetadataLen,
                StringComparer.Ordinal
            );
        }

        return new Metadata(
            mode,
            isCurrent,
            payload.IsDeleted != 0,
            Utilities.ReadNullableUtf8(payload.CacheControl),
            payload.ContentLength,
            Utilities.ReadNullableUtf8(payload.ContentMd5),
            Utilities.ReadNullableUtf8(payload.ContentType),
            Utilities.ReadNullableUtf8(payload.ContentEncoding),
            lastModified,
            Utilities.ReadNullableUtf8(payload.ETag),
            Utilities.ReadNullableUtf8(payload.ContentDisposition),
            Utilities.ReadNullableUtf8(payload.Version),
            userMetadata
        );
    }
}
