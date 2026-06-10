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
/// Converts native entry list payloads into managed <see cref="Entry"/> collections.
/// </summary>
internal static class EntryMarshaller
{
    /// <summary>
    /// Reads a native entry list pointer and converts it into managed entries.
    /// </summary>
    /// <param name="ptr">Pointer to a native <c>opendal_entry_list</c> payload.</param>
    /// <returns>A read-only collection of managed <see cref="Entry"/> values.</returns>
    /// <exception cref="InvalidOperationException">Thrown when native list size exceeds <see cref="int.MaxValue"/>.</exception>
    internal static unsafe IReadOnlyList<Entry> ToEntries(IntPtr ptr)
    {
        if (ptr == IntPtr.Zero)
        {
            return Array.Empty<Entry>();
        }

        var payload = Unsafe.Read<OpenDALEntryList>((void*)ptr);

        if (payload.Len > int.MaxValue)
        {
            throw new InvalidOperationException("Entry list too large");
        }

        var count = (int)payload.Len;
        var results = new List<Entry>(count);

        if (payload.Entries == IntPtr.Zero)
        {
            return results;
        }

        var entryPointers = new ReadOnlySpan<IntPtr>((void*)payload.Entries, count);
        for (var index = 0; index < count; index++)
        {
            var entryPtr = entryPointers[index];
            if (entryPtr == IntPtr.Zero)
            {
                continue;
            }

            var entryPayload = Unsafe.Read<OpenDALEntry>((void*)entryPtr);
            if (entryPayload.Metadata == IntPtr.Zero)
            {
                continue;
            }

            var path = Utilities.ReadUtf8(entryPayload.Path);
            var metadata = MetadataMarshaller.ToMetadata(entryPayload.Metadata);
            results.Add(new Entry(path, metadata));
        }

        return results;
    }
}