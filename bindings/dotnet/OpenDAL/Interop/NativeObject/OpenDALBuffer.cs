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

[StructLayout(LayoutKind.Sequential)]
/// <summary>
/// Read payload still owned by the native side.
/// </summary>
/// <remarks>
/// Holds the native buffer itself rather than a flattened copy, so
/// <see cref="ToManagedBytes"/> can copy straight into its final array. Released
/// through the owning result's release call, not from here.
/// </remarks>
internal struct OpenDALBuffer
{
    /// <summary>
    /// Opaque handle to the native buffer, or zero when there is no payload.
    /// </summary>
    public IntPtr Handle;

    /// <summary>
    /// Total readable bytes across every chunk.
    /// </summary>
    public nuint Len;

    /// <summary>
    /// Copies the payload into a new managed array.
    /// </summary>
    /// <returns>The payload, or an empty array when there is none.</returns>
    public readonly unsafe byte[] ToManagedBytes()
    {
        if (Handle == IntPtr.Zero || Len == 0)
        {
            return Array.Empty<byte>();
        }

        var size = checked((int)Len);
        var managed = GC.AllocateUninitializedArray<byte>(size);
        nuint written;
        fixed (byte* destination = managed)
        {
            written = NativeMethods.buffer_copy_to(Handle, destination, (nuint)size);
        }

        // The array is uninitialized, so a short copy would hand back whatever the
        // heap held rather than the payload.
        if (written != (nuint)size)
        {
            throw new InvalidOperationException(
                $"Native buffer reported {Len} bytes but produced {written}.");
        }

        return managed;
    }
}
