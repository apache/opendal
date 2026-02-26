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

namespace DotOpenDAL;

[StructLayout(LayoutKind.Sequential)]
/// <summary>
/// FFI representation of a Rust byte buffer.
/// </summary>
public struct ByteBuffer
{
    /// <summary>
    /// Pointer to the first byte in unmanaged memory.
    /// </summary>
    public IntPtr Data;

    /// <summary>
    /// Number of valid bytes in <see cref="Data"/>.
    /// </summary>
    public nuint Len;

    /// <summary>
    /// Total allocated capacity in bytes.
    /// </summary>
    public nuint Capacity;

    /// <summary>
    /// Releases the unmanaged buffer allocated by the Rust binding.
    /// </summary>
    public readonly void Release()
    {
        if (Data == IntPtr.Zero)
        {
            return;
        }

        if (Capacity == 0 || Capacity < Len)
        {
            return;
        }

        NativeMethods.buffer_free(Data, Len, Capacity);
    }
}