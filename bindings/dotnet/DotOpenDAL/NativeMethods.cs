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
using System.Runtime.CompilerServices;

namespace DotOpenDAL;

internal partial class NativeMethods
{
    const string __DllName = "opendal_dotnet";

    [LibraryImport(__DllName, EntryPoint = "operator_construct", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static unsafe partial OpenDALIntPtrResult operator_construct(
        string scheme,
        IntPtr* keys,
        IntPtr* values,
        nuint len);

    [LibraryImport(__DllName, EntryPoint = "operator_free")]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial void operator_free(IntPtr op);

    [LibraryImport(__DllName, EntryPoint = "operator_write", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static unsafe partial OpenDALResult operator_write(Operator op, string path, byte* data, nuint len);

    [LibraryImport(__DllName, EntryPoint = "operator_read", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial OpenDALByteBufferResult operator_read(Operator op, string path);

    [LibraryImport(__DllName, EntryPoint = "operator_write_async", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static unsafe partial OpenDALResult operator_write_async(
        Operator op,
        string path,
        byte* data,
        nuint len,
        delegate* unmanaged[Cdecl]<IntPtr, OpenDALResult, void> callback,
        IntPtr context);

    [LibraryImport(__DllName, EntryPoint = "operator_read_async", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static unsafe partial OpenDALResult operator_read_async(
        Operator op,
        string path,
        delegate* unmanaged[Cdecl]<IntPtr, OpenDALByteBufferResult, void> callback,
        IntPtr context);

    [LibraryImport(__DllName, EntryPoint = "message_free")]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial void message_free(IntPtr message);

    [LibraryImport(__DllName, EntryPoint = "buffer_free")]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial void buffer_free(IntPtr data, nuint len, nuint capacity);
}