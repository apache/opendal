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
using OpenDAL.Interop.Result;

namespace OpenDAL;

internal partial class NativeMethods
{
    const string __DllName = "opendal_dotnet";

    #region Operator Lifecycle

    [LibraryImport(__DllName, EntryPoint = "operator_construct", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial OpenDALOperatorResult operator_construct(
        string scheme,
        IntPtr options
    );

    [LibraryImport(__DllName, EntryPoint = "constructor_option_build", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial OpenDALOptionsResult constructor_option_build(
        [In] string[] keys,
        [In] string[] values,
        nuint len
    );

    [LibraryImport(__DllName, EntryPoint = "constructor_option_free")]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial void constructor_option_free(IntPtr options);

    [LibraryImport(__DllName, EntryPoint = "operator_free")]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial void operator_free(IntPtr op);

    [LibraryImport(__DllName, EntryPoint = "operator_info_get")]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial OpenDALOperatorInfoResult operator_info_get(Operator op);

    [LibraryImport(__DllName, EntryPoint = "operator_info_free")]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial void operator_info_free(IntPtr info);

    [LibraryImport(__DllName, EntryPoint = "operator_duplicate")]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial OpenDALOperatorResult operator_duplicate(Operator op);

    #endregion

    #region Option Builders

    #region ReadOption

    [LibraryImport(__DllName, EntryPoint = "read_option_build", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial OpenDALOptionsResult read_option_build(
        [In] string[] keys,
        [In] string[] values,
        nuint len
    );

    [LibraryImport(__DllName, EntryPoint = "read_option_free")]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial void read_option_free(IntPtr options);

    #endregion

    #region WriteOption

    [LibraryImport(__DllName, EntryPoint = "write_option_build", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial OpenDALOptionsResult write_option_build(
        [In] string[] keys,
        [In] string[] values,
        nuint len
    );

    [LibraryImport(__DllName, EntryPoint = "write_option_free")]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial void write_option_free(IntPtr options);

    #endregion

    #region StatOption

    [LibraryImport(__DllName, EntryPoint = "stat_option_build", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial OpenDALOptionsResult stat_option_build(
        [In] string[] keys,
        [In] string[] values,
        nuint len
    );

    [LibraryImport(__DllName, EntryPoint = "stat_option_free")]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial void stat_option_free(IntPtr options);

    #endregion

    #region ListOption

    [LibraryImport(__DllName, EntryPoint = "list_option_build", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial OpenDALOptionsResult list_option_build(
        [In] string[] keys,
        [In] string[] values,
        nuint len
    );

    [LibraryImport(__DllName, EntryPoint = "list_option_free")]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial void list_option_free(IntPtr options);

    #endregion

    #endregion

    #region Layer

    [LibraryImport(__DllName, EntryPoint = "operator_layer_retry")]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial OpenDALOperatorResult operator_layer_retry(
        Operator op,
        [MarshalAs(UnmanagedType.I1)] bool jitter,
        float factor,
        ulong minDelayNanos,
        ulong maxDelayNanos,
        nuint maxTimes
    );

    [LibraryImport(__DllName, EntryPoint = "operator_layer_concurrent_limit")]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial OpenDALOperatorResult operator_layer_concurrent_limit(
        Operator op,
        nuint permits
    );

    [LibraryImport(__DllName, EntryPoint = "operator_layer_timeout")]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial OpenDALOperatorResult operator_layer_timeout(
        Operator op,
        ulong timeoutNanos,
        ulong ioTimeoutNanos
    );

    #endregion

    #region IO Operations

    #region Write

    [LibraryImport(__DllName, EntryPoint = "operator_write_with_options", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial OpenDALResult operator_write_with_options(
        Operator op,
        IntPtr executor,
        string path,
        [In] byte[] data,
        nuint len,
        IntPtr options
    );

    [LibraryImport(__DllName, EntryPoint = "operator_write_with_options_async", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static unsafe partial OpenDALResult operator_write_with_options_async(
        Operator op,
        IntPtr executor,
        string path,
        ByteBuffer data,
        IntPtr options,
        delegate* unmanaged[Cdecl]<long, OpenDALResult, void> callback,
        long context
    );

    #endregion

    #region Read

    [LibraryImport(__DllName, EntryPoint = "operator_read_with_options", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial OpenDALReadResult operator_read_with_options(
        Operator op,
        IntPtr executor,
        string path,
        IntPtr options
    );

    [LibraryImport(__DllName, EntryPoint = "operator_read_with_options_async", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static unsafe partial OpenDALResult operator_read_with_options_async(
        Operator op,
        IntPtr executor,
        string path,
        IntPtr options,
        delegate* unmanaged[Cdecl]<long, OpenDALReadResult, void> callback,
        long context
    );

    #endregion

    #region Stat

    [LibraryImport(__DllName, EntryPoint = "operator_stat_with_options", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial OpenDALMetadataResult operator_stat_with_options(
        Operator op,
        IntPtr executor,
        string path,
        IntPtr options
    );

    [LibraryImport(__DllName, EntryPoint = "operator_stat_with_options_async", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static unsafe partial OpenDALResult operator_stat_with_options_async(
        Operator op,
        IntPtr executor,
        string path,
        IntPtr options,
        delegate* unmanaged[Cdecl]<long, OpenDALMetadataResult, void> callback,
        long context
    );

    #endregion

    #region List

    [LibraryImport(__DllName, EntryPoint = "operator_list_with_options", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial OpenDALEntryListResult operator_list_with_options(
        Operator op,
        IntPtr executor,
        string path,
        IntPtr options
    );

    [LibraryImport(__DllName, EntryPoint = "operator_list_with_options_async", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static unsafe partial OpenDALResult operator_list_with_options_async(
        Operator op,
        IntPtr executor,
        string path,
        IntPtr options,
        delegate* unmanaged[Cdecl]<long, OpenDALEntryListResult, void> callback,
        long context
    );

    #endregion

    #region Delete

    [LibraryImport(__DllName, EntryPoint = "operator_delete", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial OpenDALResult operator_delete(
        Operator op,
        IntPtr executor,
        string path
    );

    [LibraryImport(__DllName, EntryPoint = "operator_delete_async", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static unsafe partial OpenDALResult operator_delete_async(
        Operator op,
        IntPtr executor,
        string path,
        delegate* unmanaged[Cdecl]<long, OpenDALResult, void> callback,
        long context
    );

    #endregion

    #region CreateDir

    [LibraryImport(__DllName, EntryPoint = "operator_create_dir", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial OpenDALResult operator_create_dir(
        Operator op,
        IntPtr executor,
        string path
    );

    [LibraryImport(__DllName, EntryPoint = "operator_create_dir_async", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static unsafe partial OpenDALResult operator_create_dir_async(
        Operator op,
        IntPtr executor,
        string path,
        delegate* unmanaged[Cdecl]<long, OpenDALResult, void> callback,
        long context
    );

    #endregion

    #region Copy

    [LibraryImport(__DllName, EntryPoint = "operator_copy", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial OpenDALResult operator_copy(
        Operator op,
        IntPtr executor,
        string sourcePath,
        string targetPath
    );

    [LibraryImport(__DllName, EntryPoint = "operator_copy_async", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static unsafe partial OpenDALResult operator_copy_async(
        Operator op,
        IntPtr executor,
        string sourcePath,
        string targetPath,
        delegate* unmanaged[Cdecl]<long, OpenDALResult, void> callback,
        long context
    );

    #endregion

    #region Rename

    [LibraryImport(__DllName, EntryPoint = "operator_rename", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial OpenDALResult operator_rename(
        Operator op,
        IntPtr executor,
        string sourcePath,
        string targetPath
    );

    [LibraryImport(__DllName, EntryPoint = "operator_rename_async", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static unsafe partial OpenDALResult operator_rename_async(
        Operator op,
        IntPtr executor,
        string sourcePath,
        string targetPath,
        delegate* unmanaged[Cdecl]<long, OpenDALResult, void> callback,
        long context
    );

    #endregion

    #region RemoveAll

    [LibraryImport(__DllName, EntryPoint = "operator_remove_all", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial OpenDALResult operator_remove_all(
        Operator op,
        IntPtr executor,
        string path
    );

    [LibraryImport(__DllName, EntryPoint = "operator_remove_all_async", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static unsafe partial OpenDALResult operator_remove_all_async(
        Operator op,
        IntPtr executor,
        string path,
        delegate* unmanaged[Cdecl]<long, OpenDALResult, void> callback,
        long context
    );

    #endregion

    #region Presign

    [LibraryImport(__DllName, EntryPoint = "operator_presign_read_async", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static unsafe partial OpenDALResult operator_presign_read_async(
        Operator op,
        IntPtr executor,
        string path,
        ulong expireNanos,
        delegate* unmanaged[Cdecl]<long, OpenDALPresignedRequestResult, void> callback,
        long context
    );

    [LibraryImport(__DllName, EntryPoint = "operator_presign_write_async", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static unsafe partial OpenDALResult operator_presign_write_async(
        Operator op,
        IntPtr executor,
        string path,
        ulong expireNanos,
        delegate* unmanaged[Cdecl]<long, OpenDALPresignedRequestResult, void> callback,
        long context
    );

    [LibraryImport(__DllName, EntryPoint = "operator_presign_stat_async", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static unsafe partial OpenDALResult operator_presign_stat_async(
        Operator op,
        IntPtr executor,
        string path,
        ulong expireNanos,
        delegate* unmanaged[Cdecl]<long, OpenDALPresignedRequestResult, void> callback,
        long context
    );

    [LibraryImport(__DllName, EntryPoint = "operator_presign_delete_async", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static unsafe partial OpenDALResult operator_presign_delete_async(
        Operator op,
        IntPtr executor,
        string path,
        ulong expireNanos,
        delegate* unmanaged[Cdecl]<long, OpenDALPresignedRequestResult, void> callback,
        long context
    );

    #endregion

    #region Streams

    [LibraryImport(__DllName, EntryPoint = "operator_input_stream_create", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial OpenDALOperatorResult operator_input_stream_create(
        Operator op,
        IntPtr executor,
        string path,
        IntPtr options
    );

    [LibraryImport(__DllName, EntryPoint = "operator_input_stream_read_next")]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial OpenDALReadResult operator_input_stream_read_next(IntPtr stream);

    [LibraryImport(__DllName, EntryPoint = "operator_input_stream_free")]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial void operator_input_stream_free(IntPtr stream);

    [LibraryImport(__DllName, EntryPoint = "operator_output_stream_create", StringMarshalling = StringMarshalling.Utf8)]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial OpenDALOperatorResult operator_output_stream_create(
        Operator op,
        IntPtr executor,
        string path,
        IntPtr options
    );

    [LibraryImport(__DllName, EntryPoint = "operator_output_stream_write")]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial OpenDALResult operator_output_stream_write(
        IntPtr stream,
        [In] byte[] data,
        nuint len
    );

    [LibraryImport(__DllName, EntryPoint = "operator_output_stream_flush")]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial OpenDALResult operator_output_stream_flush(IntPtr stream);

    [LibraryImport(__DllName, EntryPoint = "operator_output_stream_close")]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial OpenDALResult operator_output_stream_close(IntPtr stream);

    [LibraryImport(__DllName, EntryPoint = "operator_output_stream_free")]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial void operator_output_stream_free(IntPtr stream);

    #endregion

    #endregion

    #region Executor

    [LibraryImport(__DllName, EntryPoint = "executor_create")]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial OpenDALExecutorResult executor_create(nuint threads);

    [LibraryImport(__DllName, EntryPoint = "executor_free")]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial void executor_free(IntPtr executor);

    #endregion

    #region Result Release

    [LibraryImport(__DllName, EntryPoint = "opendal_error_release")]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial void opendal_error_release(OpenDALError error);

    [LibraryImport(__DllName, EntryPoint = "opendal_operator_info_result_release")]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial void opendal_operator_info_result_release(OpenDALOperatorInfoResult result);

    [LibraryImport(__DllName, EntryPoint = "opendal_metadata_result_release")]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial void opendal_metadata_result_release(OpenDALMetadataResult result);

    [LibraryImport(__DllName, EntryPoint = "opendal_entry_list_result_release")]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial void opendal_entry_list_result_release(OpenDALEntryListResult result);

    [LibraryImport(__DllName, EntryPoint = "opendal_read_result_release")]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial void opendal_read_result_release(OpenDALReadResult result);

    [LibraryImport(__DllName, EntryPoint = "opendal_presigned_request_result_release")]
    [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
    internal static partial void opendal_presigned_request_result_release(OpenDALPresignedRequestResult result);

    #endregion

}
