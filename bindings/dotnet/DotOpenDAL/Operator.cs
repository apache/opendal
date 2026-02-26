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
using System.Runtime.InteropServices;

namespace DotOpenDAL;

/// <summary>
/// Managed wrapper over an OpenDAL native operator handle.
/// </summary>
public partial class Operator : SafeHandle
{
    /// <summary>
    /// Gets the underlying native operator pointer.
    /// </summary>
    public IntPtr Op => DangerousGetHandle();

    /// <summary>
    /// Gets whether the native handle is invalid.
    /// </summary>
    public override bool IsInvalid => handle == IntPtr.Zero;

    /// <summary>
    /// Creates an operator for the specified backend scheme and options.
    /// </summary>
    /// <param name="scheme">Backend scheme, such as <c>fs</c> or <c>memory</c>.</param>
    /// <param name="options">Backend-specific key/value options.</param>
    /// <exception cref="ArgumentException"><paramref name="scheme"/> is null, empty, or whitespace.</exception>
    /// <exception cref="OpenDALException">Native operator construction fails.</exception>
    public unsafe Operator(string scheme, IReadOnlyDictionary<string, string>? options = null) : base(IntPtr.Zero, true)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(scheme);

        var optionCount = options?.Count ?? 0;
        var keyPointers = optionCount > 0 ? new IntPtr[optionCount] : Array.Empty<IntPtr>();
        var valuePointers = optionCount > 0 ? new IntPtr[optionCount] : Array.Empty<IntPtr>();

        OpenDALIntPtrResult result;
        try
        {
            if (optionCount > 0)
            {
                var index = 0;
                foreach (var option in options!)
                {
                    keyPointers[index] = Marshal.StringToCoTaskMemUTF8(option.Key);
                    valuePointers[index] = Marshal.StringToCoTaskMemUTF8(option.Value);
                    index++;
                }

                fixed (IntPtr* keyPtr = keyPointers)
                fixed (IntPtr* valuePtr = valuePointers)
                {
                    result = NativeMethods.operator_construct(scheme, keyPtr, valuePtr, (nuint)optionCount);
                }
            }
            else
            {
                result = NativeMethods.operator_construct(scheme, null, null, 0);
            }
        }
        finally
        {
            for (var index = 0; index < optionCount; index++)
            {
                if (keyPointers[index] != IntPtr.Zero)
                {
                    Marshal.FreeCoTaskMem(keyPointers[index]);
                }

                if (valuePointers[index] != IntPtr.Zero)
                {
                    Marshal.FreeCoTaskMem(valuePointers[index]);
                }
            }
        }

        if (result.Ptr == IntPtr.Zero)
        {
            throw new OpenDALException(result.Error);
        }

        SetHandle(result.Ptr);
    }

    /// <summary>
    /// Writes the specified content to a path.
    /// </summary>
    /// <param name="path">Target path in the configured backend.</param>
    /// <param name="content">Bytes to write.</param>
    /// <exception cref="ObjectDisposedException">The operator has been disposed.</exception>
    /// <exception cref="OpenDALException">Native write fails.</exception>
    public void Write(string path, byte[] content)
    {
        ArgumentNullException.ThrowIfNull(content);
        ObjectDisposedException.ThrowIf(IsInvalid, this);

        unsafe
        {
            fixed (byte* ptr = content)
            {
                var result = NativeMethods.operator_write(this, path, ptr, (nuint)content.Length);
                if (result.Error.IsError)
                {
                    throw new OpenDALException(result.Error);
                }
            }
        }
    }

    /// <summary>
    /// Writes the specified content to a path asynchronously.
    /// </summary>
    /// <param name="path">Target path in the configured backend.</param>
    /// <param name="content">Bytes to write.</param>
    /// <param name="cancellationToken">Cancellation token for the managed task.</param>
    /// <returns>A task that completes when the native callback reports completion.</returns>
    /// <exception cref="ObjectDisposedException">The operator has been disposed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> is already canceled.</exception>
    /// <exception cref="OpenDALException">Native write submission fails immediately.</exception>
    public unsafe Task WriteAsync(string path, byte[] content, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(content);
        ObjectDisposedException.ThrowIf(IsInvalid, this);
        cancellationToken.ThrowIfCancellationRequested();

        var state = new WriteAsyncState();
        var context = AsyncStateRegistry.Register(state);

        OpenDALResult result;
        fixed (byte* ptr = content)
        {
            result = NativeMethods.operator_write_async(
                this,
                path,
                ptr,
                (nuint)content.Length,
                &OnWriteCompleted,
                new IntPtr(context));
        }

        if (result.Error.IsError)
        {
            AsyncStateRegistry.Unregister(context);
            throw new OpenDALException(result.Error);
        }

        if (cancellationToken.CanBeCanceled)
        {
            state.CancellationRegistration = cancellationToken.Register(static value =>
            {
                var current = (WriteAsyncState)value!;
                current.Completion.TrySetCanceled();
            }, state);
        }

        return state.Completion.Task;
    }

    /// <summary>
    /// Reads all bytes from a path.
    /// </summary>
    /// <param name="path">Source path in the configured backend.</param>
    /// <returns>The content bytes.</returns>
    /// <exception cref="ObjectDisposedException">The operator has been disposed.</exception>
    /// <exception cref="OpenDALException">Native read fails.</exception>
    public byte[] Read(string path)
    {
        ObjectDisposedException.ThrowIf(IsInvalid, this);

        var result = NativeMethods.operator_read(this, path);
        if (result.Error.IsError)
        {
            throw new OpenDALException(result.Error);
        }

        try
        {
            if (result.Buffer.Data == IntPtr.Zero || result.Buffer.Len == 0)
            {
                return Array.Empty<byte>();
            }

            var size = checked((int)result.Buffer.Len);
            var managed = new byte[size];
            Marshal.Copy(result.Buffer.Data, managed, 0, size);
            return managed;
        }
        finally
        {
            result.Buffer.Release();
        }
    }

    /// <summary>
    /// Reads all bytes from a path asynchronously.
    /// </summary>
    /// <param name="path">Source path in the configured backend.</param>
    /// <param name="cancellationToken">Cancellation token for the managed task.</param>
    /// <returns>A task that resolves with the read content.</returns>
    /// <exception cref="ObjectDisposedException">The operator has been disposed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> is already canceled.</exception>
    /// <exception cref="OpenDALException">Native read submission fails immediately.</exception>
    public unsafe Task<byte[]> ReadAsync(string path, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(IsInvalid, this);
        cancellationToken.ThrowIfCancellationRequested();

        var state = new ReadAsyncState();
        var context = AsyncStateRegistry.Register(state);

        var result = NativeMethods.operator_read_async(
            this,
            path,
            &OnReadCompleted,
            new IntPtr(context));

        if (result.Error.IsError)
        {
            AsyncStateRegistry.Unregister(context);
            throw new OpenDALException(result.Error);
        }

        if (cancellationToken.CanBeCanceled)
        {
            state.CancellationRegistration = cancellationToken.Register(static value =>
            {
                var current = (ReadAsyncState)value!;
                current.Completion.TrySetCanceled();
            }, state);
        }

        return state.Completion.Task;
    }

    /// <summary>
    /// Releases the native operator handle.
    /// </summary>
    /// <returns><see langword="true"/> after the handle has been released.</returns>
    protected override bool ReleaseHandle()
    {
        NativeMethods.operator_free(handle);
        return true;
    }

    /// <summary>
    /// Native callback invoked when an asynchronous write operation finishes.
    /// </summary>
    /// <param name="context">Opaque async state context previously registered by <see cref="AsyncStateRegistry"/>.</param>
    /// <param name="result">Write completion result returned by the native layer.</param>
    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static void OnWriteCompleted(IntPtr context, OpenDALResult result)
    {
        if (!AsyncStateRegistry.TryTake<WriteAsyncState>(context, out var state))
        {
            result.Error.Release();
            return;
        }

        try
        {
            state.CancellationRegistration.Dispose();

            if (result.Error.IsError)
            {
                state.Completion.TrySetException(new OpenDALException(result.Error));
                return;
            }

            state.Completion.TrySetResult();
        }
        catch (Exception ex)
        {
            state.Completion.TrySetException(ex);
        }
    }

    /// <summary>
    /// Native callback invoked when an asynchronous read operation finishes.
    /// </summary>
    /// <param name="context">Opaque async state context previously registered by <see cref="AsyncStateRegistry"/>.</param>
    /// <param name="result">Read completion result returned by the native layer, including byte buffer payload.</param>
    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static void OnReadCompleted(IntPtr context, OpenDALByteBufferResult result)
    {
        if (!AsyncStateRegistry.TryTake<ReadAsyncState>(context, out var state))
        {
            result.Error.Release();
            result.Buffer.Release();
            return;
        }

        try
        {
            state.CancellationRegistration.Dispose();

            if (result.Error.IsError)
            {
                state.Completion.TrySetException(new OpenDALException(result.Error));
                return;
            }

            if (result.Buffer.Data == IntPtr.Zero || result.Buffer.Len == 0)
            {
                state.Completion.TrySetResult(Array.Empty<byte>());
                return;
            }

            var size = checked((int)result.Buffer.Len);
            var managed = new byte[size];
            Marshal.Copy(result.Buffer.Data, managed, 0, size);
            state.Completion.TrySetResult(managed);
        }
        catch (Exception ex)
        {
            state.Completion.TrySetException(ex);
        }
        finally
        {
            result.Buffer.Release();
        }
    }

}