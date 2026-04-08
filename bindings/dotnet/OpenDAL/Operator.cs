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
using OpenDAL.Interop.Result.Abstractions;
using OpenDAL.Layer.Abstractions;
using OpenDAL.Options;
using OpenDAL.Options.Abstractions;
using OpenDAL.ServiceConfig.Abstractions;
using System.Diagnostics.CodeAnalysis;

namespace OpenDAL;

/// <summary>
/// Managed wrapper over an OpenDAL native operator handle.
/// </summary>
public partial class Operator : SafeHandle
{
    private Lazy<OperatorInfo> info;

    private Operator() : base(IntPtr.Zero, true)
    {
        info = CreateInfoLazy();
    }

    private Operator(IntPtr nativeHandle) : this()
    {
        if (nativeHandle == IntPtr.Zero)
        {
            throw new ArgumentException("Native operator handle must not be zero.", nameof(nativeHandle));
        }

        SetHandle(nativeHandle);
    }

    /// <summary>
    /// Gets metadata of this operator.
    /// </summary>
    /// <exception cref="ObjectDisposedException">The operator has been disposed.</exception>
    /// <exception cref="OpenDALException">Native operator info retrieval fails.</exception>
    public OperatorInfo Info
    {
        get
        {
            ObjectDisposedException.ThrowIf(IsInvalid, this);
            return info.Value;
        }
    }

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
    /// <remarks>
    /// Available scheme names are defined by OpenDAL.
    /// See <see href="https://docs.rs/opendal/latest/opendal/enum.Scheme.html">OpenDAL Scheme documentation</see>
    /// for supported backends and their related configuration options.
    /// </remarks>
    /// <param name="scheme">Name of the backend service, such as <c>fs</c> or <c>memory</c>.</param>
    /// <param name="options">Key/value options used to configure the selected backend service.</param>
    /// <exception cref="ArgumentException"><paramref name="scheme"/> is null, empty, or whitespace.</exception>
    /// <exception cref="OpenDALException">Native operator construction fails.</exception>
    public Operator(string scheme, IReadOnlyDictionary<string, string>? options = null) : base(IntPtr.Zero, true)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(scheme);
        info = CreateInfoLazy();

        using var nativeOptionsHandle = CreateConstructorOptionsHandle(options);
        var result = NativeMethods.operator_construct(scheme, GetOptionsHandle(nativeOptionsHandle));
        SetHandle(ToValueOrThrowAndRelease<IntPtr, OpenDALOperatorResult>(result));
    }

    /// <summary>
    /// Creates an operator from a typed service configuration.
    /// </summary>
    /// <remarks>
    /// This overload converts <paramref name="config"/> into backend key/value options internally,
    /// then creates the same native operator as <see cref="Operator(string, IReadOnlyDictionary{string, string}?)"/>.
    /// </remarks>
    /// <param name="config">Typed service configuration for the target backend service.</param>
    /// <exception cref="ArgumentNullException"><paramref name="config"/> is null.</exception>
    /// <exception cref="OpenDALException">Native operator construction fails.</exception>
    public Operator(IServiceConfig config) : this(
        config?.Scheme ?? throw new ArgumentNullException(nameof(config)),
        config.ToOptions())
    {
    }

    /// <summary>
    /// Applies the specified layer and returns a new operator instance.
    /// </summary>
    /// <param name="layer">Layer to apply.</param>
    /// <returns>A new operator with the layer applied.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="layer"/> is null.</exception>
    /// <exception cref="ObjectDisposedException">The operator has been disposed.</exception>
    /// <exception cref="OpenDALException">Native layer application fails.</exception>
    public Operator WithLayer(ILayer layer)
    {
        ArgumentNullException.ThrowIfNull(layer);
        ObjectDisposedException.ThrowIf(IsInvalid, this);
        return layer.Apply(this);
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
        Write(path, content, options: null, executor: null);
    }

    /// <summary>
    /// Writes the specified content to a path with write options.
    /// </summary>
    /// <param name="path">Target path in the configured backend.</param>
    /// <param name="content">Bytes to write.</param>
    /// <param name="options">Additional write options.</param>
    public void Write(string path, byte[] content, WriteOptions options)
    {
        Write(path, content, (WriteOptions?)options, executor: null);
    }

    /// <summary>
    /// Writes the specified content to a path using the provided executor.
    /// </summary>
    /// <param name="path">Target path in the configured backend.</param>
    /// <param name="content">Bytes to write.</param>
    /// <param name="executor">Executor used for this operation, or <see langword="null"/> to use default executor.</param>
    /// <exception cref="ObjectDisposedException">The operator or executor has been disposed.</exception>
    /// <exception cref="OpenDALException">Native write fails.</exception>
    public void Write(string path, byte[] content, Executor? executor)
    {
        Write(path, content, options: null, executor);
    }

    /// <summary>
    /// Writes the specified content to a path with write options using the provided executor.
    /// </summary>
    /// <param name="path">Target path in the configured backend.</param>
    /// <param name="content">Bytes to write.</param>
    /// <param name="options">Additional write options.</param>
    /// <param name="executor">Executor used for this operation, or <see langword="null"/> to use default executor.</param>
    public void Write(string path, byte[] content, WriteOptions? options, Executor? executor)
    {
        ArgumentNullException.ThrowIfNull(content);
        ObjectDisposedException.ThrowIf(IsInvalid, this);
        var executorHandle = GetExecutorHandle(executor);

        using var nativeOptionsHandle = options?.BuildNativeOptionsHandle();

        OpenDALResult result = NativeMethods.operator_write_with_options(
            this,
            executorHandle,
            path,
            content,
            (nuint)content.Length,
            GetOptionsHandle(nativeOptionsHandle)
        );

        ThrowIfErrorAndRelease(result);
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
    public Task WriteAsync(string path, byte[] content, CancellationToken cancellationToken = default)
    {
        return WriteAsync(path, content, options: null, executor: null, cancellationToken);
    }

    /// <summary>
    /// Writes the specified content to a path asynchronously with write options.
    /// </summary>
    /// <param name="path">Target path in the configured backend.</param>
    /// <param name="content">Bytes to write.</param>
    /// <param name="options">Additional write options.</param>
    /// <param name="cancellationToken">Cancellation token for the managed task.</param>
    /// <returns>A task that completes when the native callback reports completion.</returns>
    public Task WriteAsync(
        string path,
        byte[] content,
        WriteOptions options,
        CancellationToken cancellationToken = default)
    {
        return WriteAsync(path, content, (WriteOptions?)options, executor: null, cancellationToken);
    }

    /// <summary>
    /// Writes the specified content to a path asynchronously using the provided executor.
    /// </summary>
    /// <param name="path">Target path in the configured backend.</param>
    /// <param name="content">Bytes to write.</param>
    /// <param name="executor">Executor used for this operation, or <see langword="null"/> to use default executor.</param>
    /// <param name="cancellationToken">Cancellation token for the managed task.</param>
    /// <returns>A task that completes when the native callback reports completion.</returns>
    /// <exception cref="ObjectDisposedException">The operator or executor has been disposed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> is already canceled.</exception>
    /// <exception cref="OpenDALException">Native write submission fails immediately.</exception>
    public Task WriteAsync(
        string path,
        byte[] content,
        Executor? executor,
        CancellationToken cancellationToken = default)
    {
        return WriteAsync(path, content, options: null, executor, cancellationToken);
    }

    /// <summary>
    /// Writes the specified content to a path asynchronously with optional write options and executor.
    /// </summary>
    /// <param name="path">Target path in the configured backend.</param>
    /// <param name="content">Bytes to write.</param>
    /// <param name="options">Additional write options, or <see langword="null"/> for default behavior.</param>
    /// <param name="executor">Executor used for this operation, or <see langword="null"/> to use default executor.</param>
    /// <param name="cancellationToken">Cancellation token for the managed task.</param>
    /// <returns>A task that completes when the native callback reports completion.</returns>
    public Task WriteAsync(
        string path,
        byte[] content,
        WriteOptions? options,
        Executor? executor,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(content);
        ObjectDisposedException.ThrowIf(IsInvalid, this);
        var executorHandle = GetExecutorHandle(executor);

        return SubmitAsyncOperation<bool, WriteOptions>(options, SubmitWriteAsync, cancellationToken);

        OpenDALResult SubmitWriteAsync(long context, IntPtr optionsHandle)
        {
            unsafe
            {
                fixed (byte* ptr = content)
                {
                    var buffer = new ByteBuffer
                    {
                        Data = (IntPtr)ptr,
                        Len = (nuint)content.Length,
                        Capacity = (nuint)content.Length,
                    };

                    return NativeMethods.operator_write_with_options_async(
                        this,
                        executorHandle,
                        path,
                        buffer,
                        optionsHandle,
                        &OnWriteCompleted,
                        context
                    );
                }
            }
        }
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
        return Read(path, options: null, executor: null);
    }

    /// <summary>
    /// Reads bytes from a path with read options.
    /// </summary>
    /// <param name="path">Source path in the configured backend.</param>
    /// <param name="options">Additional read options.</param>
    /// <returns>The content bytes.</returns>
    public byte[] Read(string path, ReadOptions options)
    {
        return Read(path, options, executor: null);
    }

    /// <summary>
    /// Reads all bytes from a path using the provided executor.
    /// </summary>
    /// <param name="path">Source path in the configured backend.</param>
    /// <param name="executor">Executor used for this operation, or <see langword="null"/> to use default executor.</param>
    /// <returns>The content bytes.</returns>
    /// <exception cref="ObjectDisposedException">The operator or executor has been disposed.</exception>
    /// <exception cref="OpenDALException">Native read fails.</exception>
    public byte[] Read(string path, Executor? executor)
    {
        return Read(path, options: null, executor);
    }

    /// <summary>
    /// Reads bytes from a path with read options using the provided executor.
    /// </summary>
    /// <param name="path">Source path in the configured backend.</param>
    /// <param name="options">Additional read options.</param>
    /// <param name="executor">Executor used for this operation, or <see langword="null"/> to use default executor.</param>
    /// <returns>The content bytes.</returns>
    public byte[] Read(string path, ReadOptions? options, Executor? executor)
    {
        ObjectDisposedException.ThrowIf(IsInvalid, this);
        var executorHandle = GetExecutorHandle(executor);

        OpenDALReadResult result;
        using var nativeOptionsHandle = options?.BuildNativeOptionsHandle();
        result = NativeMethods.operator_read_with_options(this, executorHandle, path, GetOptionsHandle(nativeOptionsHandle));

        return ToValueOrThrowAndRelease<byte[], OpenDALReadResult>(result);
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
    public Task<byte[]> ReadAsync(string path, CancellationToken cancellationToken = default)
    {
        return ReadAsync(path, options: null, executor: null, cancellationToken);
    }

    /// <summary>
    /// Reads bytes from a path asynchronously with read options.
    /// </summary>
    /// <param name="path">Source path in the configured backend.</param>
    /// <param name="options">Additional read options.</param>
    /// <param name="cancellationToken">Cancellation token for the managed task.</param>
    /// <returns>A task that resolves with the read content.</returns>
    public Task<byte[]> ReadAsync(string path, ReadOptions options, CancellationToken cancellationToken = default)
    {
        return ReadAsync(path, options, executor: null, cancellationToken);
    }

    /// <summary>
    /// Reads all bytes from a path asynchronously using the provided executor.
    /// </summary>
    /// <param name="path">Source path in the configured backend.</param>
    /// <param name="executor">Executor used for this operation, or <see langword="null"/> to use default executor.</param>
    /// <param name="cancellationToken">Cancellation token for the managed task.</param>
    /// <returns>A task that resolves with the read content.</returns>
    /// <exception cref="ObjectDisposedException">The operator or executor has been disposed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> is already canceled.</exception>
    /// <exception cref="OpenDALException">Native read submission fails immediately.</exception>
    public Task<byte[]> ReadAsync(string path, Executor? executor, CancellationToken cancellationToken = default)
    {
        return ReadAsync(path, options: null, executor, cancellationToken);
    }

    /// <summary>
    /// Reads bytes from a path asynchronously with optional read options and executor.
    /// </summary>
    /// <param name="path">Source path in the configured backend.</param>
    /// <param name="options">Additional read options, or <see langword="null"/> for default behavior.</param>
    /// <param name="executor">Executor used for this operation, or <see langword="null"/> to use default executor.</param>
    /// <param name="cancellationToken">Cancellation token for the managed task.</param>
    /// <returns>A task that resolves with the read content.</returns>
    public Task<byte[]> ReadAsync(
        string path,
        ReadOptions? options,
        Executor? executor,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(IsInvalid, this);
        var executorHandle = GetExecutorHandle(executor);

        return SubmitAsyncOperation<byte[], ReadOptions>(options, SubmitReadAsync, cancellationToken);

        OpenDALResult SubmitReadAsync(long context, IntPtr optionsHandle)
        {
            unsafe
            {
                return NativeMethods.operator_read_with_options_async(
                    this,
                    executorHandle,
                    path,
                    optionsHandle,
                    &OnReadCompleted,
                    context
                );
            }
        }
    }

    /// <summary>
    /// Gets metadata for the specified path.
    /// </summary>
    /// <param name="path">Target path in the configured backend.</param>
    /// <param name="options">Additional stat options.</param>
    /// <returns>Metadata of the target path.</returns>
    public Metadata Stat(string path, StatOptions? options = null)
    {
        return Stat(path, options, executor: null);
    }

    /// <summary>
    /// Gets metadata for the specified path using the provided executor.
    /// </summary>
    /// <param name="path">Target path in the configured backend.</param>
    /// <param name="options">Additional stat options.</param>
    /// <param name="executor">Executor used for this operation, or <see langword="null"/> to use default executor.</param>
    /// <returns>Metadata of the target path.</returns>
    public Metadata Stat(string path, StatOptions? options, Executor? executor)
    {
        ObjectDisposedException.ThrowIf(IsInvalid, this);
        var executorHandle = GetExecutorHandle(executor);

        OpenDALMetadataResult result;
        using var nativeOptionsHandle = options?.BuildNativeOptionsHandle();
        result = NativeMethods.operator_stat_with_options(this, executorHandle, path, GetOptionsHandle(nativeOptionsHandle));

        return ToValueOrThrowAndRelease<Metadata, OpenDALMetadataResult>(result);
    }

    /// <summary>
    /// Gets metadata for the specified path asynchronously.
    /// </summary>
    /// <param name="path">Target path in the configured backend.</param>
    /// <param name="options">Additional stat options.</param>
    /// <param name="cancellationToken">Cancellation token for the managed task.</param>
    /// <returns>A task that resolves with metadata.</returns>
    public Task<Metadata> StatAsync(
        string path,
        StatOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        return StatAsync(path, options, executor: null, cancellationToken);
    }

    /// <summary>
    /// Gets metadata for the specified path asynchronously using the provided executor.
    /// </summary>
    /// <param name="path">Target path in the configured backend.</param>
    /// <param name="options">Additional stat options.</param>
    /// <param name="executor">Executor used for this operation, or <see langword="null"/> to use default executor.</param>
    /// <param name="cancellationToken">Cancellation token for the managed task.</param>
    /// <returns>A task that resolves with metadata.</returns>
    public Task<Metadata> StatAsync(
        string path,
        StatOptions? options,
        Executor? executor,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(IsInvalid, this);
        var executorHandle = GetExecutorHandle(executor);

        return SubmitAsyncOperation<Metadata, StatOptions>(options, SubmitStatAsync, cancellationToken);

        OpenDALResult SubmitStatAsync(long context, IntPtr optionsHandle)
        {
            unsafe
            {
                return NativeMethods.operator_stat_with_options_async(
                    this,
                    executorHandle,
                    path,
                    optionsHandle,
                    &OnStatCompleted,
                    context
                );
            }
        }
    }

    /// <summary>
    /// Lists entries under the specified path.
    /// </summary>
    /// <param name="path">Target path in the configured backend.</param>
    /// <param name="options">Additional list options.</param>
    /// <returns>Listed entries.</returns>
    public IReadOnlyList<Entry> List(string path, ListOptions? options = null)
    {
        return List(path, options, executor: null);
    }

    /// <summary>
    /// Lists entries under the specified path using the provided executor.
    /// </summary>
    /// <param name="path">Target path in the configured backend.</param>
    /// <param name="options">Additional list options.</param>
    /// <param name="executor">Executor used for this operation, or <see langword="null"/> to use default executor.</param>
    /// <returns>Listed entries.</returns>
    public IReadOnlyList<Entry> List(string path, ListOptions? options, Executor? executor)
    {
        ObjectDisposedException.ThrowIf(IsInvalid, this);
        var executorHandle = GetExecutorHandle(executor);

        OpenDALEntryListResult result;
        using var nativeOptionsHandle = options?.BuildNativeOptionsHandle();
        result = NativeMethods.operator_list_with_options(this, executorHandle, path, GetOptionsHandle(nativeOptionsHandle));

        return ToValueOrThrowAndRelease<IReadOnlyList<Entry>, OpenDALEntryListResult>(result);
    }

    /// <summary>
    /// Lists entries under the specified path asynchronously.
    /// </summary>
    /// <param name="path">Target path in the configured backend.</param>
    /// <param name="options">Additional list options.</param>
    /// <param name="cancellationToken">Cancellation token for the managed task.</param>
    /// <returns>A task that resolves with listed entries.</returns>
    public Task<IReadOnlyList<Entry>> ListAsync(
        string path,
        ListOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        return ListAsync(path, options, executor: null, cancellationToken);
    }

    /// <summary>
    /// Lists entries under the specified path asynchronously using the provided executor.
    /// </summary>
    /// <param name="path">Target path in the configured backend.</param>
    /// <param name="options">Additional list options.</param>
    /// <param name="executor">Executor used for this operation, or <see langword="null"/> to use default executor.</param>
    /// <param name="cancellationToken">Cancellation token for the managed task.</param>
    /// <returns>A task that resolves with listed entries.</returns>
    public Task<IReadOnlyList<Entry>> ListAsync(
        string path,
        ListOptions? options,
        Executor? executor,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(IsInvalid, this);
        var executorHandle = GetExecutorHandle(executor);

        return SubmitAsyncOperation<IReadOnlyList<Entry>, ListOptions>(options, SubmitListAsync, cancellationToken);

        OpenDALResult SubmitListAsync(long context, IntPtr optionsHandle)
        {
            unsafe
            {
                return NativeMethods.operator_list_with_options_async(
                    this,
                    executorHandle,
                    path,
                    optionsHandle,
                    &OnListCompleted,
                    context
                );
            }
        }
    }

    /// <summary>
    /// Duplicates this operator and returns a new managed instance.
    /// </summary>
    /// <returns>A new operator handle that shares the same backend configuration.</returns>
    /// <exception cref="ObjectDisposedException">The operator has been disposed.</exception>
    /// <exception cref="OpenDALException">Native operator duplication fails.</exception>
    public Operator Duplicate()
    {
        ObjectDisposedException.ThrowIf(IsInvalid, this);

        var result = NativeMethods.operator_duplicate(this);
        var newHandle = ToValueOrThrowAndRelease<IntPtr, OpenDALOperatorResult>(result);
        if (newHandle == IntPtr.Zero)
        {
            throw new InvalidOperationException("Duplicate returned null operator pointer");
        }

        return new Operator(newHandle);
    }

    /// <summary>
    /// Deletes the file at the specified path.
    /// </summary>
    /// <param name="path">Target path in the configured backend.</param>
    public void Delete(string path)
    {
        Delete(path, executor: null);
    }

    /// <summary>
    /// Deletes the file at the specified path using the provided executor.
    /// </summary>
    /// <param name="path">Target path in the configured backend.</param>
    /// <param name="executor">Executor used for this operation, or <see langword="null"/> to use default executor.</param>
    public void Delete(string path, Executor? executor)
    {
        ObjectDisposedException.ThrowIf(IsInvalid, this);
        var executorHandle = GetExecutorHandle(executor);
        var result = NativeMethods.operator_delete(this, executorHandle, path);
        ThrowIfErrorAndRelease(result);
    }

    /// <summary>
    /// Deletes the file at the specified path asynchronously.
    /// </summary>
    /// <param name="path">Target path in the configured backend.</param>
    /// <param name="cancellationToken">Cancellation token for the managed task.</param>
    /// <returns>A task that completes when the native callback reports completion.</returns>
    public Task DeleteAsync(string path, CancellationToken cancellationToken = default)
    {
        return DeleteAsync(path, executor: null, cancellationToken);
    }

    /// <summary>
    /// Deletes the file at the specified path asynchronously using the provided executor.
    /// </summary>
    /// <param name="path">Target path in the configured backend.</param>
    /// <param name="executor">Executor used for this operation, or <see langword="null"/> to use default executor.</param>
    /// <param name="cancellationToken">Cancellation token for the managed task.</param>
    /// <returns>A task that completes when the native callback reports completion.</returns>
    public Task DeleteAsync(string path, Executor? executor, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(IsInvalid, this);
        var executorHandle = GetExecutorHandle(executor);

        return SubmitAsyncOperation(SubmitDeleteAsync, cancellationToken);

        OpenDALResult SubmitDeleteAsync(long context)
        {
            unsafe
            {
                return NativeMethods.operator_delete_async(
                    this,
                    executorHandle,
                    path,
                    &OnDeleteCompleted,
                    context
                );
            }
        }
    }

    /// <summary>
    /// Creates a directory at the specified path.
    /// </summary>
    /// <param name="path">Target path in the configured backend.</param>
    public void CreateDir(string path)
    {
        CreateDir(path, executor: null);
    }

    /// <summary>
    /// Creates a directory at the specified path using the provided executor.
    /// </summary>
    /// <param name="path">Target path in the configured backend.</param>
    /// <param name="executor">Executor used for this operation, or <see langword="null"/> to use default executor.</param>
    public void CreateDir(string path, Executor? executor)
    {
        ObjectDisposedException.ThrowIf(IsInvalid, this);
        var executorHandle = GetExecutorHandle(executor);
        var result = NativeMethods.operator_create_dir(this, executorHandle, path);
        ThrowIfErrorAndRelease(result);
    }

    /// <summary>
    /// Creates a directory at the specified path asynchronously.
    /// </summary>
    /// <param name="path">Target path in the configured backend.</param>
    /// <param name="cancellationToken">Cancellation token for the managed task.</param>
    /// <returns>A task that completes when the native callback reports completion.</returns>
    public Task CreateDirAsync(string path, CancellationToken cancellationToken = default)
    {
        return CreateDirAsync(path, executor: null, cancellationToken);
    }

    /// <summary>
    /// Creates a directory at the specified path asynchronously using the provided executor.
    /// </summary>
    /// <param name="path">Target path in the configured backend.</param>
    /// <param name="executor">Executor used for this operation, or <see langword="null"/> to use default executor.</param>
    /// <param name="cancellationToken">Cancellation token for the managed task.</param>
    /// <returns>A task that completes when the native callback reports completion.</returns>
    public Task CreateDirAsync(string path, Executor? executor, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(IsInvalid, this);
        var executorHandle = GetExecutorHandle(executor);

        return SubmitAsyncOperation(SubmitCreateDirAsync, cancellationToken);

        OpenDALResult SubmitCreateDirAsync(long context)
        {
            unsafe
            {
                return NativeMethods.operator_create_dir_async(
                    this,
                    executorHandle,
                    path,
                    &OnCreateDirCompleted,
                    context
                );
            }
        }
    }

    /// <summary>
    /// Copies a file from source path to target path.
    /// </summary>
    /// <param name="sourcePath">Source path in the configured backend.</param>
    /// <param name="targetPath">Target path in the configured backend.</param>
    public void Copy(string sourcePath, string targetPath)
    {
        Copy(sourcePath, targetPath, executor: null);
    }

    /// <summary>
    /// Copies a file from source path to target path using the provided executor.
    /// </summary>
    /// <param name="sourcePath">Source path in the configured backend.</param>
    /// <param name="targetPath">Target path in the configured backend.</param>
    /// <param name="executor">Executor used for this operation, or <see langword="null"/> to use default executor.</param>
    public void Copy(string sourcePath, string targetPath, Executor? executor)
    {
        ObjectDisposedException.ThrowIf(IsInvalid, this);
        var executorHandle = GetExecutorHandle(executor);
        var result = NativeMethods.operator_copy(this, executorHandle, sourcePath, targetPath);
        ThrowIfErrorAndRelease(result);
    }

    /// <summary>
    /// Copies a file from source path to target path asynchronously.
    /// </summary>
    /// <param name="sourcePath">Source path in the configured backend.</param>
    /// <param name="targetPath">Target path in the configured backend.</param>
    /// <param name="cancellationToken">Cancellation token for the managed task.</param>
    /// <returns>A task that completes when the native callback reports completion.</returns>
    public Task CopyAsync(string sourcePath, string targetPath, CancellationToken cancellationToken = default)
    {
        return CopyAsync(sourcePath, targetPath, executor: null, cancellationToken);
    }

    /// <summary>
    /// Copies a file from source path to target path asynchronously using the provided executor.
    /// </summary>
    /// <param name="sourcePath">Source path in the configured backend.</param>
    /// <param name="targetPath">Target path in the configured backend.</param>
    /// <param name="executor">Executor used for this operation, or <see langword="null"/> to use default executor.</param>
    /// <param name="cancellationToken">Cancellation token for the managed task.</param>
    /// <returns>A task that completes when the native callback reports completion.</returns>
    public Task CopyAsync(
        string sourcePath,
        string targetPath,
        Executor? executor,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(IsInvalid, this);
        var executorHandle = GetExecutorHandle(executor);

        return SubmitAsyncOperation(SubmitCopyAsync, cancellationToken);

        OpenDALResult SubmitCopyAsync(long context)
        {
            unsafe
            {
                return NativeMethods.operator_copy_async(
                    this,
                    executorHandle,
                    sourcePath,
                    targetPath,
                    &OnCopyCompleted,
                    context
                );
            }
        }
    }

    /// <summary>
    /// Renames a file from source path to target path.
    /// </summary>
    /// <param name="sourcePath">Source path in the configured backend.</param>
    /// <param name="targetPath">Target path in the configured backend.</param>
    public void Rename(string sourcePath, string targetPath)
    {
        Rename(sourcePath, targetPath, executor: null);
    }

    /// <summary>
    /// Renames a file from source path to target path using the provided executor.
    /// </summary>
    /// <param name="sourcePath">Source path in the configured backend.</param>
    /// <param name="targetPath">Target path in the configured backend.</param>
    /// <param name="executor">Executor used for this operation, or <see langword="null"/> to use default executor.</param>
    public void Rename(string sourcePath, string targetPath, Executor? executor)
    {
        ObjectDisposedException.ThrowIf(IsInvalid, this);
        var executorHandle = GetExecutorHandle(executor);
        var result = NativeMethods.operator_rename(this, executorHandle, sourcePath, targetPath);
        ThrowIfErrorAndRelease(result);
    }

    /// <summary>
    /// Renames a file from source path to target path asynchronously.
    /// </summary>
    /// <param name="sourcePath">Source path in the configured backend.</param>
    /// <param name="targetPath">Target path in the configured backend.</param>
    /// <param name="cancellationToken">Cancellation token for the managed task.</param>
    /// <returns>A task that completes when the native callback reports completion.</returns>
    public Task RenameAsync(string sourcePath, string targetPath, CancellationToken cancellationToken = default)
    {
        return RenameAsync(sourcePath, targetPath, executor: null, cancellationToken);
    }

    /// <summary>
    /// Renames a file from source path to target path asynchronously using the provided executor.
    /// </summary>
    /// <param name="sourcePath">Source path in the configured backend.</param>
    /// <param name="targetPath">Target path in the configured backend.</param>
    /// <param name="executor">Executor used for this operation, or <see langword="null"/> to use default executor.</param>
    /// <param name="cancellationToken">Cancellation token for the managed task.</param>
    /// <returns>A task that completes when the native callback reports completion.</returns>
    public Task RenameAsync(
        string sourcePath,
        string targetPath,
        Executor? executor,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(IsInvalid, this);
        var executorHandle = GetExecutorHandle(executor);

        return SubmitAsyncOperation(SubmitRenameAsync, cancellationToken);

        OpenDALResult SubmitRenameAsync(long context)
        {
            unsafe
            {
                return NativeMethods.operator_rename_async(
                    this,
                    executorHandle,
                    sourcePath,
                    targetPath,
                    &OnRenameCompleted,
                    context
                );
            }
        }
    }

    /// <summary>
    /// Removes all entries under the specified path recursively.
    /// </summary>
    /// <param name="path">Target path in the configured backend.</param>
    public void RemoveAll(string path)
    {
        RemoveAll(path, executor: null);
    }

    /// <summary>
    /// Removes all entries under the specified path recursively using the provided executor.
    /// </summary>
    /// <param name="path">Target path in the configured backend.</param>
    /// <param name="executor">Executor used for this operation, or <see langword="null"/> to use default executor.</param>
    public void RemoveAll(string path, Executor? executor)
    {
        ObjectDisposedException.ThrowIf(IsInvalid, this);
        var executorHandle = GetExecutorHandle(executor);
        var result = NativeMethods.operator_remove_all(this, executorHandle, path);
        ThrowIfErrorAndRelease(result);
    }

    /// <summary>
    /// Removes all entries under the specified path recursively asynchronously.
    /// </summary>
    /// <param name="path">Target path in the configured backend.</param>
    /// <param name="cancellationToken">Cancellation token for the managed task.</param>
    /// <returns>A task that completes when the native callback reports completion.</returns>
    public Task RemoveAllAsync(string path, CancellationToken cancellationToken = default)
    {
        return RemoveAllAsync(path, executor: null, cancellationToken);
    }

    /// <summary>
    /// Removes all entries under the specified path recursively asynchronously using the provided executor.
    /// </summary>
    /// <param name="path">Target path in the configured backend.</param>
    /// <param name="executor">Executor used for this operation, or <see langword="null"/> to use default executor.</param>
    /// <param name="cancellationToken">Cancellation token for the managed task.</param>
    /// <returns>A task that completes when the native callback reports completion.</returns>
    public Task RemoveAllAsync(string path, Executor? executor, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(IsInvalid, this);
        var executorHandle = GetExecutorHandle(executor);

        return SubmitAsyncOperation(SubmitRemoveAllAsync, cancellationToken);

        OpenDALResult SubmitRemoveAllAsync(long context)
        {
            unsafe
            {
                return NativeMethods.operator_remove_all_async(
                    this,
                    executorHandle,
                    path,
                    &OnRemoveAllCompleted,
                    context
                );
            }
        }
    }

    /// <summary>
    /// Creates a presigned read request asynchronously.
    /// </summary>
    /// <param name="path">Target path in the configured backend.</param>
    /// <param name="expiration">Presigned request expiration duration.</param>
    /// <param name="cancellationToken">Cancellation token for the managed task.</param>
    /// <returns>A task that resolves with a presigned request.</returns>
    public Task<PresignedRequest> PresignReadAsync(
        string path,
        TimeSpan expiration,
        CancellationToken cancellationToken = default)
    {
        return PresignReadAsync(path, expiration, executor: null, cancellationToken);
    }

    /// <summary>
    /// Creates a presigned read request asynchronously using the provided executor.
    /// </summary>
    public Task<PresignedRequest> PresignReadAsync(
        string path,
        TimeSpan expiration,
        Executor? executor,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(IsInvalid, this);
        var executorHandle = GetExecutorHandle(executor);
        var expireNanos = Utilities.ToNanoseconds(expiration, nameof(expiration));

        return SubmitAsyncOperation<PresignedRequest>(SubmitPresignReadAsync, cancellationToken);

        OpenDALResult SubmitPresignReadAsync(long context)
        {
            unsafe
            {
                return NativeMethods.operator_presign_read_async(
                    this,
                    executorHandle,
                    path,
                    expireNanos,
                    &OnPresignReadCompleted,
                    context
                );
            }
        }
    }

    /// <summary>
    /// Creates a presigned write request asynchronously.
    /// </summary>
    public Task<PresignedRequest> PresignWriteAsync(
        string path,
        TimeSpan expiration,
        CancellationToken cancellationToken = default)
    {
        return PresignWriteAsync(path, expiration, executor: null, cancellationToken);
    }

    /// <summary>
    /// Creates a presigned write request asynchronously using the provided executor.
    /// </summary>
    public Task<PresignedRequest> PresignWriteAsync(
        string path,
        TimeSpan expiration,
        Executor? executor,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(IsInvalid, this);
        var executorHandle = GetExecutorHandle(executor);
        var expireNanos = Utilities.ToNanoseconds(expiration, nameof(expiration));

        return SubmitAsyncOperation<PresignedRequest>(SubmitPresignWriteAsync, cancellationToken);

        OpenDALResult SubmitPresignWriteAsync(long context)
        {
            unsafe
            {
                return NativeMethods.operator_presign_write_async(
                    this,
                    executorHandle,
                    path,
                    expireNanos,
                    &OnPresignWriteCompleted,
                    context
                );
            }
        }
    }

    /// <summary>
    /// Creates a presigned stat request asynchronously.
    /// </summary>
    public Task<PresignedRequest> PresignStatAsync(
        string path,
        TimeSpan expiration,
        CancellationToken cancellationToken = default)
    {
        return PresignStatAsync(path, expiration, executor: null, cancellationToken);
    }

    /// <summary>
    /// Creates a presigned stat request asynchronously using the provided executor.
    /// </summary>
    public Task<PresignedRequest> PresignStatAsync(
        string path,
        TimeSpan expiration,
        Executor? executor,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(IsInvalid, this);
        var executorHandle = GetExecutorHandle(executor);
        var expireNanos = Utilities.ToNanoseconds(expiration, nameof(expiration));

        return SubmitAsyncOperation<PresignedRequest>(SubmitPresignStatAsync, cancellationToken);

        OpenDALResult SubmitPresignStatAsync(long context)
        {
            unsafe
            {
                return NativeMethods.operator_presign_stat_async(
                    this,
                    executorHandle,
                    path,
                    expireNanos,
                    &OnPresignStatCompleted,
                    context
                );
            }
        }
    }

    /// <summary>
    /// Creates a presigned delete request asynchronously.
    /// </summary>
    public Task<PresignedRequest> PresignDeleteAsync(
        string path,
        TimeSpan expiration,
        CancellationToken cancellationToken = default)
    {
        return PresignDeleteAsync(path, expiration, executor: null, cancellationToken);
    }

    /// <summary>
    /// Creates a presigned delete request asynchronously using the provided executor.
    /// </summary>
    public Task<PresignedRequest> PresignDeleteAsync(
        string path,
        TimeSpan expiration,
        Executor? executor,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(IsInvalid, this);
        var executorHandle = GetExecutorHandle(executor);
        var expireNanos = Utilities.ToNanoseconds(expiration, nameof(expiration));

        return SubmitAsyncOperation<PresignedRequest>(SubmitPresignDeleteAsync, cancellationToken);

        OpenDALResult SubmitPresignDeleteAsync(long context)
        {
            unsafe
            {
                return NativeMethods.operator_presign_delete_async(
                    this,
                    executorHandle,
                    path,
                    expireNanos,
                    &OnPresignDeleteCompleted,
                    context
                );
            }
        }
    }

    /// <summary>
    /// Opens a read stream for the specified path.
    /// </summary>
    /// <param name="path">Target path in the configured backend.</param>
    /// <param name="options">Optional read options.</param>
    /// <param name="executor">Executor used for stream creation, or <see langword="null"/> to use default executor.</param>
    /// <returns>A readable stream over the given path.</returns>
    public OperatorInputStream OpenReadStream(
        string path,
        ReadOptions? options = null,
        Executor? executor = null)
    {
        ObjectDisposedException.ThrowIf(IsInvalid, this);
        var executorHandle = GetExecutorHandle(executor);

        using var nativeOptions = options?.BuildNativeOptionsHandle();
        var result = NativeMethods.operator_input_stream_create(
            this,
            executorHandle,
            path,
            GetOptionsHandle(nativeOptions)
        );

        var streamHandle = ToValueOrThrowAndRelease<IntPtr, OpenDALOperatorResult>(result);
        if (streamHandle == IntPtr.Zero)
        {
            throw new InvalidOperationException("OpenReadStream returned null stream pointer");
        }

        return new OperatorInputStream(streamHandle);
    }

    /// <summary>
    /// Opens a write stream for the specified path.
    /// </summary>
    /// <param name="path">Target path in the configured backend.</param>
    /// <param name="options">Optional write options.</param>
    /// <param name="bufferSize">Buffer size used by the managed write stream.</param>
    /// <param name="executor">Executor used for stream creation, or <see langword="null"/> to use default executor.</param>
    /// <returns>A writable stream over the given path.</returns>
    public OperatorOutputStream OpenWriteStream(
        string path,
        WriteOptions? options = null,
        int bufferSize = OperatorOutputStream.DefaultBufferSize,
        Executor? executor = null)
    {
        ObjectDisposedException.ThrowIf(IsInvalid, this);
        var executorHandle = GetExecutorHandle(executor);

        using var nativeOptions = options?.BuildNativeOptionsHandle();
        var result = NativeMethods.operator_output_stream_create(
            this,
            executorHandle,
            path,
            GetOptionsHandle(nativeOptions)
        );

        var streamHandle = ToValueOrThrowAndRelease<IntPtr, OpenDALOperatorResult>(result);
        if (streamHandle == IntPtr.Zero)
        {
            throw new InvalidOperationException("OpenWriteStream returned null stream pointer");
        }

        return new OperatorOutputStream(streamHandle, bufferSize);
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
    /// Applies a native layer result by creating a new operator from the returned handle.
    /// </summary>
    /// <param name="result">Native result that contains a new operator pointer.</param>
    /// <returns>A new operator instance.</returns>
    /// <exception cref="InvalidOperationException">Returned operator pointer is null.</exception>
    /// <exception cref="OpenDALException">Native layer application fails.</exception>
    internal Operator ApplyLayerResult(OpenDALOperatorResult result)
    {
        var newHandle = ToValueOrThrowAndRelease<IntPtr, OpenDALOperatorResult>(result);
        if (newHandle == IntPtr.Zero)
        {
            throw new InvalidOperationException("Layer application returned null operator pointer");
        }

        return new Operator(newHandle);
    }

    /// <summary>
    /// Gets the native executor handle for operation submission.
    /// </summary>
    /// <param name="executor">Executor instance or <see langword="null"/> to use default executor.</param>
    /// <returns>Native executor pointer, or <see cref="IntPtr.Zero"/> when no executor is specified.</returns>
    /// <exception cref="ObjectDisposedException">The executor has already been disposed.</exception>
    private static IntPtr GetExecutorHandle(Executor? executor)
    {
        if (executor is null)
        {
            return IntPtr.Zero;
        }

        ObjectDisposedException.ThrowIf(executor.IsClosed || executor.IsInvalid, executor);
        return executor.DangerousGetHandle();
    }

    /// <summary>
    /// Gets the native options pointer from an optional native options handle.
    /// </summary>
    /// <param name="options">Native options handle or <see langword="null"/>.</param>
    /// <returns>Native options pointer, or <see cref="IntPtr.Zero"/> when options are not provided.</returns>
    private static IntPtr GetOptionsHandle(NativeOptionsHandle? options)
    {
        return options is null ? IntPtr.Zero : options.DangerousGetHandle();
    }

    /// <summary>
    /// Creates the lazily-evaluated operator info loader.
    /// </summary>
    /// <returns>A thread-safe lazy loader for <see cref="OperatorInfo"/>.</returns>
    private Lazy<OperatorInfo> CreateInfoLazy()
    {
        return new Lazy<OperatorInfo>(CreateOperatorInfo, LazyThreadSafetyMode.ExecutionAndPublication);
    }

    /// <summary>
    /// Retrieves operator info from the native layer.
    /// </summary>
    /// <returns>Managed operator info value.</returns>
    /// <exception cref="OpenDALException">Native operator info retrieval fails.</exception>
    private OperatorInfo CreateOperatorInfo()
    {
        var result = NativeMethods.operator_info_get(this);

        return ToValueOrThrowAndRelease<OperatorInfo, OpenDALOperatorInfoResult>(result);
    }

    /// <summary>
    /// Builds constructor options for operator creation when key/value options are provided.
    /// </summary>
    /// <param name="options">Backend options dictionary.</param>
    /// <returns>Native options handle, or <see langword="null"/> when options are empty.</returns>
    private static NativeOptionsHandle? CreateConstructorOptionsHandle(IReadOnlyDictionary<string, string>? options)
    {
        if (options is null || options.Count == 0)
        {
            return null;
        }

        return NativeOptionsBuilder.BuildNativeOptionsHandle(
            options,
            NativeMethods.constructor_option_build,
            NativeMethods.constructor_option_free
        );
    }

    /// <summary>
    /// Converts a native result into a managed value, throwing on native error and always releasing native resources.
    /// </summary>
    /// <typeparam name="TOutput">Managed output type.</typeparam>
    /// <typeparam name="TResult">Native result type.</typeparam>
    /// <param name="result">Native result payload.</param>
    /// <returns>Managed value converted from <paramref name="result"/>.</returns>
    /// <exception cref="OpenDALException">Native operation returns an error.</exception>
    internal static TOutput ToValueOrThrowAndRelease<TOutput, TResult>(TResult result)
        where TResult : struct, INativeValueResult<TOutput>
    {
        try
        {
            var error = result.GetError();
            if (error.IsError)
            {
                throw new OpenDALException(error);
            }

            return result.ToValue();
        }
        finally
        {
            result.Release();
        }
    }

    /// <summary>
    /// Throws when a native result reports an error and always releases native resources.
    /// </summary>
    /// <typeparam name="TResult">Native result type.</typeparam>
    /// <param name="result">Native result payload.</param>
    /// <exception cref="OpenDALException">Native operation returns an error.</exception>
    internal static void ThrowIfErrorAndRelease<TResult>(TResult result)
        where TResult : struct, INativeResult
    {
        try
        {
            var error = result.GetError();
            if (error.IsError)
            {
                throw new OpenDALException(error);
            }
        }
        finally
        {
            result.Release();
        }
    }

    /// <summary>
    /// Submits a native async operation and binds it to a managed task completion source.
    /// </summary>
    /// <typeparam name="TOutput">Managed task result type.</typeparam>
    /// <typeparam name="TOptions">Managed options type.</typeparam>
    /// <param name="options">Optional managed options for this operation.</param>
    /// <param name="submit">Submission delegate that invokes the native async API.</param>
    /// <param name="cancellationToken">Cancellation token for managed task observation.</param>
    /// <returns>A task completed by the corresponding native callback.</returns>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> is already canceled.</exception>
    /// <exception cref="OpenDALException">Native submission returns an immediate error.</exception>
    internal static Task<TOutput> SubmitAsyncOperation<TOutput, TOptions>(
        TOptions? options,
        Func<long, IntPtr, OpenDALResult> submit,
        CancellationToken cancellationToken)
        where TOptions : class, IOptions
    {
        cancellationToken.ThrowIfCancellationRequested();
        var context = AsyncStateRegistry.Register<TOutput>(out var asyncState);
        try
        {
            using var nativeOptionsHandle = options?.BuildNativeOptionsHandle();
            var submitResult = submit(context, GetOptionsHandle(nativeOptionsHandle));
            ThrowIfErrorAndRelease(submitResult);
            asyncState.BindCancellation(cancellationToken);
            return asyncState.Completion.Task;
        }
        catch
        {
            AsyncStateRegistry.Unregister(context);
            throw;
        }
    }

    /// <summary>
    /// Submits a native async operation without options and binds it to a managed task completion source.
    /// </summary>
    /// <typeparam name="TOutput">Managed task result type.</typeparam>
    /// <param name="submit">Submission delegate that invokes the native async API.</param>
    /// <param name="cancellationToken">Cancellation token for managed task observation.</param>
    /// <returns>A task completed by the corresponding native callback.</returns>
    internal static Task<TOutput> SubmitAsyncOperation<TOutput>(
        Func<long, OpenDALResult> submit,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var context = AsyncStateRegistry.Register<TOutput>(out var asyncState);
        try
        {
            var submitResult = submit(context);
            ThrowIfErrorAndRelease(submitResult);
            asyncState.BindCancellation(cancellationToken);
            return asyncState.Completion.Task;
        }
        catch
        {
            AsyncStateRegistry.Unregister(context);
            throw;
        }
    }

    /// <summary>
    /// Submits a native async operation without options and binds it to a managed task completion source.
    /// </summary>
    /// <param name="submit">Submission delegate that invokes the native async API.</param>
    /// <param name="cancellationToken">Cancellation token for managed task observation.</param>
    /// <returns>A task completed by the corresponding native callback.</returns>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> is already canceled.</exception>
    /// <exception cref="OpenDALException">Native submission returns an immediate error.</exception>
    internal static Task SubmitAsyncOperation(
        Func<long, OpenDALResult> submit,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var context = AsyncStateRegistry.Register<bool>(out var asyncState);
        try
        {
            var submitResult = submit(context);
            ThrowIfErrorAndRelease(submitResult);
            asyncState.BindCancellation(cancellationToken);
            return asyncState.Completion.Task;
        }
        catch
        {
            AsyncStateRegistry.Unregister(context);
            throw;
        }
    }

    /// <summary>
    /// Attempts to retrieve and remove async state for a callback context.
    /// </summary>
    /// <typeparam name="T">Async state result type.</typeparam>
    /// <param name="context">Native callback context id.</param>
    /// <param name="state">Resolved async state when found.</param>
    /// <returns><see langword="true"/> if an async state is found; otherwise <see langword="false"/>.</returns>
    private static bool TryTakeAsyncState<T>(long context, [NotNullWhen(true)] out AsyncState<T>? state)
    {
        if (AsyncStateRegistry.TryTake<AsyncState<T>>(context, out var current))
        {
            state = current;
            return true;
        }

        state = null;
        return false;
    }

    /// <summary>
    /// Completes a value-producing async state from a native callback result.
    /// </summary>
    /// <typeparam name="TOutput">Managed output type.</typeparam>
    /// <typeparam name="TResult">Native result type.</typeparam>
    /// <param name="context">Native callback context id.</param>
    /// <param name="result">Native callback result payload.</param>
    private static void CompleteAsyncState<TOutput, TResult>(long context, TResult result)
        where TResult : struct, INativeValueResult<TOutput>
    {
        if (!TryTakeAsyncState(context, out AsyncState<TOutput>? state))
        {
            return;
        }

        try
        {
            state.CancellationRegistration.Dispose();

            if (result.GetError().IsError)
            {
                state.Completion.TrySetException(new OpenDALException(result.GetError()));
                return;
            }

            state.Completion.TrySetResult(result.ToValue());
        }
        catch (Exception ex)
        {
            state.Completion.TrySetException(ex);
        }
    }

    /// <summary>
    /// Completes a non-value async state from a native callback result.
    /// </summary>
    /// <typeparam name="TResult">Native result type.</typeparam>
    /// <param name="context">Native callback context id.</param>
    /// <param name="result">Native callback result payload.</param>
    private static void CompleteAsyncState<TResult>(long context, TResult result)
        where TResult : struct, INativeResult
    {
        if (!TryTakeAsyncState(context, out AsyncState<bool>? state))
        {
            return;
        }

        try
        {
            state.CancellationRegistration.Dispose();

            var error = result.GetError();
            if (error.IsError)
            {
                state.Completion.TrySetException(new OpenDALException(error));
                return;
            }

            state.Completion.TrySetResult(true);
        }
        catch (Exception ex)
        {
            state.Completion.TrySetException(ex);
        }
    }

    /// <summary>
    /// Finalizes a value-producing native callback by completing managed state and releasing native resources.
    /// </summary>
    /// <typeparam name="TOutput">Managed output type.</typeparam>
    /// <typeparam name="TResult">Native result type.</typeparam>
    /// <param name="context">Native callback context id.</param>
    /// <param name="result">Native callback result payload.</param>
    internal static void CompleteAsyncCallback<TOutput, TResult>(long context, TResult result)
        where TResult : struct, INativeValueResult<TOutput>
    {
        try
        {
            CompleteAsyncState<TOutput, TResult>(context, result);
        }
        finally
        {
            result.Release();
        }
    }

    /// <summary>
    /// Finalizes a non-value native callback by completing managed state and releasing native resources.
    /// </summary>
    /// <typeparam name="TResult">Native result type.</typeparam>
    /// <param name="context">Native callback context id.</param>
    /// <param name="result">Native callback result payload.</param>
    internal static void CompleteAsyncCallback<TResult>(long context, TResult result)
        where TResult : struct, INativeResult
    {
        try
        {
            CompleteAsyncState(context, result);
        }
        finally
        {
            result.Release();
        }
    }

    #region Async Callbacks

    /// <summary>
    /// Native callback invoked when an asynchronous write operation finishes.
    /// </summary>
    /// <param name="context">Opaque async state context previously registered by <see cref="AsyncStateRegistry"/>.</param>
    /// <param name="result">Write completion result returned by the native layer.</param>
    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static void OnWriteCompleted(long context, OpenDALResult result)
    {
        CompleteAsyncCallback(context, result);
    }

    /// <summary>
    /// Native callback invoked when an asynchronous read operation finishes.
    /// </summary>
    /// <param name="context">Opaque async state context previously registered by <see cref="AsyncStateRegistry"/>.</param>
    /// <param name="result">Read completion result returned by the native layer, including byte buffer payload.</param>
    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static void OnReadCompleted(long context, OpenDALReadResult result)
    {
        CompleteAsyncCallback<byte[], OpenDALReadResult>(context, result);
    }

    /// <summary>
    /// Native callback invoked when an asynchronous stat operation finishes.
    /// </summary>
    /// <param name="context">Opaque async state context previously registered by <see cref="AsyncStateRegistry"/>.</param>
    /// <param name="result">Stat completion result returned by the native layer.</param>
    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static void OnStatCompleted(long context, OpenDALMetadataResult result)
    {
        CompleteAsyncCallback<Metadata, OpenDALMetadataResult>(context, result);
    }

    /// <summary>
    /// Native callback invoked when an asynchronous list operation finishes.
    /// </summary>
    /// <param name="context">Opaque async state context previously registered by <see cref="AsyncStateRegistry"/>.</param>
    /// <param name="result">List completion result returned by the native layer.</param>
    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static void OnListCompleted(long context, OpenDALEntryListResult result)
    {
        CompleteAsyncCallback<IReadOnlyList<Entry>, OpenDALEntryListResult>(context, result);
    }

    /// <summary>
    /// Native callback invoked when an asynchronous delete operation finishes.
    /// </summary>
    /// <param name="context">Opaque async state context previously registered by <see cref="AsyncStateRegistry"/>.</param>
    /// <param name="result">Delete completion result returned by the native layer.</param>
    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static void OnDeleteCompleted(long context, OpenDALResult result)
    {
        CompleteAsyncCallback(context, result);
    }

    /// <summary>
    /// Native callback invoked when an asynchronous create-dir operation finishes.
    /// </summary>
    /// <param name="context">Opaque async state context previously registered by <see cref="AsyncStateRegistry"/>.</param>
    /// <param name="result">Create-dir completion result returned by the native layer.</param>
    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static void OnCreateDirCompleted(long context, OpenDALResult result)
    {
        CompleteAsyncCallback(context, result);
    }

    /// <summary>
    /// Native callback invoked when an asynchronous copy operation finishes.
    /// </summary>
    /// <param name="context">Opaque async state context previously registered by <see cref="AsyncStateRegistry"/>.</param>
    /// <param name="result">Copy completion result returned by the native layer.</param>
    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static void OnCopyCompleted(long context, OpenDALResult result)
    {
        CompleteAsyncCallback(context, result);
    }

    /// <summary>
    /// Native callback invoked when an asynchronous rename operation finishes.
    /// </summary>
    /// <param name="context">Opaque async state context previously registered by <see cref="AsyncStateRegistry"/>.</param>
    /// <param name="result">Rename completion result returned by the native layer.</param>
    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static void OnRenameCompleted(long context, OpenDALResult result)
    {
        CompleteAsyncCallback(context, result);
    }

    /// <summary>
    /// Native callback invoked when an asynchronous remove-all operation finishes.
    /// </summary>
    /// <param name="context">Opaque async state context previously registered by <see cref="AsyncStateRegistry"/>.</param>
    /// <param name="result">Remove-all completion result returned by the native layer.</param>
    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static void OnRemoveAllCompleted(long context, OpenDALResult result)
    {
        CompleteAsyncCallback(context, result);
    }

    /// <summary>
    /// Native callback invoked when an asynchronous presign-read operation finishes.
    /// </summary>
    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static void OnPresignReadCompleted(long context, OpenDALPresignedRequestResult result)
    {
        CompleteAsyncCallback<PresignedRequest, OpenDALPresignedRequestResult>(context, result);
    }

    /// <summary>
    /// Native callback invoked when an asynchronous presign-write operation finishes.
    /// </summary>
    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static void OnPresignWriteCompleted(long context, OpenDALPresignedRequestResult result)
    {
        CompleteAsyncCallback<PresignedRequest, OpenDALPresignedRequestResult>(context, result);
    }

    /// <summary>
    /// Native callback invoked when an asynchronous presign-stat operation finishes.
    /// </summary>
    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static void OnPresignStatCompleted(long context, OpenDALPresignedRequestResult result)
    {
        CompleteAsyncCallback<PresignedRequest, OpenDALPresignedRequestResult>(context, result);
    }

    /// <summary>
    /// Native callback invoked when an asynchronous presign-delete operation finishes.
    /// </summary>
    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static void OnPresignDeleteCompleted(long context, OpenDALPresignedRequestResult result)
    {
        CompleteAsyncCallback<PresignedRequest, OpenDALPresignedRequestResult>(context, result);
    }

    #endregion

}