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

using OpenDAL.Interop.Result;

namespace OpenDAL;

/// <summary>
/// Write-only stream over an OpenDAL path.
/// </summary>
public sealed class OperatorOutputStream : Stream
{
    internal const int DefaultBufferSize = 16 * 1024;

    private IntPtr handle;
    private bool disposed;
    private readonly byte[] buffer;
    private int buffered;

    internal OperatorOutputStream(IntPtr handle, int bufferSize)
    {
        if (handle == IntPtr.Zero)
        {
            throw new ArgumentException("Native output stream handle must not be zero.", nameof(handle));
        }

        if (bufferSize <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(bufferSize), "Buffer size must be greater than zero.");
        }

        this.handle = handle;
        buffer = GC.AllocateUninitializedArray<byte>(bufferSize);
    }

    public override bool CanRead => false;

    public override bool CanSeek => false;

    public override bool CanWrite => !disposed;

    public override long Length => throw new NotSupportedException();

    public override long Position
    {
        get => throw new NotSupportedException();
        set => throw new NotSupportedException();
    }

    public override void Write(byte[] source, int offset, int count)
    {
        ArgumentNullException.ThrowIfNull(source);
        Write(source.AsSpan(offset, count));
    }

    public override void Write(ReadOnlySpan<byte> source)
    {
        ThrowIfDisposed();
        while (!source.IsEmpty)
        {
            var writable = buffer.Length - buffered;
            if (writable == 0)
            {
                FlushBuffered();
                writable = buffer.Length;
            }

            var take = Math.Min(writable, source.Length);
            source[..take].CopyTo(buffer.AsSpan(buffered));
            buffered += take;
            source = source[take..];
        }
    }

    public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        Write(buffer, offset, count);
        return Task.CompletedTask;
    }

    public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        Write(buffer.Span);
        return ValueTask.CompletedTask;
    }

    public override void Flush()
    {
        ThrowIfDisposed();
        FlushBuffered();
        var result = NativeMethods.operator_output_stream_flush(handle);
        Operator.ThrowIfErrorAndRelease(result);
    }

    public override Task FlushAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        Flush();
        return Task.CompletedTask;
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        throw new NotSupportedException();
    }

    public override long Seek(long offset, SeekOrigin origin)
    {
        throw new NotSupportedException();
    }

    public override void SetLength(long value)
    {
        throw new NotSupportedException();
    }

    private void FlushBuffered()
    {
        if (buffered == 0)
        {
            return;
        }

        var result = NativeMethods.operator_output_stream_write(handle, buffer, (nuint)buffered);
        Operator.ThrowIfErrorAndRelease(result);
        buffered = 0;
    }

    private void ThrowIfDisposed()
    {
        ObjectDisposedException.ThrowIf(disposed || handle == IntPtr.Zero, this);
    }

    protected override void Dispose(bool disposing)
    {
        if (!disposed)
        {
            OpenDALException? pending = null;
            try
            {
                Flush();
                var close = NativeMethods.operator_output_stream_close(handle);
                try
                {
                    Operator.ThrowIfErrorAndRelease(close);
                }
                catch (OpenDALException ex)
                {
                    pending = ex;
                }
            }
            finally
            {
                NativeMethods.operator_output_stream_free(handle);
                handle = IntPtr.Zero;
                buffered = 0;
                disposed = true;
            }

            if (pending is not null)
            {
                throw pending;
            }
        }

        base.Dispose(disposing);
    }
}
