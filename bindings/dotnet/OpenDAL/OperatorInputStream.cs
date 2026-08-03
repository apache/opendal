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
/// Read-only stream over an OpenDAL path.
/// </summary>
public sealed class OperatorInputStream : Stream
{
    private IntPtr handle;
    private bool disposed;

    // Current native chunk. Keeping the native handle instead of a managed
    // copy lets Read copy each byte exactly once, straight into the caller's
    // destination. Released once fully consumed, on EOF, or on dispose.
    private OpenDALReadResult? currentChunk;
    private nuint chunkOffset;

    internal OperatorInputStream(IntPtr handle)
    {
        if (handle == IntPtr.Zero)
        {
            throw new ArgumentException("Native input stream handle must not be zero.", nameof(handle));
        }

        this.handle = handle;
    }

    public override bool CanRead => !disposed;

    public override bool CanSeek => false;

    public override bool CanWrite => false;

    public override long Length => throw new NotSupportedException();

    public override long Position
    {
        get => throw new NotSupportedException();
        set => throw new NotSupportedException();
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        ArgumentNullException.ThrowIfNull(buffer);
        return Read(buffer.AsSpan(offset, count));
    }

    public override int Read(Span<byte> destination)
    {
        ThrowIfDisposed();

        var totalRead = 0;
        while (destination.Length > 0)
        {
            if (currentChunk is null || chunkOffset >= currentChunk.Value.Buffer.Len)
            {
                ReleaseCurrentChunk();
                var next = NativeMethods.operator_input_stream_read_next(handle);
                var error = next.GetError();
                if (error.IsError)
                {
                    var exception = new OpenDALException(error);
                    next.Release();
                    throw exception;
                }

                if (next.Buffer.Handle == IntPtr.Zero || next.Buffer.Len == 0)
                {
                    next.Release();
                    break;
                }

                currentChunk = next;
                chunkOffset = 0;
            }

            var copied = currentChunk.Value.Buffer.CopyTo(chunkOffset, destination);
            if (copied == 0)
            {
                break;
            }

            chunkOffset += (nuint)copied;
            destination = destination[copied..];
            totalRead += copied;
        }

        return totalRead;
    }

    private void ReleaseCurrentChunk()
    {
        if (currentChunk is { } chunk)
        {
            chunk.Release();
            currentChunk = null;
            chunkOffset = 0;
        }
    }

    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(Read(buffer, offset, count));
    }

    public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return ValueTask.FromResult(Read(buffer.Span));
    }

    public override void Flush()
    {
    }

    public override Task FlushAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.CompletedTask;
    }

    public override long Seek(long offset, SeekOrigin origin)
    {
        throw new NotSupportedException();
    }

    public override void SetLength(long value)
    {
        throw new NotSupportedException();
    }

    public override void Write(byte[] buffer, int offset, int count)
    {
        throw new NotSupportedException();
    }

    private void ThrowIfDisposed()
    {
        ObjectDisposedException.ThrowIf(disposed || handle == IntPtr.Zero, this);
    }

    protected override void Dispose(bool disposing)
    {
        if (!disposed)
        {
            ReleaseCurrentChunk();
            NativeMethods.operator_input_stream_free(handle);
            handle = IntPtr.Zero;
            disposed = true;
        }

        base.Dispose(disposing);
    }
}
