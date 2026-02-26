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

namespace DotOpenDAL.Tests;

public class MemoryOperatorTest
{
    [Fact]
    public async Task ReadWrite_DisposeRace_DoesNotCrashProcess()
    {
        var op = new Operator("memory");
        byte[] content = [0x10, 0x20, 0x30, 0x40];
        op.Write("seed", content);

        var workers = Enumerable.Range(0, 64).Select(async i =>
        {
            var path = $"race-{i % 8}";

            try
            {
                op.Write(path, content);
                _ = op.Read(path);
            }
            catch (ObjectDisposedException)
            {
            }
            catch (OpenDALException)
            {
            }

            await Task.Yield();
        });

        var dispose = Task.Run(op.Dispose);
        await Task.WhenAll(workers.Append(dispose));
    }

    [Fact]
    public void ReadWrite_Utf8Bytes_RoundTripsSuccessfully()
    {
        using var op = new Operator("memory");
        var content = "123456";
        var bytes = System.Text.Encoding.UTF8.GetBytes(content);
        Assert.NotEqual(op.Op, IntPtr.Zero);
        op.Write("test", bytes);
        var resultBytes = op.Read("test");
        var result = System.Text.Encoding.UTF8.GetString(resultBytes);
        Assert.Equal(content, result);
    }

    [Fact]
    public void ReadWrite_BinaryBytes_RoundTripsSuccessfully()
    {
        using var op = new Operator("memory");
        byte[] content = [0x00, 0x01, 0x02, 0x7F, 0x80, 0xFE, 0xFF];

        op.Write("test-bytes", content);
        var result = op.Read("test-bytes");

        Assert.Equal(content, result);
    }

    [Fact]
    public void ReadWrite_EmptyBytes_RoundTripsSuccessfully()
    {
        using var op = new Operator("memory");
        var content = Array.Empty<byte>();

        op.Write("test-empty-bytes", content);
        var result = op.Read("test-empty-bytes");

        Assert.Empty(result);
    }

    [Fact]
    public void Read_NonUtf8Bytes_ThrowsDecoderFallbackExceptionOnStrictDecode()
    {
        using var op = new Operator("memory");
        byte[] nonUtf8 = [0xFF, 0xFE, 0x00, 0xC3, 0x28];

        op.Write("test-non-utf8", nonUtf8);
        var resultBytes = op.Read("test-non-utf8");

        var strictUtf8 = new System.Text.UTF8Encoding(false, true);
        Assert.Throws<System.Text.DecoderFallbackException>(() => strictUtf8.GetString(resultBytes));
    }

    [Fact]
    public void Read_PathNotExists_ThrowsNotFoundError()
    {
        using var op = new Operator("memory");

        var ex = Assert.Throws<OpenDALException>(() => op.Read("path-not-exists"));

        Assert.Equal(ErrorCode.NotFound, ex.Code);
        Assert.Contains("path-not-exists", ex.Message);
    }

    [Fact]
    public async Task ReadWriteAsync_Utf8Bytes_RoundTripsSuccessfully()
    {
        using var op = new Operator("memory");
        var content = "abcdef";
        var bytes = System.Text.Encoding.UTF8.GetBytes(content);

        await op.WriteAsync("test-async", bytes);
        var resultBytes = await op.ReadAsync("test-async");
        var result = System.Text.Encoding.UTF8.GetString(resultBytes);

        Assert.Equal(content, result);
    }

    [Fact]
    public async Task ReadWriteAsync_BinaryBytes_RoundTripsSuccessfully()
    {
        using var op = new Operator("memory");
        byte[] content = [0x00, 0x01, 0x02, 0x7F, 0x80, 0xFE, 0xFF];

        await op.WriteAsync("test-async-bytes", content);
        var result = await op.ReadAsync("test-async-bytes");

        Assert.Equal(content, result);
    }

    [Fact]
    public async Task ReadWriteAsync_EmptyBytes_RoundTripsSuccessfully()
    {
        using var op = new Operator("memory");
        var content = Array.Empty<byte>();

        await op.WriteAsync("test-async-empty-bytes", content);
        var result = await op.ReadAsync("test-async-empty-bytes");

        Assert.Empty(result);
    }

    [Fact]
    public async Task ReadAsync_PathNotExists_ThrowsNotFoundError()
    {
        using var op = new Operator("memory");

        var ex = await Assert.ThrowsAsync<OpenDALException>(async () => await op.ReadAsync("path-not-exists-async"));

        Assert.Equal(ErrorCode.NotFound, ex.Code);
        Assert.Contains("path-not-exists-async", ex.Message);
    }

    [Fact]
    public async Task ReadWriteAsync_CancellationRequested_ThrowsOperationCanceledException()
    {
        using var op = new Operator("memory");
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAsync<OperationCanceledException>(async () =>
            await op.WriteAsync("test-cancel-write", [1, 2, 3], cts.Token));
        await Assert.ThrowsAsync<OperationCanceledException>(async () =>
            await op.ReadAsync("test-cancel-read", cts.Token));
    }

    [Fact]
    public async Task ReadWriteAsync_CancelAfterDispatch_DoesNotBreakSubsequentOperations()
    {
        using var op = new Operator("memory");
        var seed = System.Text.Encoding.UTF8.GetBytes("seed-content");
        await op.WriteAsync("seed", seed);

        using (var writeCts = new CancellationTokenSource())
        {
            var writeTask = op.WriteAsync("late-cancel-write", [1, 2, 3, 4], writeCts.Token);
            writeCts.Cancel();

            try
            {
                await writeTask;
            }
            catch (OperationCanceledException)
            {
            }
        }

        using (var readCts = new CancellationTokenSource())
        {
            var readTask = op.ReadAsync("seed", readCts.Token);
            readCts.Cancel();

            try
            {
                _ = await readTask;
            }
            catch (OperationCanceledException)
            {
            }
        }

        var stableRead = await op.ReadAsync("seed");
        Assert.Equal("seed-content", System.Text.Encoding.UTF8.GetString(stableRead));
    }

    [Fact]
    public async Task ReadWriteAsync_DisposeRace_DoesNotCrashProcess()
    {
        var op = new Operator("memory");
        byte[] content = [1, 2, 3, 4, 5, 6, 7, 8];
        await op.WriteAsync("seed-async", content);

        var workers = Enumerable.Range(0, 64).Select(async i =>
        {
            var path = $"race-async-{i % 8}";

            try
            {
                await op.WriteAsync(path, content);
                _ = await op.ReadAsync(path);
            }
            catch (ObjectDisposedException)
            {
            }
            catch (OpenDALException)
            {
            }
        });

        var dispose = Task.Run(op.Dispose);
        await Task.WhenAll(workers.Append(dispose));
    }

    [Fact]
    public async Task ReadAsync_PathNotExists_RepeatedErrors_DoNotPoisonSubsequentCalls()
    {
        using var op = new Operator("memory");

        for (var i = 0; i < 32; i++)
        {
            var ex = await Assert.ThrowsAsync<OpenDALException>(() => op.ReadAsync($"missing-{i}"));
            Assert.Equal(ErrorCode.NotFound, ex.Code);
        }

        var content = System.Text.Encoding.UTF8.GetBytes("healthy");
        await op.WriteAsync("healthy", content);
        var read = await op.ReadAsync("healthy");
        Assert.Equal("healthy", System.Text.Encoding.UTF8.GetString(read));
    }
}
