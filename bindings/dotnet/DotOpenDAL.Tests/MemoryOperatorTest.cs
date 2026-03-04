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

using DotOpenDAL.ServiceConfig;
using DotOpenDAL.Options;

namespace DotOpenDAL.Tests;

public class MemoryOperatorTest
{
    [Fact]
    public void Info_MemoryConfig_ReturnsSchemeRootAndName()
    {
        var config = new MemoryServiceConfig
        {
            Root = "/tmp",
        };

        using var op = new Operator(config);
        var info = op.Info;

        Assert.Equal("memory", info.Scheme);
        Assert.Equal("/tmp", info.Root.TrimEnd('/'));
        Assert.False(string.IsNullOrEmpty(info.Name));
        Assert.True(info.FullCapability.Read);
        Assert.True(info.FullCapability.Write);
        Assert.True(info.NativeCapability.Read);
        Assert.True(info.NativeCapability.Write);
    }

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
            await op.WriteAsync("test-cancel-write", [1, 2, 3], cts.Token)
        );
        await Assert.ThrowsAsync<OperationCanceledException>(async () =>
            await op.ReadAsync("test-cancel-read", cts.Token)
        );
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

    [Fact]
    public void Read_WithOptions_OffsetAndLength_ReturnsRequestedRange()
    {
        using var op = new Operator("memory");
        var content = System.Text.Encoding.UTF8.GetBytes("abcdef");
        op.Write("read-range", content);

        var result = op.Read("read-range", new ReadOptions
        {
            Offset = 2,
            Length = 3,
        });

        Assert.Equal("cde", System.Text.Encoding.UTF8.GetString(result));
    }

    [Fact]
    public async Task ReadAsync_WithOptions_OffsetAndLength_ReturnsRequestedRange()
    {
        using var op = new Operator("memory");
        var content = System.Text.Encoding.UTF8.GetBytes("abcdef");
        await op.WriteAsync("read-range-async", content);

        var result = await op.ReadAsync("read-range-async", new ReadOptions
        {
            Offset = 1,
            Length = 4,
        });

        Assert.Equal("bcde", System.Text.Encoding.UTF8.GetString(result));
    }

    [Fact]
    public void Write_WithOptions_Append_AppendsWhenSupported()
    {
        using var op = new Operator("memory");
        if (!op.Info.FullCapability.WriteCanAppend)
        {
            return;
        }

        op.Write("append-sync", System.Text.Encoding.UTF8.GetBytes("a"));
        op.Write("append-sync", System.Text.Encoding.UTF8.GetBytes("b"), new WriteOptions
        {
            Append = true,
        });

        var result = System.Text.Encoding.UTF8.GetString(op.Read("append-sync"));
        Assert.Equal("ab", result);
    }

    [Fact]
    public async Task WriteAsync_WithOptions_Append_AppendsWhenSupported()
    {
        using var op = new Operator("memory");
        if (!op.Info.FullCapability.WriteCanAppend)
        {
            return;
        }

        await op.WriteAsync("append-async", System.Text.Encoding.UTF8.GetBytes("x"));
        await op.WriteAsync("append-async", System.Text.Encoding.UTF8.GetBytes("y"), new WriteOptions
        {
            Append = true,
        });

        var result = System.Text.Encoding.UTF8.GetString(await op.ReadAsync("append-async"));
        Assert.Equal("xy", result);
    }

    [Fact]
    public void Write_WithOptions_IfNotExists_FailsOnExistingPathWhenSupported()
    {
        using var op = new Operator("memory");
        if (!op.Info.FullCapability.WriteWithIfNotExists)
        {
            return;
        }

        op.Write("if-not-exists", System.Text.Encoding.UTF8.GetBytes("first"));
        var ex = Assert.Throws<OpenDALException>(() =>
            op.Write("if-not-exists", System.Text.Encoding.UTF8.GetBytes("second"), new WriteOptions
            {
                IfNotExists = true,
            })
        );

        Assert.Equal(ErrorCode.ConditionNotMatch, ex.Code);
    }

    [Fact]
    public void Stat_WithOptions_ReturnsMetadata()
    {
        using var op = new Operator("memory");
        var payload = System.Text.Encoding.UTF8.GetBytes("metadata");
        op.Write("stat-sync", payload);

        var metadata = op.Stat("stat-sync");

        Assert.True(metadata.IsFile);
        Assert.Equal((ulong)payload.Length, metadata.ContentLength);
    }

    [Fact]
    public async Task StatAsync_WithOptions_ReturnsMetadata()
    {
        using var op = new Operator("memory");
        var payload = System.Text.Encoding.UTF8.GetBytes("metadata-async");
        await op.WriteAsync("stat-async", payload);

        var metadata = await op.StatAsync("stat-async");

        Assert.True(metadata.IsFile);
        Assert.Equal((ulong)payload.Length, metadata.ContentLength);
    }

    [Fact]
    public void Stat_WithOptions_IfModifiedSince_FailsWhenConditionNotMatched()
    {
        using var op = new Operator("memory");
        if (!op.Info.FullCapability.StatWithIfModifiedSince)
        {
            return;
        }

        op.Write("stat-condition", System.Text.Encoding.UTF8.GetBytes("value"));

        var ex = Assert.Throws<OpenDALException>(() =>
            op.Stat("stat-condition", new StatOptions
            {
                IfModifiedSince = DateTimeOffset.UtcNow.AddDays(1),
            })
        );

        Assert.Equal(ErrorCode.ConditionNotMatch, ex.Code);
    }

    [Fact]
    public void List_WithOptions_Recursive_WorksAsExpected()
    {
        using var op = new Operator("memory");
        if (!op.Info.FullCapability.List)
        {
            return;
        }

        op.Write("list-options/alpha.txt", System.Text.Encoding.UTF8.GetBytes("a"));
        op.Write("list-options/nested/beta.txt", System.Text.Encoding.UTF8.GetBytes("b"));

        var shallow = op.List("list-options/", new ListOptions { Recursive = false });
        var deep = op.List("list-options/", new ListOptions { Recursive = true });

        Assert.Contains(shallow, entry => entry.Path == "list-options/alpha.txt");
        Assert.DoesNotContain(shallow, entry => entry.Path == "list-options/nested/beta.txt");
        Assert.Contains(deep, entry => entry.Path == "list-options/alpha.txt");
        Assert.Contains(deep, entry => entry.Path == "list-options/nested/beta.txt");
    }

    [Fact]
    public void List_WithOptions_Limit_AppliesWhenSupported()
    {
        using var op = new Operator("memory");
        if (!op.Info.FullCapability.List || !op.Info.FullCapability.ListWithLimit)
        {
            return;
        }

        for (var i = 0; i < 8; i++)
        {
            op.Write($"list-limit/file-{i}.txt", System.Text.Encoding.UTF8.GetBytes(i.ToString()));
        }

        var entries = op.List("list-limit/", new ListOptions
        {
            Recursive = true,
            Limit = 3,
        });

        Assert.True(entries.Count <= 3);
    }

    [Fact]
    public async Task ListAsync_WithOptions_Recursive_WorksAsExpected()
    {
        using var op = new Operator("memory");
        if (!op.Info.FullCapability.List)
        {
            return;
        }

        await op.WriteAsync("list-options-async/alpha.txt", System.Text.Encoding.UTF8.GetBytes("a"));
        await op.WriteAsync("list-options-async/nested/beta.txt", System.Text.Encoding.UTF8.GetBytes("b"));

        var entries = await op.ListAsync("list-options-async/", new ListOptions
        {
            Recursive = true,
        });

        Assert.Contains(entries, entry => entry.Path == "list-options-async/alpha.txt");
        Assert.Contains(entries, entry => entry.Path == "list-options-async/nested/beta.txt");
    }

    [Fact]
    public void Duplicate_ReturnsIndependentOperatorHandle()
    {
        using var op = new Operator("memory");
        using var duplicated = op.Duplicate();

        duplicated.Write("duplicate-sync", System.Text.Encoding.UTF8.GetBytes("ok"));
        var result = op.Read("duplicate-sync");

        Assert.Equal("ok", System.Text.Encoding.UTF8.GetString(result));
    }

    [Fact]
    public void CreateDirDeleteCopyRenameRemoveAll_Sync_WorksAsExpected()
    {
        using var op = new Operator("memory");

        op.CreateDir("sync-dir/");
        var dirMeta = op.Stat("sync-dir/");
        Assert.True(dirMeta.IsDir);

        op.Write("sync-dir/source.txt", System.Text.Encoding.UTF8.GetBytes("payload"));

        if (op.Info.FullCapability.Copy)
        {
            op.Copy("sync-dir/source.txt", "sync-dir/copied.txt");
            var copied = op.Read("sync-dir/copied.txt");
            Assert.Equal("payload", System.Text.Encoding.UTF8.GetString(copied));
        }

        if (op.Info.FullCapability.Rename)
        {
            var renameSource = op.Info.FullCapability.Copy ? "sync-dir/copied.txt" : "sync-dir/source.txt";
            op.Rename(renameSource, "sync-dir/renamed.txt");
            Assert.Throws<OpenDALException>(() => op.Read(renameSource));
            var renamed = op.Read("sync-dir/renamed.txt");
            Assert.Equal("payload", System.Text.Encoding.UTF8.GetString(renamed));
            op.Delete("sync-dir/renamed.txt");
            Assert.Throws<OpenDALException>(() => op.Read("sync-dir/renamed.txt"));
        }
        else
        {
            op.Delete("sync-dir/source.txt");
            Assert.Throws<OpenDALException>(() => op.Read("sync-dir/source.txt"));
        }

        op.RemoveAll("sync-dir/");
        var entries = op.List("sync-dir/", new ListOptions { Recursive = true });
        Assert.Empty(entries);
    }

    [Fact]
    public async Task CreateDirDeleteCopyRenameRemoveAll_Async_WorksAsExpected()
    {
        using var op = new Operator("memory");

        await op.CreateDirAsync("async-dir/");
        var dirMeta = await op.StatAsync("async-dir/");
        Assert.True(dirMeta.IsDir);

        await op.WriteAsync("async-dir/source.txt", System.Text.Encoding.UTF8.GetBytes("payload"));

        if (op.Info.FullCapability.Copy)
        {
            await op.CopyAsync("async-dir/source.txt", "async-dir/copied.txt");
            var copied = await op.ReadAsync("async-dir/copied.txt");
            Assert.Equal("payload", System.Text.Encoding.UTF8.GetString(copied));
        }

        if (op.Info.FullCapability.Rename)
        {
            var renameSource = op.Info.FullCapability.Copy ? "async-dir/copied.txt" : "async-dir/source.txt";
            await op.RenameAsync(renameSource, "async-dir/renamed.txt");
            await Assert.ThrowsAsync<OpenDALException>(async () => await op.ReadAsync(renameSource));
            var renamed = await op.ReadAsync("async-dir/renamed.txt");
            Assert.Equal("payload", System.Text.Encoding.UTF8.GetString(renamed));
            await op.DeleteAsync("async-dir/renamed.txt");
            await Assert.ThrowsAsync<OpenDALException>(async () => await op.ReadAsync("async-dir/renamed.txt"));
        }
        else
        {
            await op.DeleteAsync("async-dir/source.txt");
            await Assert.ThrowsAsync<OpenDALException>(async () => await op.ReadAsync("async-dir/source.txt"));
        }

        await op.RemoveAllAsync("async-dir/");
        var entries = await op.ListAsync("async-dir/", new ListOptions { Recursive = true });
        Assert.Empty(entries);
    }

    [Fact]
    public void Delete_PathNotExists_IsIdempotent()
    {
        using var op = new Operator("memory");

        op.Delete("not-exists-delete-sync");
        op.Write("delete-idempotent-sync", System.Text.Encoding.UTF8.GetBytes("ok"));
        var result = op.Read("delete-idempotent-sync");
        Assert.Equal("ok", System.Text.Encoding.UTF8.GetString(result));
    }

    [Fact]
    public async Task DeleteAsync_PathNotExists_IsIdempotent()
    {
        using var op = new Operator("memory");

        await op.DeleteAsync("not-exists-delete-async");
        await op.WriteAsync("delete-idempotent-async", System.Text.Encoding.UTF8.GetBytes("ok"));
        var result = await op.ReadAsync("delete-idempotent-async");
        Assert.Equal("ok", System.Text.Encoding.UTF8.GetString(result));
    }

    [Fact]
    public void OpenReadWriteStream_RoundTripsSuccessfully()
    {
        using var op = new Operator("memory");
        var content = System.Text.Encoding.UTF8.GetBytes("stream-content");

        using (var output = op.OpenWriteStream("stream.txt"))
        {
            output.Write(content, 0, content.Length);
            output.Flush();
        }

        using var input = op.OpenReadStream("stream.txt");
        var buffer = new byte[content.Length];
        var read = input.Read(buffer, 0, buffer.Length);

        Assert.Equal(content.Length, read);
        Assert.Equal(content, buffer);
        Assert.Equal(0, input.Read(buffer, 0, buffer.Length));
    }

    [Fact]
    public void OpenReadStream_WithReadOptionsRange_ReadsSelectedSlice()
    {
        using var op = new Operator("memory");
        op.Write("stream-range.txt", System.Text.Encoding.UTF8.GetBytes("0123456789"));

        using var input = op.OpenReadStream("stream-range.txt", new ReadOptions
        {
            Offset = 3,
            Length = 4,
        });

        var buffer = new byte[8];
        var read = input.Read(buffer, 0, buffer.Length);

        Assert.Equal(4, read);
        Assert.Equal("3456", System.Text.Encoding.UTF8.GetString(buffer, 0, read));
    }

    [Fact]
    public async Task OpenReadWriteStream_AsyncMethods_WorkAsExpected()
    {
        using var op = new Operator("memory");
        var content = System.Text.Encoding.UTF8.GetBytes("stream-async-content");

        using (var output = op.OpenWriteStream("stream-async.txt"))
        {
            await output.WriteAsync(content, 0, content.Length);
            await output.FlushAsync();
        }

        using var input = op.OpenReadStream("stream-async.txt");
        var buffer = new byte[content.Length];
        var read = await input.ReadAsync(buffer, 0, buffer.Length);

        Assert.Equal(content.Length, read);
        Assert.Equal(content, buffer);
    }

    [Fact]
    public async Task PresignAsync_RespectsCapabilityFlags()
    {
        using var op = new Operator("memory");
        var capability = op.Info.FullCapability;
        var expiration = TimeSpan.FromMinutes(5);

        await AssertPresign(
            () => op.PresignReadAsync("presign-read", expiration),
            capability.PresignRead
        );
        await AssertPresign(
            () => op.PresignWriteAsync("presign-write", expiration),
            capability.PresignWrite
        );
        await AssertPresign(
            () => op.PresignStatAsync("presign-stat", expiration),
            capability.PresignStat
        );
        await AssertPresign(
            () => op.PresignDeleteAsync("presign-delete", expiration),
            capability.PresignDelete
        );

        static async Task AssertPresign(Func<Task<PresignedRequest>> presignAction, bool supported)
        {
            if (supported)
            {
                var request = await presignAction();
                Assert.False(string.IsNullOrWhiteSpace(request.Method));
                Assert.False(string.IsNullOrWhiteSpace(request.Uri));
                return;
            }

            var ex = await Assert.ThrowsAsync<OpenDALException>(presignAction);
            Assert.Equal(ErrorCode.Unsupported, ex.Code);
        }
    }
}
