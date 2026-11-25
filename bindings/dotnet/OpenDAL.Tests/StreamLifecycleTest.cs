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
using OpenDAL.Layer;
using OpenDAL.Options;

namespace OpenDAL.Tests;

public sealed class StreamLifecycleTest
{
    private static CancellationToken CT => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(0)]
    [InlineData(-16)]
    public void OpenWriteStream_NonPositiveBufferSize_ValidatesBeforeNativeCreation(int bufferSize)
    {
        using var op = new Operator("memory");

        // A directory path is rejected by native stream creation with OpenDALException,
        // so receiving ArgumentOutOfRangeException proves bufferSize is validated first.
        Assert.Throws<OpenDALException>(() => op.OpenWriteStream("dir/"));
        Assert.Throws<ArgumentOutOfRangeException>(() => op.OpenWriteStream("dir/", bufferSize: bufferSize));
    }

    [Fact]
    public void UndisposedStreams_GcFinalizationDoesNotCommitIncompleteWritesAndAllowsOperatorDispose()
    {
        using var executor = new Executor(1);
        using var op = new Operator("memory", executor: executor);

        var sourcePayload = new byte[64 * 1024];
        Random.Shared.NextBytes(sourcePayload);
        op.Write("finalizer-read-source", sourcePayload);

        var (inputRef, outputRef) = OpenStreamsWithoutDisposing(op);

        ForceFullCollectionUntilCollected(inputRef, outputRef);

        Assert.False(inputRef.IsAlive);
        Assert.False(outputRef.IsAlive);
        // Finalization must only free the native output stream handle and must
        // never perform blocking I/O or commit uncommitted bytes.
        Assert.False(op.Exists("finalizer-uncommitted-output"));
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static (WeakReference InputRef, WeakReference OutputRef) OpenStreamsWithoutDisposing(Operator op)
    {
        var input = op.OpenReadStream("finalizer-read-source");
        // Pull a chunk into currentChunk and leave the rest unread so finalization
        // must release both the native chunk buffer and the stream handle.
        Assert.NotEqual(-1, input.ReadByte());

        var output = op.OpenWriteStream("finalizer-uncommitted-output");
        output.Write("uncommitted"u8);

        return (new WeakReference(input), new WeakReference(output));
    }

    [Fact]
    public void AsyncState_DisposeCancellationBeforeBind_DoesNotLeakRegistration()
    {
        using var cts = new CancellationTokenSource();
        var weakRef = CreateCompletedThenBoundAsyncState(cts.Token);

        ForceFullCollectionUntilCollected(weakRef);

        Assert.False(weakRef.IsAlive);
        GC.KeepAlive(cts);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static WeakReference CreateCompletedThenBoundAsyncState(CancellationToken token)
    {
        var state = new AsyncState<int>();
        state.DisposeCancellation();
        state.Completion.TrySetResult(42);
        state.BindCancellation(token);
        return new WeakReference(state);
    }

    [Fact]
    public void AsyncState_ConcurrentBindAndDisposeCancellation_DoesNotLeakRegistrations()
    {
        using var cts = new CancellationTokenSource();
        var refs = RunConcurrentBindAndDisposeRace(cts.Token);

        ForceFullCollectionUntilCollected(refs);

        Assert.All(refs, r => Assert.False(r.IsAlive));
        GC.KeepAlive(cts);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static WeakReference[] RunConcurrentBindAndDisposeRace(CancellationToken token)
    {
        const int iterations = 1000;
        var refs = new WeakReference[iterations];

        for (var i = 0; i < iterations; i++)
        {
            refs[i] = RunSingleBindAndDisposeRace(token);
        }

        return refs;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static WeakReference RunSingleBindAndDisposeRace(CancellationToken token)
    {
        var state = new AsyncState<int>();
        using var gate = new ManualResetEventSlim(false);

        var disposeTask = Task.Run(() =>
        {
            gate.Wait();
            state.DisposeCancellation();
            state.Completion.TrySetResult(1);
        });

        var bindTask = Task.Run(() =>
        {
            gate.Wait();
            state.BindCancellation(token);
        });

        gate.Set();
        Task.WaitAll(disposeTask, bindTask);
        return new WeakReference(state);
    }

    [Fact]
    public void AsyncOperations_SharedLongLivedCancellationToken_ReclaimsCompletedTasks()
    {
        using var op = new Operator("memory");
        op.Write("shared-cts-file", "hello"u8.ToArray());
        using var cts = new CancellationTokenSource();

        var taskRefs = RunConcurrentOpsWithSharedToken(op, cts.Token);

        ForceFullCollectionUntilCollected(taskRefs);

        Assert.All(taskRefs, r => Assert.False(r.IsAlive));
        GC.KeepAlive(cts);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static WeakReference[] RunConcurrentOpsWithSharedToken(Operator op, CancellationToken token)
    {
        const int count = 500;
        var tasks = new Task[count];
        var refs = new WeakReference[count];

        for (var i = 0; i < count; i++)
        {
            var task = (i & 1) == 0
                ? (Task)op.ExistsAsync("shared-cts-file", token)
                : op.ReadAsync("shared-cts-file", cancellationToken: token);
            tasks[i] = task;
            refs[i] = new WeakReference(task);
        }

        Task.WaitAll(tasks);
        return refs;
    }

    [Fact]
    public void OutputStream_FailedFlushBuffered_PoisonsStreamAndDoesNotReplayOnDispose()
    {
        using var baseOp = new Operator("memory");
        using var throttled = baseOp.WithLayer(new ThrottleLayer(bandwidth: 4, burst: 4));

        var output = throttled.OpenWriteStream("failed-flush-sync", bufferSize: 16);
        try
        {
            // 8 bytes exceeds the 4-byte burst, so native write fails with RateLimited.
            output.Write(new byte[8]);
            var ex = Assert.Throws<OpenDALException>(output.Flush);
            Assert.Equal(ErrorCode.RateLimited, ex.Code);

            Assert.False(output.CanWrite);
            Assert.Throws<InvalidOperationException>(() => output.Write(new byte[1]));
            Assert.Throws<InvalidOperationException>(output.Flush);
            Assert.Throws<InvalidOperationException>(output.Complete);
        }
        finally
        {
            output.Dispose();
        }

        Assert.False(baseOp.Exists("failed-flush-sync"));
    }

    [Fact]
    public async Task OutputStream_CanceledCompleteAsync_PoisonsStreamAndDoesNotCommitOnDisposeAsync()
    {
        using var baseOp = new Operator("memory");
        using var throttled = baseOp.WithLayer(new ThrottleLayer(bandwidth: 1, burst: 4));

        var output = throttled.OpenWriteStream("canceled-complete-async", bufferSize: 16);
        try
        {
            // Consume the initial 4-byte burst so the next 4-byte flush waits on the throttle.
            await output.WriteAsync(new byte[4], CT);
            await output.FlushAsync(CT);

            await output.WriteAsync(new byte[4], CT);
            using var cancelCts = CancellationTokenSource.CreateLinkedTokenSource(CT);
            // CompleteAsync submits the buffered flush before returning its pending task.
            var completion = output.CompleteAsync(cancelCts.Token);
            Assert.False(completion.IsCompleted);
            cancelCts.Cancel();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => completion);

            Assert.False(output.CanWrite);
            await Assert.ThrowsAsync<InvalidOperationException>(async () => await output.WriteAsync(new byte[1], CT));
            await Assert.ThrowsAsync<InvalidOperationException>(() => output.CompleteAsync(CT));
        }
        finally
        {
            await output.DisposeAsync();
        }

        Assert.False(await baseOp.ExistsAsync("canceled-complete-async", CT));
    }

    [Fact]
    public async Task OutputStream_FailedCloseAsync_PoisonsStreamAndDoesNotRetryOnDisposeAsync()
    {
        using var op = new Operator("memory");
        const string path = "failed-close-async";
        await op.WriteAsync(path, new byte[16], cancellationToken: CT);

        var output = op.OpenWriteStream(path, new WriteOptions { IfNotExists = true });
        try
        {
            await output.WriteAsync(new byte[32], CT);
            // The memory backend checks IfNotExists on close, so flushing first
            // guarantees CompleteAsync reaches native close_async with an empty buffer.
            await output.FlushAsync(CT);

            var ex = await Assert.ThrowsAsync<OpenDALException>(() => output.CompleteAsync(CT));
            Assert.Equal(ErrorCode.ConditionNotMatch, ex.Code);
            Assert.False(output.CanWrite);
            await Assert.ThrowsAsync<InvalidOperationException>(async () => await output.WriteAsync(new byte[1], CT));
            await Assert.ThrowsAsync<InvalidOperationException>(() => output.FlushAsync(CT));
            await Assert.ThrowsAsync<InvalidOperationException>(() => output.CompleteAsync(CT));

            // Remove the existing object before disposal so a retry of close_async()
            // during DisposeAsync() would visibly recreate the path.
            await op.DeleteAsync(path, cancellationToken: CT);
        }
        finally
        {
            await output.DisposeAsync();
        }

        Assert.False(await op.ExistsAsync(path, CT));
    }

    private static void ForceFullCollectionUntilCollected(params WeakReference[] refs)
    {
        for (var attempt = 0; attempt < 20; attempt++)
        {
            GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, blocking: true, compacting: true);
            GC.WaitForPendingFinalizers();
            GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, blocking: true, compacting: true);

            var anyAlive = false;
            foreach (var r in refs)
            {
                if (r.IsAlive)
                {
                    anyAlive = true;
                    break;
                }
            }

            if (!anyAlive)
            {
                return;
            }

            Thread.Sleep(10);
        }
    }
}
