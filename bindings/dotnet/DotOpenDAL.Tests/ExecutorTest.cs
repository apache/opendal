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

public class ExecutorTest
{
    private static CancellationToken CT => TestContext.Current.CancellationToken;

    [Fact]
    public void CreateExecutor_InvalidCores_ThrowsArgumentOutOfRangeException()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new Executor(0));
    }

    [Fact]
    public void ReadWrite_WithDedicatedExecutor_RoundTripsSuccessfully()
    {
        using var executor = new Executor(1);
        using var op = new Operator("memory");
        var content = System.Text.Encoding.UTF8.GetBytes("executor-content");

        op.Write("executor-sync", content, executor);
        var read = op.Read("executor-sync", executor);

        Assert.Equal(content, read);
    }

    [Fact]
    public async Task ReadWriteAsync_WithDedicatedExecutor_RoundTripsSuccessfully()
    {
        using var executor = new Executor(1);
        using var op = new Operator("memory");
        var content = System.Text.Encoding.UTF8.GetBytes("executor-async-content");

        await op.WriteAsync("executor-async", content, executor, CT);
        var read = await op.ReadAsync("executor-async", executor, CT);

        Assert.Equal(content, read);
    }

    [Fact]
    public async Task OperatorCalls_WithDisposedExecutor_ThrowObjectDisposedException()
    {
        var executor = new Executor(1);
        using var op = new Operator("memory");
        var content = new byte[] { 1, 2, 3 };
        executor.Dispose();

        Assert.Throws<ObjectDisposedException>(() => op.Write("disposed-executor", content, executor));
        Assert.Throws<ObjectDisposedException>(() => op.Read("disposed-executor", executor));
        await Assert.ThrowsAsync<ObjectDisposedException>(() => op.WriteAsync("disposed-executor", content, executor, CT));
        await Assert.ThrowsAsync<ObjectDisposedException>(() => op.ReadAsync("disposed-executor", executor, CT));
    }

    [Fact]
    public void ReadWrite_WithProcessorCountThreads_RoundTripsSuccessfully()
    {
        var threads = Math.Max(1, Environment.ProcessorCount);
        using var executor = new Executor(threads);
        using var op = new Operator("memory");
        var content = System.Text.Encoding.UTF8.GetBytes($"executor-threads-{threads}");

        op.Write("executor-processor-sync", content, executor);
        var read = op.Read("executor-processor-sync", executor);

        Assert.Equal(content, read);
    }

    [Fact]
    public async Task ReadWriteAsync_WithProcessorCountThreads_ParallelOperationsSucceed()
    {
        var threads = Math.Max(1, Environment.ProcessorCount);
        var operationCount = Math.Min(threads * 2, 64);

        using var executor = new Executor(threads);
        using var op = new Operator("memory");

        var tasks = Enumerable.Range(0, operationCount).Select(async i =>
        {
            var path = $"executor-processor-async-{i}";
            var content = System.Text.Encoding.UTF8.GetBytes($"executor-content-{i}");

            await op.WriteAsync(path, content, executor, CT);
            var read = await op.ReadAsync(path, executor, CT);

            Assert.Equal(content, read);
        });

        await Task.WhenAll(tasks);
    }
}
