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

public class FsOperatorTest
{
    [Fact]
    public void ReadWrite_RootOptionProvided_RoundTripsSuccessfully()
    {
        var root = Path.Combine(Path.GetTempPath(), $"opendal-dotnet-test-{Guid.NewGuid():N}");
        Directory.CreateDirectory(root);

        try
        {
            var options = new Dictionary<string, string>
            {
                ["root"] = root,
            };

            using var op = new Operator("fs", options);
            var path = "nested/test-fs.txt";
            var content = "hello-from-fs";
            var bytes = System.Text.Encoding.UTF8.GetBytes(content);

            op.Write(path, bytes);
            var result = op.Read(path);

            Assert.Equal(content, System.Text.Encoding.UTF8.GetString(result));
            Assert.True(File.Exists(Path.Combine(root, "nested", "test-fs.txt")));
        }
        finally
        {
            if (Directory.Exists(root))
            {
                Directory.Delete(root, recursive: true);
            }
        }
    }

    [Fact]
    public async Task ReadWriteAsync_RootOptionProvided_RoundTripsSuccessfully()
    {
        var root = Path.Combine(Path.GetTempPath(), $"opendal-dotnet-test-async-{Guid.NewGuid():N}");
        Directory.CreateDirectory(root);

        try
        {
            var options = new Dictionary<string, string>
            {
                ["root"] = root,
            };

            using var op = new Operator("fs", options);
            var path = "nested/test-fs-async.txt";
            var content = "hello-from-fs-async";
            var bytes = System.Text.Encoding.UTF8.GetBytes(content);

            await op.WriteAsync(path, bytes);
            var result = await op.ReadAsync(path);

            Assert.Equal(content, System.Text.Encoding.UTF8.GetString(result));
            Assert.True(File.Exists(Path.Combine(root, "nested", "test-fs-async.txt")));
        }
        finally
        {
            if (Directory.Exists(root))
            {
                Directory.Delete(root, recursive: true);
            }
        }
    }

    [Fact]
    public async Task ReadWriteAsync_CancelAfterDispatch_DoesNotBreakSubsequentOperations()
    {
        var root = Path.Combine(Path.GetTempPath(), $"opendal-dotnet-test-async-cancel-{Guid.NewGuid():N}");
        Directory.CreateDirectory(root);

        try
        {
            var options = new Dictionary<string, string>
            {
                ["root"] = root,
            };

            using var op = new Operator("fs", options);
            var seedPath = "nested/seed.txt";
            var seedBytes = System.Text.Encoding.UTF8.GetBytes("seed-content");
            await op.WriteAsync(seedPath, seedBytes);

            using (var writeCts = new CancellationTokenSource())
            {
                var writeTask = op.WriteAsync("nested/late-cancel-write.txt", [1, 2, 3, 4], writeCts.Token);
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
                var readTask = op.ReadAsync(seedPath, readCts.Token);
                readCts.Cancel();

                try
                {
                    _ = await readTask;
                }
                catch (OperationCanceledException)
                {
                }
            }

            var stableRead = await op.ReadAsync(seedPath);
            Assert.Equal("seed-content", System.Text.Encoding.UTF8.GetString(stableRead));
        }
        finally
        {
            if (Directory.Exists(root))
            {
                Directory.Delete(root, recursive: true);
            }
        }
    }
}
