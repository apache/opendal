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

using DotOpenDAL.Options;

namespace DotOpenDAL.Tests;

[Collection("BehaviorOperator")]
public sealed class OperatorBehaviorTest : BehaviorTestBase
{
    private static CancellationToken CT => TestContext.Current.CancellationToken;

    public OperatorBehaviorTest(BehaviorOperatorFixture fixture)
        : base(fixture)
    {
    }

    [Fact]
    public void OperatorBehavior_Info_IsAvailable()
    {
        if (!IsEnabled)
        {
            return;
        }

        var info = Op.Info;

        Assert.False(string.IsNullOrWhiteSpace(info.Scheme));
        Assert.False(string.IsNullOrWhiteSpace(info.Name));
        Assert.True(info.FullCapability.Read || info.FullCapability.Write);
    }

    [Fact]
    public void OperatorBehavior_Duplicate_SharesBackendState()
    {
        if (!Supports(c => c.Read && c.Write))
        {
            return;
        }

        using var duplicated = Op.Duplicate();

        var path = NewPath("duplicate");
        var content = RandomBytes(32);

        duplicated.Write(path, content);
        Assert.Equal(content, Op.Read(path));
    }

    [Fact]
    public void OperatorBehavior_StreamRoundtrip_Works()
    {
        if (!Supports(c => c.Read && c.Write))
        {
            return;
        }

        var path = NewPath("stream-sync");
        var content = RandomBytes(64);

        using (var output = Op.OpenWriteStream(path))
        {
            output.Write(content, 0, content.Length);
            output.Flush();
        }

        using var input = Op.OpenReadStream(path);
        var buffer = new byte[content.Length];
        var read = input.Read(buffer, 0, buffer.Length);

        Assert.Equal(content.Length, read);
        Assert.Equal(content, buffer);
    }

    [Fact]
    public async Task OperatorBehavior_StreamRoundtripAsync_Works()
    {
        if (!Supports(c => c.Read && c.Write))
        {
            return;
        }

        var path = NewPath("stream-async");
        var content = RandomBytes(64);

        using (var output = Op.OpenWriteStream(path))
        {
            await output.WriteAsync(content, 0, content.Length, CT);
            await output.FlushAsync(CT);
        }

        using var input = Op.OpenReadStream(path);
        var buffer = new byte[content.Length];
        var read = await input.ReadAsync(buffer, 0, buffer.Length, CT);

        Assert.Equal(content.Length, read);
        Assert.Equal(content, buffer);
    }

    [Fact]
    public void OperatorBehavior_OpenReadStream_WithRange_ReadsSelectedSlice()
    {
        if (!Supports(c => c.Read && c.Write))
        {
            return;
        }

        var path = NewPath("stream-range");
        Op.Write(path, System.Text.Encoding.UTF8.GetBytes("0123456789"));

        using var input = Op.OpenReadStream(path, new ReadOptions
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
    public async Task OperatorBehavior_CancelAfterDispatch_DoesNotBreakSubsequentOperations()
    {
        if (!Supports(c => c.Read && c.Write))
        {
            return;
        }

        var path = NewPath("cancel-seed");
        await Op.WriteAsync(path, System.Text.Encoding.UTF8.GetBytes("seed-content"), CT);

        using (var writeCts = new CancellationTokenSource())
        {
            var writeTask = Op.WriteAsync(NewPath("cancel-write"), [1, 2, 3, 4], writeCts.Token);
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
            var readTask = Op.ReadAsync(path, readCts.Token);
            readCts.Cancel();

            try
            {
                _ = await readTask;
            }
            catch (OperationCanceledException)
            {
            }
        }

        var stableRead = await Op.ReadAsync(path, CT);
        Assert.Equal("seed-content", System.Text.Encoding.UTF8.GetString(stableRead));
    }
}
