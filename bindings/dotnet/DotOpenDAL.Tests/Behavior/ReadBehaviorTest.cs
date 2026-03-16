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
public sealed class ReadBehaviorTest : BehaviorTestBase
{
    private static CancellationToken CT => TestContext.Current.CancellationToken;

    public ReadBehaviorTest(BehaviorOperatorFixture fixture)
        : base(fixture)
    {
    }

    [Fact]
    public void ReadBehavior_ReadsWrittenData()
    {
        if (!Supports(c => c.Read && c.Write))
        {
            return;
        }

        var path = NewPath("read-sync");
        var content = RandomBytes(2048);

        Op.Write(path, content);
        var actual = Op.Read(path);

        Assert.Equal(content, actual);
    }

    [Fact]
    public async Task ReadBehavior_ReadsWrittenDataAsync()
    {
        if (!Supports(c => c.Read && c.Write))
        {
            return;
        }

        var path = NewPath("read-async");
        var content = RandomBytes(2048);

        await Op.WriteAsync(path, content, CT);
        var actual = await Op.ReadAsync(path, CT);

        Assert.Equal(content, actual);
    }

    [Fact]
    public void ReadBehavior_WithRange_ReturnsExpectedBytes()
    {
        if (!Supports(c => c.Read && c.Write))
        {
            return;
        }

        var path = NewPath("read-range");
        var content = RandomBytes(512);

        Op.Write(path, content);
        var partial = Op.Read(path, new ReadOptions { Offset = 10, Length = 100 });

        Assert.Equal(content.AsSpan(10, 100).ToArray(), partial);
    }

    [Fact]
    public async Task ReadBehavior_WithRange_ReturnsExpectedBytesAsync()
    {
        if (!Supports(c => c.Read && c.Write))
        {
            return;
        }

        var path = NewPath("read-range-async");
        var content = RandomBytes(512);

        await Op.WriteAsync(path, content, CT);
        var partial = await Op.ReadAsync(path, new ReadOptions { Offset = 10, Length = 100 }, CT);

        Assert.Equal(content.AsSpan(10, 100).ToArray(), partial);
    }

    [Fact]
    public void ReadBehavior_MissingPath_ReturnsNotFound()
    {
        if (!Supports(c => c.Read))
        {
            return;
        }

        var ex = Assert.Throws<OpenDALException>(() => Op.Read(NewPath("missing")));

        Assert.True(IsMissingError(ex));
    }

    [Fact]
    public async Task ReadBehavior_MissingPath_ReturnsNotFoundAsync()
    {
        if (!Supports(c => c.Read))
        {
            return;
        }

        var ex = await Assert.ThrowsAsync<OpenDALException>(() => Op.ReadAsync(NewPath("missing-async"), CT));

        Assert.True(IsMissingError(ex));
    }
}
