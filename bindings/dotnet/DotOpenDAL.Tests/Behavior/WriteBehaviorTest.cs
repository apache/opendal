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
public sealed class WriteBehaviorTest : BehaviorTestBase
{
    private static CancellationToken CT => TestContext.Current.CancellationToken;

    public WriteBehaviorTest(BehaviorOperatorFixture fixture)
        : base(fixture)
    {
    }

    [Fact]
    public void WriteBehavior_BasicRoundtrip()
    {
        if (!Supports(c => c.Write && c.Read))
        {
            return;
        }

        var path = NewPath("write");
        var content = RandomBytes(1024);

        Op.Write(path, content);
        var actual = Op.Read(path);

        Assert.Equal(content, actual);
    }

    [Fact]
    public async Task WriteBehavior_BasicRoundtripAsync()
    {
        if (!Supports(c => c.Write && c.Read))
        {
            return;
        }

        var path = NewPath("write-async");
        var content = RandomBytes(1024);

        await Op.WriteAsync(path, content, CT);
        var actual = await Op.ReadAsync(path, CT);

        Assert.Equal(content, actual);
    }

    [Fact]
    public void WriteBehavior_IfNotExists_RejectsOverwrite()
    {
        if (!Supports(c => c.Write && c.Read && c.WriteWithIfNotExists))
        {
            return;
        }

        var path = NewPath("write-if-not-exists");
        var first = RandomBytes(128);
        var second = RandomBytes(64);

        Op.Write(path, first);

        var ex = Assert.Throws<OpenDALException>(() =>
            Op.Write(path, second, new WriteOptions { IfNotExists = true }));

        Assert.Contains(ex.Code, new[] { ErrorCode.ConditionNotMatch, ErrorCode.AlreadyExists });
        Assert.Equal(first, Op.Read(path));
    }

    [Fact]
    public async Task WriteBehavior_IfNotExists_RejectsOverwriteAsync()
    {
        if (!Supports(c => c.Write && c.Read && c.WriteWithIfNotExists))
        {
            return;
        }

        var path = NewPath("write-if-not-exists-async");
        var first = RandomBytes(128);
        var second = RandomBytes(64);

        await Op.WriteAsync(path, first, CT);

        var ex = await Assert.ThrowsAsync<OpenDALException>(() =>
            Op.WriteAsync(path, second, new WriteOptions { IfNotExists = true }, CT));

        Assert.Contains(ex.Code, new[] { ErrorCode.ConditionNotMatch, ErrorCode.AlreadyExists });
        Assert.Equal(first, await Op.ReadAsync(path, CT));
    }

    [Fact]
    public void WriteBehavior_Append_AppendsWhenSupported()
    {
        if (!Supports(c => c.Write && c.Read && c.WriteCanAppend))
        {
            return;
        }

        var path = NewPath("write-append");
        Op.Write(path, System.Text.Encoding.UTF8.GetBytes("a"), new WriteOptions { Append = true });
        Op.Write(path, System.Text.Encoding.UTF8.GetBytes("b"), new WriteOptions { Append = true });

        Assert.Equal("ab", System.Text.Encoding.UTF8.GetString(Op.Read(path)));
    }

    [Fact]
    public async Task WriteBehavior_Append_AppendsWhenSupportedAsync()
    {
        if (!Supports(c => c.Write && c.Read && c.WriteCanAppend))
        {
            return;
        }

        var path = NewPath("write-append-async");
        await Op.WriteAsync(path, System.Text.Encoding.UTF8.GetBytes("a"), new WriteOptions { Append = true }, CT);
        await Op.WriteAsync(path, System.Text.Encoding.UTF8.GetBytes("b"), new WriteOptions { Append = true }, CT);

        Assert.Equal("ab", System.Text.Encoding.UTF8.GetString(await Op.ReadAsync(path, CT)));
    }
}
