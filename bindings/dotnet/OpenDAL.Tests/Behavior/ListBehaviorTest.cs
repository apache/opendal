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

using OpenDAL.Options;

namespace OpenDAL.Tests;

[Collection("BehaviorOperator")]
public sealed class ListBehaviorTest : BehaviorTestBase
{
    private static CancellationToken CT => TestContext.Current.CancellationToken;

    public ListBehaviorTest(BehaviorOperatorFixture fixture)
        : base(fixture)
    {
    }

    [Fact]
    public void ListBehavior_ListsEntriesUnderPrefix()
    {
        if (!Supports(c => c.List && c.Write && c.CreateDir))
        {
            return;
        }

        var dir = NewPath("list") + "/";
        var a = $"{dir}a.txt";
        var b = $"{dir}nested/b.txt";

        Op.CreateDir(dir);
        Op.Write(a, RandomBytes(10));
        Op.Write(b, RandomBytes(20));

        var entries = Op.List(dir, new ListOptions { Recursive = true });

        Assert.Contains(entries, e => e.Path.EndsWith("a.txt", StringComparison.Ordinal));
        Assert.Contains(entries, e => e.Path.EndsWith("nested/b.txt", StringComparison.Ordinal));
    }

    [Fact]
    public async Task ListBehavior_ListsEntriesUnderPrefixAsync()
    {
        if (!Supports(c => c.List && c.Write && c.CreateDir))
        {
            return;
        }

        var dir = NewPath("list-async") + "/";
        var a = $"{dir}a.txt";
        var b = $"{dir}nested/b.txt";

        await Op.CreateDirAsync(dir, CT);
        await Op.WriteAsync(a, RandomBytes(10), CT);
        await Op.WriteAsync(b, RandomBytes(20), CT);

        var entries = await Op.ListAsync(dir, new ListOptions { Recursive = true }, CT);

        Assert.Contains(entries, e => e.Path.EndsWith("a.txt", StringComparison.Ordinal));
        Assert.Contains(entries, e => e.Path.EndsWith("nested/b.txt", StringComparison.Ordinal));
    }

    [Fact]
    public async Task ListBehavior_WithLimit_ReturnsAtMostRequestedSizeAsync()
    {
        if (!Supports(c => c.List && c.ListWithLimit && c.Write))
        {
            return;
        }

        var dir = NewPath("list-limit") + "/";
        var files = new List<string>();
        for (var i = 0; i < 6; i++)
        {
            var path = $"{dir}file-{i}.txt";
            files.Add(path);
            await Op.WriteAsync(path, RandomBytes(8), CT);
        }

        var entries = await Op.ListAsync(dir, new ListOptions { Recursive = true, Limit = 3 }, CT);
        foreach (var file in files)
        {
            Assert.Contains(entries, e => e.Path == file);
        }
    }

    [Fact]
    public void ListBehavior_WithLimit_ReturnsAtMostRequestedSize()
    {
        if (!Supports(c => c.List && c.ListWithLimit && c.Write))
        {
            return;
        }

        var dir = NewPath("list-limit-sync") + "/";
        var files = new List<string>();
        for (var i = 0; i < 6; i++)
        {
            var path = $"{dir}file-{i}.txt";
            files.Add(path);
            Op.Write(path, RandomBytes(8));
        }

        var entries = Op.List(dir, new ListOptions { Recursive = true, Limit = 3 });
        foreach (var file in files)
        {
            Assert.Contains(entries, e => e.Path == file);
        }
    }
}
