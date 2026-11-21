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
public sealed class DeleteBehaviorTest : BehaviorTestBase
{
    private static CancellationToken CT => TestContext.Current.CancellationToken;

    public DeleteBehaviorTest(BehaviorOperatorFixture fixture)
        : base(fixture)
    {
    }

    [Fact]
    public void DeleteBehavior_RemovesObject()
    {
        if (!Supports(c => c.Delete && c.Read && c.Write))
        {
            return;
        }

        var path = NewPath("delete");

        Op.Write(path, RandomBytes(12));
        Op.Delete(path);

        var ex = Assert.Throws<OpenDALException>(() => Op.Read(path));
        Assert.True(IsMissingError(ex));
    }

    [Fact]
    public async Task DeleteBehavior_RemovesObjectAsync()
    {
        if (!Supports(c => c.Delete && c.Read && c.Write))
        {
            return;
        }

        var path = NewPath("delete-async");

        await Op.WriteAsync(path, RandomBytes(12), cancellationToken: CT);
        await Op.DeleteAsync(path, cancellationToken: CT);

        var ex = await Assert.ThrowsAsync<OpenDALException>(() => Op.ReadAsync(path, cancellationToken: CT));
        Assert.True(IsMissingError(ex));
    }

    [Fact]
    public void DeleteBehavior_DeletingMissingPath_IsAllowed()
    {
        if (!Supports(c => c.Delete))
        {
            return;
        }

        Op.Delete(NewPath("delete-missing"));
    }

    [Fact]
    public async Task DeleteBehavior_DeletingMissingPath_IsAllowedAsync()
    {
        if (!Supports(c => c.Delete))
        {
            return;
        }

        await Op.DeleteAsync(NewPath("delete-missing-async"), cancellationToken: CT);
    }

    [Fact]
    public async Task DeleteBehavior_WithVersion_RemovesVersionPermanentlyAsync()
    {
        if (!Supports(c => c.DeleteWithVersion && c.StatWithVersion && c.Write))
        {
            return;
        }

        var path = NewPath("delete-version");
        await Op.WriteAsync(path, RandomBytes(16), cancellationToken: CT);

        var version = (await Op.StatAsync(path, cancellationToken: CT)).Version;
        Assert.NotNull(version);

        await Op.DeleteAsync(path, cancellationToken: CT);
        var missing = await Assert.ThrowsAsync<OpenDALException>(() => Op.StatAsync(path, cancellationToken: CT));
        Assert.True(IsMissingError(missing));

        var archived = await Op.StatAsync(path, new StatOptions { Version = version }, CT);
        Assert.Equal(version, archived.Version);

        await Op.DeleteAsync(path, new DeleteOptions { Version = version }, CT);

        var gone = await Assert.ThrowsAsync<OpenDALException>(
            () => Op.StatAsync(path, new StatOptions { Version = version }, CT));
        Assert.Equal(ErrorCode.NotFound, gone.Code);
    }

    [Fact]
    public void DeleteBehavior_WithRecursive_RemovesTree()
    {
        if (!Supports(c => c.DeleteWithRecursive && c.Write && c.List))
        {
            return;
        }

        var dir = NewPath("delete-recursive") + "/";
        Op.Write($"{dir}a.txt", RandomBytes(16));
        Op.Write($"{dir}nested/b.txt", RandomBytes(16));

        Op.Delete(dir, new DeleteOptions { Recursive = true });

        Assert.Empty(Op.List(dir, new ListOptions { Recursive = true }));
    }

    [Fact]
    public void DeleteBehavior_WithVersionFromAnotherObject_IsAllowed()
    {
        if (!Supports(c => c.DeleteWithVersion && c.StatWithVersion && c.Write))
        {
            return;
        }

        var donor = NewPath("delete-version-donor");
        Op.Write(donor, RandomBytes(8));
        var foreignVersion = Op.Stat(donor).Version;
        Assert.NotNull(foreignVersion);

        var target = NewPath("delete-version-target");
        Op.Write(target, RandomBytes(8));

        Op.Delete(target, new DeleteOptions { Version = foreignVersion });
    }

    [Fact]
    public async Task DeleteBehavior_WithIfMatch_RemovesObjectIfMatchedAsync()
    {
        if (!Supports(c => c.DeleteWithIfMatch && c.Write && c.Stat))
        {
            return;
        }

        var path = NewPath("delete-if-match");
        await Op.WriteAsync(path, RandomBytes(16), cancellationToken: CT);

        var etag = (await Op.StatAsync(path, cancellationToken: CT)).ETag;
        Assert.NotNull(etag);

        await Op.DeleteAsync(path, new DeleteOptions { IfMatch = etag }, CT);

        var ex = await Assert.ThrowsAsync<OpenDALException>(() => Op.StatAsync(path, cancellationToken: CT));
        Assert.True(IsMissingError(ex));
    }

    [Fact]
    public async Task DeleteBehavior_WithIfMatch_DoesNotRemoveObjectIfNotMatchedAsync()
    {
        if (!Supports(c => c.DeleteWithIfMatch && c.Write && c.Stat))
        {
            return;
        }

        var path = NewPath("delete-if-match-not-matched");
        await Op.WriteAsync(path, RandomBytes(16), cancellationToken: CT);

        var etag = (await Op.StatAsync(path, cancellationToken: CT)).ETag;
        Assert.NotNull(etag);

        var ex = await Assert.ThrowsAsync<OpenDALException>(
            () => Op.DeleteAsync(path, new DeleteOptions { IfMatch = "\"this-etag-does-not-match\"" }, CT));
        Assert.Equal(ErrorCode.ConditionNotMatch, ex.Code);

        var stat = await Op.StatAsync(path, cancellationToken: CT);
        Assert.Equal(etag, stat.ETag);
    }

    [Fact]
    public async Task DeleteBehavior_IfNotChanged_RejectsStaleMetadataAsync()
    {
        if (!Supports(c => c.Write && c.Stat && (c.DeleteWithIfMatch || c.DeleteWithIfVersionMatch)))
        {
            return;
        }

        var path = NewPath("delete-if-not-changed");
        await Op.WriteAsync(path, RandomBytes(16), cancellationToken: CT);
        var stale = await Op.StatAsync(path, cancellationToken: CT);
        await Op.WriteAsync(path, RandomBytes(32), cancellationToken: CT);

        var ex = await Assert.ThrowsAsync<OpenDALException>(
            () => Op.DeleteAsync(path, new DeleteOptions { IfNotChanged = stale }, CT));
        Assert.Equal(ErrorCode.ConditionNotMatch, ex.Code);

        var current = await Op.StatAsync(path, cancellationToken: CT);
        await Op.DeleteAsync(path, new DeleteOptions { IfNotChanged = current }, CT);

        var missing = await Assert.ThrowsAsync<OpenDALException>(() => Op.StatAsync(path, cancellationToken: CT));
        Assert.True(IsMissingError(missing));
    }

    [Fact]
    public void DeleteBehavior_DeleteMany_RemovesEveryPath()
    {
        if (!Supports(c => c.Delete && c.Write && c.Stat))
        {
            return;
        }

        var first = NewPath("delete-many-a");
        var second = NewPath("delete-many-b");
        Op.Write(first, RandomBytes(8));
        Op.Write(second, RandomBytes(8));

        Op.DeleteMany([first, second, NewPath("delete-many-missing")]);

        Assert.False(Op.Exists(first));
        Assert.False(Op.Exists(second));
    }

    [Fact]
    public async Task DeleteBehavior_DeleteManyAsync_DeletesListedEntriesAsync()
    {
        if (!Supports(c => c.Delete && c.Write && c.List && c.CreateDir))
        {
            return;
        }

        // More targets than the stack buffer holds, converted from listed entries.
        var dir = NewPath("delete-many-listed") + "/";
        await Op.CreateDirAsync(dir, CT);
        for (var i = 0; i < 300; i++)
        {
            await Op.WriteAsync($"{dir}file-{i:D3}", RandomBytes(1), cancellationToken: CT);
        }

        var files = Op.List(dir)
            .Where(e => e.Metadata.IsFile)
            .Select(e => (DeleteInput)e);
        await Op.DeleteManyAsync(files, CT);

        Assert.DoesNotContain(Op.List(dir), e => e.Metadata.IsFile);
    }

    [Fact]
    public async Task DeleteBehavior_DeleteManyAsync_AppliesPerTargetOptionsAsync()
    {
        if (!Supports(c => c.DeleteWithVersion && c.StatWithVersion && c.Write))
        {
            return;
        }

        var path = NewPath("delete-many-version");
        await Op.WriteAsync(path, RandomBytes(16), cancellationToken: CT);
        var version = (await Op.StatAsync(path, cancellationToken: CT)).Version;
        Assert.NotNull(version);
        await Op.WriteAsync(path, RandomBytes(32), cancellationToken: CT);

        await Op.DeleteManyAsync([new DeleteInput(path, new DeleteOptions { Version = version })], CT);

        var gone = await Assert.ThrowsAsync<OpenDALException>(
            () => Op.StatAsync(path, new StatOptions { Version = version }, CT));
        Assert.Equal(ErrorCode.NotFound, gone.Code);
        Assert.Equal(32UL, (await Op.StatAsync(path, cancellationToken: CT)).ContentLength);
    }

    [Fact]
    public void DeleteBehavior_DeleteMany_RejectsNullTarget()
    {
        if (!IsEnabled)
        {
            return;
        }

        Assert.Throws<ArgumentException>(() => Op.DeleteMany([NewPath("delete-many-null"), null!]));
    }

    [Fact]
    public async Task DeleteBehavior_ConcurrentDeleteMany_DeletesOnlyEachCallsTargetsAsync()
    {
        // The race this guards against is in the binding, not the service, and
        // it needs many concurrent calls to show, so only memory runs it.
        if (!IsService("memory") || !Supports(c => c.Delete && c.Write && c.List && c.ListWithRecursive))
        {
            return;
        }

        const int calls = 128;
        const int filesPerCall = 4;
        // Missing paths are cheap to delete and fill each call with several
        // full native batches plus a partial one, so calls serialize both
        // batches that outgrow a thread's reused buffer and batches that fit.
        const int targetsPerCall = 1000;
        var dir = NewPath("delete-many-concurrent") + "/";
        string FilePath(int call, int file) => $"{dir}call-{call:D3}/file-{file:D3}";

        // The last call's files are never targeted and must survive.
        for (var call = 0; call <= calls; call++)
        {
            for (var file = 0; file < filesPerCall; file++)
            {
                await Op.WriteAsync(FilePath(call, file), RandomBytes(1), cancellationToken: CT);
            }
        }

        var deletions = Enumerable.Range(0, calls).Select(call => Task.Run(() => Op.DeleteManyAsync(
            Enumerable.Range(0, targetsPerCall).Select(file => new DeleteInput(FilePath(call, file))), CT), CT));
        await Task.WhenAll(deletions);

        var remaining = Op.List(dir, new ListOptions { Recursive = true })
            .Where(e => e.Metadata.IsFile)
            .Select(e => e.Path)
            .ToList();
        Assert.Equal(filesPerCall, remaining.Count);
        Assert.All(remaining, path => Assert.Contains($"call-{calls:D3}/", path));
    }
}
