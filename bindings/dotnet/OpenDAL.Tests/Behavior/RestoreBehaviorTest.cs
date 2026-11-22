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
public sealed class RestoreBehaviorTest : BehaviorTestBase
{
    private static CancellationToken CT => TestContext.Current.CancellationToken;

    public RestoreBehaviorTest(BehaviorOperatorFixture fixture)
        : base(fixture)
    {
    }

    [Fact]
    public void RestoreBehavior_RestoresDeletedFile()
    {
        if (!Supports(c => c.Restore && c.Read && c.Write && c.Delete))
        {
            return;
        }

        var path = NewPath("restore-deleted");
        var content = RandomBytes(64);

        Op.Write(path, content);
        Op.Delete(path);
        Assert.False(Op.Exists(path));

        Op.Restore(path);

        Assert.Equal(content, Op.Read(path));
    }

    [Fact]
    public async Task RestoreBehavior_LiveFileIsUnchangedAsync()
    {
        if (!Supports(c => c.Restore && c.Read && c.Write && c.Delete))
        {
            return;
        }

        var path = NewPath("restore-live");
        var content = RandomBytes(64);

        await Op.WriteAsync(path, content, cancellationToken: CT);
        await Op.RestoreAsync(path, cancellationToken: CT);

        Assert.Equal(content, await Op.ReadAsync(path, cancellationToken: CT));
    }

    [Fact]
    public async Task RestoreBehavior_UnknownPath_ReturnsNotFoundAsync()
    {
        if (!Supports(c => c.Restore && c.Read && c.Write && c.Delete))
        {
            return;
        }

        var ex = await Assert.ThrowsAsync<OpenDALException>(
            () => Op.RestoreAsync(NewPath("restore-unknown"), cancellationToken: CT));
        Assert.Equal(ErrorCode.NotFound, ex.Code);
    }

    [Fact]
    public async Task RestoreBehavior_WithVersion_RestoresThatVersionAsync()
    {
        if (!Supports(c => c.Restore && c.RestoreWithVersion && c.Read && c.Write && c.StatWithVersion))
        {
            return;
        }

        var path = NewPath("restore-version");
        var oldContent = RandomBytes(64);

        await Op.WriteAsync(path, oldContent, cancellationToken: CT);
        var version = (await Op.StatAsync(path, cancellationToken: CT)).Version;
        Assert.NotNull(version);
        await Op.WriteAsync(path, RandomBytes(96), cancellationToken: CT);

        await Op.RestoreAsync(path, new RestoreOptions { Version = version }, CT);

        Assert.Equal(oldContent, await Op.ReadAsync(path, cancellationToken: CT));
    }

    [Fact]
    public async Task RestoreBehavior_IfNotExists_RejectsLivePathAsync()
    {
        if (!Supports(c => c.Restore && c.RestoreWithVersion && c.RestoreWithIfNotExists
            && c.Read && c.Write && c.StatWithVersion))
        {
            return;
        }

        var path = NewPath("restore-if-not-exists");
        await Op.WriteAsync(path, RandomBytes(64), cancellationToken: CT);
        var version = (await Op.StatAsync(path, cancellationToken: CT)).Version;
        var newContent = RandomBytes(96);
        await Op.WriteAsync(path, newContent, cancellationToken: CT);

        var ex = await Assert.ThrowsAsync<OpenDALException>(
            () => Op.RestoreAsync(path, new RestoreOptions { Version = version, IfNotExists = true }, CT));
        Assert.Equal(ErrorCode.ConditionNotMatch, ex.Code);
        Assert.Equal(newContent, await Op.ReadAsync(path, cancellationToken: CT));
    }
}
