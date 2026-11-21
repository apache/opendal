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

using System.Text;
using OpenDAL.Options;

namespace OpenDAL.Tests;

[Collection("BehaviorOperator")]
public sealed class ComposeBehaviorTest : BehaviorTestBase
{
    private static CancellationToken CT => TestContext.Current.CancellationToken;

    public ComposeBehaviorTest(BehaviorOperatorFixture fixture)
        : base(fixture)
    {
    }

    [Fact]
    public void ComposeBehavior_ConcatenatesSourcesInOrder()
    {
        if (!Supports(c => c.Compose && c.Read && c.Write))
        {
            return;
        }

        var first = NewPath("compose-first");
        var second = NewPath("compose-second");
        var target = NewPath("compose-target");

        Op.Write(first, Encoding.UTF8.GetBytes("hello"));
        Op.Write(second, Encoding.UTF8.GetBytes(" world"));
        Op.Write(target, Encoding.UTF8.GetBytes("stale"));

        var meta = Op.Compose([first, second, first], target);

        Assert.Equal(16UL, meta.ContentLength);
        Assert.Equal("hello worldhello", Encoding.UTF8.GetString(Op.Read(target)));
        Assert.Equal("hello", Encoding.UTF8.GetString(Op.Read(first)));
        Assert.Equal(" world", Encoding.UTF8.GetString(Op.Read(second)));
    }

    [Fact]
    public async Task ComposeBehavior_EmptySources_ReturnsConfigInvalidAsync()
    {
        if (!Supports(c => c.Compose && c.Read && c.Write))
        {
            return;
        }

        var ex = await Assert.ThrowsAsync<OpenDALException>(
            () => Op.ComposeAsync([], NewPath("compose-empty"), cancellationToken: CT));
        Assert.Equal(ErrorCode.ConfigInvalid, ex.Code);
    }

    [Fact]
    public async Task ComposeBehavior_SourceIsTarget_ReturnsIsSameFileAsync()
    {
        if (!Supports(c => c.Compose && c.Read && c.Write))
        {
            return;
        }

        var path = NewPath("compose-self");
        await Op.WriteAsync(path, Encoding.UTF8.GetBytes("content"), cancellationToken: CT);

        var ex = await Assert.ThrowsAsync<OpenDALException>(
            () => Op.ComposeAsync([path], path, cancellationToken: CT));
        Assert.Equal(ErrorCode.IsSameFile, ex.Code);
        Assert.Equal("content", Encoding.UTF8.GetString(await Op.ReadAsync(path, cancellationToken: CT)));
    }

    [Fact]
    public async Task ComposeBehavior_SourceIfNotChanged_GuardsEachSourceAsync()
    {
        if (!Supports(c => c.Compose && c.Read && c.Write && c.Stat
            && (c.ComposeWithSourceVersion || c.ComposeWithSourceIfMatch)))
        {
            return;
        }

        var source = NewPath("compose-source-guarded");
        var other = NewPath("compose-source-other");
        var target = NewPath("compose-source-target");
        var rejectedTarget = NewPath("compose-source-rejected");

        await Op.WriteAsync(source, Encoding.UTF8.GetBytes("source"), cancellationToken: CT);
        var expected = await Op.StatAsync(source, cancellationToken: CT);
        await Op.WriteAsync(other, Encoding.UTF8.GetBytes("unrelated"), cancellationToken: CT);
        var unrelated = await Op.StatAsync(other, cancellationToken: CT);

        await Op.ComposeAsync(
            [new ComposeInput(source, new ComposeSourceOptions { IfNotChanged = expected })],
            target,
            cancellationToken: CT);
        Assert.Equal("source", Encoding.UTF8.GetString(await Op.ReadAsync(target, cancellationToken: CT)));

        await Assert.ThrowsAsync<OpenDALException>(
            () => Op.ComposeAsync(
                [new ComposeInput(source, new ComposeSourceOptions { IfNotChanged = unrelated })],
                rejectedTarget,
                cancellationToken: CT));
        Assert.False(await Op.ExistsAsync(rejectedTarget, CT));
    }

    [Fact]
    public async Task ComposeBehavior_IfNotExists_RejectsExistingTargetAsync()
    {
        if (!Supports(c => c.Compose && c.Read && c.Write && c.ComposeWithIfNotExists))
        {
            return;
        }

        var source = NewPath("compose-if-not-exists-source");
        var target = NewPath("compose-if-not-exists-target");
        await Op.WriteAsync(source, Encoding.UTF8.GetBytes("new"), cancellationToken: CT);
        await Op.WriteAsync(target, Encoding.UTF8.GetBytes("existing"), cancellationToken: CT);

        var ex = await Assert.ThrowsAsync<OpenDALException>(
            () => Op.ComposeAsync([source], target, new ComposeOptions { IfNotExists = true }, CT));
        Assert.Equal(ErrorCode.ConditionNotMatch, ex.Code);
        Assert.Equal("existing", Encoding.UTF8.GetString(await Op.ReadAsync(target, cancellationToken: CT)));
    }

    [Fact]
    public void ComposeBehavior_EntryConvertsToInput_KeepsPathAndVersion()
    {
        if (!Supports(c => c.Write && c.List && c.CreateDir))
        {
            return;
        }

        var dir = NewPath("compose-entry") + "/";
        Op.CreateDir(dir);
        Op.Write($"{dir}part", RandomBytes(8));
        var entry = Assert.Single(Op.List(dir), e => e.Metadata.IsFile);

        ComposeInput input = entry;

        Assert.Equal(entry.Path, input.Path);
        Assert.Equal(entry.Metadata.Version, input.Options?.Version);
    }

    [Fact]
    public void ComposeBehavior_NullSource_RejectsArguments()
    {
        if (!IsEnabled)
        {
            return;
        }

        Assert.Throws<ArgumentException>(() => Op.Compose([null!], NewPath("compose-null")));
    }
}
