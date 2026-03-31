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

        await Op.WriteAsync(path, RandomBytes(12), CT);
        await Op.DeleteAsync(path, CT);

        var ex = await Assert.ThrowsAsync<OpenDALException>(() => Op.ReadAsync(path, CT));
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

        await Op.DeleteAsync(NewPath("delete-missing-async"), CT);
    }
}
