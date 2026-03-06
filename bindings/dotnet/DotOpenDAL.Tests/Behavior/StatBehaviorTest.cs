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

public sealed class StatBehaviorTest : BehaviorTestBase
{
    [Fact]
    public void StatBehavior_ReturnsFileMetadata()
    {
        if (!Supports(c => c.Stat && c.Write))
        {
            return;
        }

        var path = NewPath("stat-file");
        var content = RandomBytes(333);

        Op.Write(path, content);
        var meta = Op.Stat(path);

        Assert.True(meta.IsFile);
        Assert.Equal((ulong)content.Length, meta.ContentLength);
    }

    [Fact]
    public void StatBehavior_MissingPath_ReturnsNotFound()
    {
        if (!Supports(c => c.Stat))
        {
            return;
        }

        var ex = Assert.Throws<OpenDALException>(() => Op.Stat(NewPath("stat-missing")));

        Assert.True(IsMissingError(ex));
    }
}
