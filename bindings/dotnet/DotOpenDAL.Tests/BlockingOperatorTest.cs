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

using Xunit.Abstractions;

namespace DotOpenDAL.Tests;

public class BlockingOperatorTest
{

    private readonly ITestOutputHelper output;

    public BlockingOperatorTest(ITestOutputHelper output)
    {
        this.output = output;
    }

    [Fact]
    public void TestReadWrite()
    {
        var op = new BlockingOperator();
        output.WriteLine("{0}", op.Op.ToString());
        op.Write("test");
        var result = op.Read("test", "12345");
        output.WriteLine("{0}", result);
    }
}
