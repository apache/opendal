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

namespace OpenDAL.Tests;

public sealed class OpenDALExceptionTest
{
    [Theory]
    [InlineData(12, ErrorCode.Conflict)]
    [InlineData(13, ErrorCode.ChecksumMismatch)]
    [InlineData(-1, ErrorCode.Unexpected)]
    [InlineData(int.MaxValue, ErrorCode.Unexpected)]
    public void Constructor_DecodesNativeErrorCode(int code, ErrorCode expected)
    {
        var exception = new OpenDALException(new OpenDALError { HasError = 1, Code = code });

        Assert.Equal(expected, exception.Code);
    }
}
