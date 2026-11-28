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

using System.Text.Json;
using OpenDAL.ServiceConfig;

namespace OpenDAL.Tests;

public class ServiceConfigMapTest
{
    private static Dictionary<string, string> SpecialCharacters() => new()
    {
        ["q\""] = "b\\",
        ["nl"] = "a\nb",
        ["ctl"] = "\u0001",
        ["ü"] = "ß",
        ["url"] = "hdfs://ns",
    };

    [Fact]
    public void S3SessionTags_ToOptions_EmitsOneJsonObject()
    {
        var config = new S3ServiceConfig
        {
            AssumeRoleSessionTags = new Dictionary<string, string> { ["Team"] = "x" },
        };

        var options = config.ToOptions();

        Assert.Equal("{\"Team\":\"x\"}", options["assume_role_session_tags"]);
        Assert.False(options.ContainsKey("Team"));
    }

    [Fact]
    public void HdfsNativeOptions_ToOptions_EmitsOneJsonObject()
    {
        var config = new HdfsNativeServiceConfig
        {
            Options = new Dictionary<string, string> { ["dfs.nameservices"] = "ns" },
        };

        var options = config.ToOptions();

        Assert.Equal("{\"dfs.nameservices\":\"ns\"}", options["options"]);
    }

    [Fact]
    public void MapOption_SortsKeysOrdinalAndKeepsCase()
    {
        var tags = new Dictionary<string, string>
        {
            ["b"] = "2",
            ["a"] = "1",
            ["Fs"] = "x",
        };

        var json = Utilities.ToOptionJson(tags);

        Assert.Equal("{\"Fs\":\"x\",\"a\":\"1\",\"b\":\"2\"}", json);
    }

    [Fact]
    public void MapOption_SpecialCharacters_RoundTrip()
    {
        var json = Utilities.ToOptionJson(SpecialCharacters());

        using var document = JsonDocument.Parse(json);
        var decoded = document.RootElement
            .EnumerateObject()
            .ToDictionary(property => property.Name, property => property.Value.GetString());

        Assert.Equal(new Dictionary<string, string?>
        {
            ["q\""] = "b\\",
            ["nl"] = "a\nb",
            ["ctl"] = "\u0001",
            ["ü"] = "ß",
            ["url"] = "hdfs://ns",
        }, decoded);
    }

    [Fact]
    public void MapOption_Empty_EmitsEmptyObject()
    {
        var config = new S3ServiceConfig
        {
            AssumeRoleSessionTags = new Dictionary<string, string>(),
        };

        var options = config.ToOptions();

        Assert.Equal("{}", options["assume_role_session_tags"]);
    }

    [Fact]
    public void MapOption_Null_IsOmitted()
    {
        var config = new S3ServiceConfig
        {
            Bucket = "b",
        };

        var options = config.ToOptions();

        Assert.False(options.ContainsKey("assume_role_session_tags"));
    }

    [Fact]
    public void Constructor_S3SessionTags_ConstructsOperator()
    {
        var config = new S3ServiceConfig
        {
            Bucket = "b",
            Region = "us-east-1",
            AssumeRoleSessionTags = SpecialCharacters(),
        };

        using var op = new Operator(config);

        Assert.NotEqual(IntPtr.Zero, op.DangerousGetHandle());
    }

    [Fact]
    public void Constructor_MapOptionNullValue_ThrowsConfigInvalid()
    {
        var config = new S3ServiceConfig
        {
            Bucket = "b",
            Region = "us-east-1",
            AssumeRoleSessionTags = new Dictionary<string, string> { ["Team"] = null! },
        };

        var ex = Assert.Throws<OpenDALException>(() => new Operator(config));

        Assert.Equal(ErrorCode.ConfigInvalid, ex.Code);
        Assert.Contains("assume_role_session_tags", ex.Message);
    }
}
