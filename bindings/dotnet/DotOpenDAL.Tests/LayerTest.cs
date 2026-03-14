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

using DotOpenDAL.Layer;

namespace DotOpenDAL.Tests;

public class LayerTest
{
    [Fact]
    public void WithConcurrentLimit_ReturnsNewOperator()
    {
        using var op = new Operator("memory");
        var before = op.Op;
        using var layered = op.WithLayer(new ConcurrentLimitLayer(4));

        Assert.NotEqual(IntPtr.Zero, layered.Op);
        Assert.NotSame(op, layered);
        Assert.Equal(before, op.Op);
        Assert.NotEqual(before, layered.Op);

        layered.Write("layer-concurrent", [1, 2, 3]);
        var value = layered.Read("layer-concurrent");
        Assert.Equal([1, 2, 3], value);
    }

    [Fact]
    public void WithRetry_ReturnsNewOperator()
    {
        using var op = new Operator("memory");
        var before = op.Op;
        using var layered = op.WithLayer(new RetryLayer
        {
            Jitter = false,
            Factor = 2,
            MinDelay = TimeSpan.FromMilliseconds(1),
            MaxDelay = TimeSpan.FromMilliseconds(10),
            MaxTimes = 2,
        });

        Assert.NotEqual(IntPtr.Zero, layered.Op);
        Assert.NotSame(op, layered);
        Assert.Equal(before, op.Op);
        Assert.NotEqual(before, layered.Op);

        layered.Write("layer-retry", [4, 5, 6]);
        var value = layered.Read("layer-retry");
        Assert.Equal([4, 5, 6], value);
    }

    [Fact]
    public void WithConcurrentLimit_ZeroPermits_ThrowsArgumentOutOfRangeException()
    {
        using var op = new Operator("memory");

        Assert.Throws<ArgumentOutOfRangeException>(() => op.WithLayer(new ConcurrentLimitLayer(0)));
    }

    [Fact]
    public void WithRetry_InvalidFactor_ThrowsArgumentOutOfRangeException()
    {
        using var op = new Operator("memory");

        Assert.Throws<ArgumentOutOfRangeException>(() => op.WithLayer(new RetryLayer
        {
            Factor = 0,
        }));
    }

    [Fact]
    public void WithTimeout_ReturnsNewOperator()
    {
        using var op = new Operator("memory");
        var before = op.Op;
        using var layered = op.WithLayer(new TimeoutLayer
        {
            Timeout = TimeSpan.FromSeconds(5),
            IoTimeout = TimeSpan.FromSeconds(2),
        });

        Assert.NotEqual(IntPtr.Zero, layered.Op);
        Assert.NotSame(op, layered);
        Assert.Equal(before, op.Op);
        Assert.NotEqual(before, layered.Op);

        layered.Write("layer-timeout", [7, 8, 9]);
        var value = layered.Read("layer-timeout");
        Assert.Equal([7, 8, 9], value);
    }

    [Fact]
    public void WithTimeout_ZeroTimeout_ThrowsArgumentOutOfRangeException()
    {
        using var op = new Operator("memory");

        Assert.Throws<ArgumentOutOfRangeException>(() => op.WithLayer(new TimeoutLayer
        {
            Timeout = TimeSpan.Zero,
        }));
    }

    [Fact]
    public void WithLayer_OperatorsCanBeDisposedIndependently()
    {
        var op = new Operator("memory");
        var layered = op.WithLayer(new ConcurrentLimitLayer(2));

        layered.Dispose();

        op.Write("layer-dispose-origin", [1, 1, 1]);
        var originalValue = op.Read("layer-dispose-origin");
        Assert.Equal([1, 1, 1], originalValue);

        op.Dispose();

        var op2 = new Operator("memory");
        var layered2 = op2.WithLayer(new ConcurrentLimitLayer(2));

        op2.Dispose();

        layered2.Write("layer-dispose-layered", [2, 2, 2]);
        var layeredValue = layered2.Read("layer-dispose-layered");
        Assert.Equal([2, 2, 2], layeredValue);

        layered2.Dispose();
    }
}
