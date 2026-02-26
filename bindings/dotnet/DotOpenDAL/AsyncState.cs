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

namespace DotOpenDAL;

using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;

/// <summary>
/// Marker interface for async operation state tracked by the callback registry.
/// </summary>
public interface IAsyncState;

internal static class AsyncStateRegistry
{
    private static long nextAsyncStateId;
    private static readonly ConcurrentDictionary<long, IAsyncState> AsyncStates = new();

    public static long Register(IAsyncState state)
    {
        while (true)
        {
            var id = Interlocked.Increment(ref nextAsyncStateId);
            if (id == 0)
            {
                continue;
            }

            if (AsyncStates.TryAdd(id, state))
            {
                return id;
            }
        }
    }

    public static void Unregister(long context)
    {
        AsyncStates.TryRemove(context, out _);
    }

    public static bool TryTake<TState>(IntPtr context, [NotNullWhen(true)] out TState? state) where TState : class
    {
        state = null;
        var key = context.ToInt64();
        if (!AsyncStates.TryRemove(key, out var value))
        {
            return false;
        }

        state = value as TState;
        return state is not null;
    }
}

/// <summary>
/// State object for a pending asynchronous write operation.
/// </summary>
public sealed class WriteAsyncState : IAsyncState
{
    /// <summary>
    /// Completion source that resolves when the native write callback arrives.
    /// </summary>
    public TaskCompletionSource Completion { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>
    /// Registration used to detach cancellation callbacks when the operation completes.
    /// </summary>
    public CancellationTokenRegistration CancellationRegistration { get; set; }
}

/// <summary>
/// State object for a pending asynchronous read operation.
/// </summary>
public sealed class ReadAsyncState : IAsyncState
{
    /// <summary>
    /// Completion source that resolves with bytes when the native read callback arrives.
    /// </summary>
    public TaskCompletionSource<byte[]> Completion { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>
    /// Registration used to detach cancellation callbacks when the operation completes.
    /// </summary>
    public CancellationTokenRegistration CancellationRegistration { get; set; }
}