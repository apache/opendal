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

using System.Runtime.InteropServices;
using DotOpenDAL.Interop.Result;

namespace DotOpenDAL;

/// <summary>
/// Managed wrapper over an OpenDAL native executor handle.
/// </summary>
public sealed class Executor : SafeHandle
{
    /// <summary>
    /// Creates an executor with the given number of worker threads.
    /// </summary>
    /// <remarks>
    /// This executor can be passed to operator APIs to control the runtime that executes the underlying native tasks.
    /// </remarks>
    /// <param name="threads">Number of Tokio worker threads. Must be greater than zero.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="threads"/> is less than or equal to zero.</exception>
    /// <exception cref="OpenDALException">Native executor creation fails.</exception>
    public Executor(int threads) : base(IntPtr.Zero, true)
    {
        SetHandle(CreateExecutor(threads));
    }

    /// <summary>
    /// Gets whether the native handle is invalid.
    /// </summary>
    public override bool IsInvalid => handle == IntPtr.Zero;

    private static IntPtr CreateExecutor(int threads)
    {
        if (threads <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(threads), "threads must be greater than zero.");
        }

        var result = NativeMethods.executor_create((nuint)threads);
        return Operator.ToValueOrThrowAndRelease<IntPtr, OpenDALExecutorResult>(result);
    }

    /// <summary>
    /// Releases the native executor handle.
    /// </summary>
    protected override bool ReleaseHandle()
    {
        NativeMethods.executor_free(handle);
        return true;
    }
}
