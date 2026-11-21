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

namespace OpenDAL;

/// <summary>
/// One path of a batch delete operation.
/// </summary>
/// <remarks>
/// A plain path converts implicitly, so <c>op.DeleteMany(["a", "b"])</c>
/// deletes the current versions of <c>a</c> and <c>b</c>.
/// </remarks>
public sealed class DeleteInput
{
    /// <summary>
    /// Initializes a delete input.
    /// </summary>
    /// <param name="path">Target path in the configured backend.</param>
    /// <param name="options">Options that select and guard this deletion.</param>
    public DeleteInput(string path, DeleteOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(path);
        Path = path;
        Options = options;
    }

    /// <summary>
    /// Target path in the configured backend.
    /// </summary>
    public string Path { get; }

    /// <summary>
    /// Options that select and guard this deletion.
    /// </summary>
    public DeleteOptions? Options { get; }

    /// <summary>
    /// Converts a path into a target that deletes its current version.
    /// </summary>
    public static implicit operator DeleteInput(string path) => new(path);

    /// <summary>
    /// Converts a listed entry into a target that deletes the entry's version, when it has one.
    /// </summary>
    public static implicit operator DeleteInput(Entry entry)
    {
        ArgumentNullException.ThrowIfNull(entry);
        var version = entry.Metadata.Version;
        return new(entry.Path, version is null ? null : new DeleteOptions { Version = version });
    }
}
