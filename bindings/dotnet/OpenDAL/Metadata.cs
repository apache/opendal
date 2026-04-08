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

namespace OpenDAL;

/// <summary>
/// Entry mode of a path reported by native OpenDAL metadata.
/// </summary>
public enum EntryMode
{
    /// <summary>
    /// Entry is a file.
    /// </summary>
    File = 0,

    /// <summary>
    /// Entry is a directory.
    /// </summary>
    Dir = 1,

    /// <summary>
    /// Entry mode is unknown.
    /// </summary>
    Unknown = 2,
}

/// <summary>
/// Metadata associated with a path.
/// </summary>
public sealed class Metadata
{
    internal Metadata(
        EntryMode mode,
        ulong contentLength,
        string? contentDisposition,
        string? contentMd5,
        string? contentType,
        string? contentEncoding,
        string? cacheControl,
        string? etag,
        DateTimeOffset? lastModified,
        string? version)
    {
        Mode = mode;
        ContentLength = contentLength;
        ContentDisposition = contentDisposition;
        ContentMd5 = contentMd5;
        ContentType = contentType;
        ContentEncoding = contentEncoding;
        CacheControl = cacheControl;
        ETag = etag;
        LastModified = lastModified;
        Version = version;
    }

    /// <summary>
    /// Gets entry mode.
    /// </summary>
    public EntryMode Mode { get; }

    /// <summary>
    /// Gets content length in bytes.
    /// </summary>
    public ulong ContentLength { get; }

    /// <summary>
    /// Gets <c>Content-Disposition</c> header value, if available.
    /// </summary>
    public string? ContentDisposition { get; }

    /// <summary>
    /// Gets <c>Content-MD5</c> header value, if available.
    /// </summary>
    public string? ContentMd5 { get; }

    /// <summary>
    /// Gets <c>Content-Type</c> header value, if available.
    /// </summary>
    public string? ContentType { get; }

    /// <summary>
    /// Gets <c>Content-Encoding</c> header value, if available.
    /// </summary>
    public string? ContentEncoding { get; }

    /// <summary>
    /// Gets <c>Cache-Control</c> header value, if available.
    /// </summary>
    public string? CacheControl { get; }

    /// <summary>
    /// Gets entity tag (<c>ETag</c>) value, if available.
    /// </summary>
    public string? ETag { get; }

    /// <summary>
    /// Gets last-modified timestamp, if available.
    /// The value is materialized from native Unix seconds and nanoseconds.
    /// </summary>
    public DateTimeOffset? LastModified { get; }

    /// <summary>
    /// Gets object version, if available.
    /// </summary>
    public string? Version { get; }

    /// <summary>
    /// Gets whether this metadata represents a file.
    /// </summary>
    public bool IsFile => Mode == EntryMode.File;

    /// <summary>
    /// Gets whether this metadata represents a directory.
    /// </summary>
    public bool IsDir => Mode == EntryMode.Dir;
}