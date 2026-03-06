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

using System.Collections;
using System.Security.Cryptography;
using System.Threading;

namespace DotOpenDAL.Tests;

public abstract class BehaviorTestBase : IDisposable
{
    private static readonly HashSet<string> LockSensitiveServices = ["persy", "redb", "sled"];
    private static readonly SemaphoreSlim LockSensitiveServiceGate = new(1, 1);

    private readonly Operator? op;
    private readonly bool holdsLock;

    protected string? Scheme { get; }

    protected BehaviorTestBase()
    {
        Scheme = Environment.GetEnvironmentVariable("OPENDAL_TEST");
        if (string.IsNullOrWhiteSpace(Scheme))
        {
            return;
        }

        var scheme = Scheme.ToLowerInvariant().Replace('_', '-');
        var options = BuildConfigFromEnvironment(Scheme);

        if (IsLockSensitiveService(scheme))
        {
            LockSensitiveServiceGate.Wait();
            holdsLock = true;
        }

        // Align with other bindings: isolate behavior tests with random roots by default.
        if (!IsRandomRootDisabled())
        {
            var baseRoot = options.TryGetValue("root", out var root) && !string.IsNullOrWhiteSpace(root)
                ? root!
                : "/";
            options["root"] = BuildRandomRoot(baseRoot);
        }

        try
        {
            op = new Operator(scheme, options);
        }
        catch
        {
            if (holdsLock)
            {
                LockSensitiveServiceGate.Release();
            }

            throw;
        }
    }

    protected bool IsEnabled => op is not null;

    protected Operator Op => op ?? throw new InvalidOperationException("Behavior operator is not initialized.");

    protected Capability Capability => Op.Info.FullCapability;

    protected bool Supports(Func<Capability, bool> predicate)
    {
        return IsEnabled && predicate(Capability);
    }

    protected static byte[] RandomBytes(int size)
    {
        var bytes = new byte[size];
        RandomNumberGenerator.Fill(bytes);
        return bytes;
    }

    protected string NewPath(string prefix)
    {
        return $"dotnet-behavior/{prefix}-{Guid.NewGuid():N}";
    }

    protected static bool IsMissingError(OpenDALException ex)
    {
        return ex.Code == ErrorCode.NotFound || ex.Code == ErrorCode.NotADirectory;
    }

    public virtual void Dispose()
    {
        op?.Dispose();

        if (holdsLock)
        {
            LockSensitiveServiceGate.Release();
        }
    }

    private static bool IsLockSensitiveService(string scheme)
    {
        return LockSensitiveServices.Contains(scheme);
    }

    private static Dictionary<string, string> BuildConfigFromEnvironment(string service)
    {
        var variables = Environment.GetEnvironmentVariables();
        var prefix = $"opendal_{service.ToLowerInvariant()}_";
        var config = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        foreach (DictionaryEntry entry in variables)
        {
            var key = entry.Key?.ToString();
            var value = entry.Value?.ToString();
            if (string.IsNullOrWhiteSpace(key) || value is null)
            {
                continue;
            }

            var normalized = key.ToLowerInvariant();
            if (!normalized.StartsWith(prefix, StringComparison.Ordinal))
            {
                continue;
            }

            config[normalized[prefix.Length..]] = value;
        }

        return config;
    }

    private static bool IsRandomRootDisabled()
    {
        return string.Equals(
            Environment.GetEnvironmentVariable("OPENDAL_DISABLE_RANDOM_ROOT"),
            "true",
            StringComparison.OrdinalIgnoreCase);
    }

    private static string BuildRandomRoot(string baseRoot)
    {
        var trimmed = baseRoot.Trim();
        if (trimmed.Length == 0)
        {
            trimmed = "/";
        }

        if (!trimmed.EndsWith('/'))
        {
            trimmed += "/";
        }

        return $"{trimmed}{Guid.NewGuid():N}/";
    }
}
