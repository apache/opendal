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
using System.Globalization;
using System.Reflection;
using System.Text;
using OpenDAL.Layer;

namespace OpenDAL.Tests;

public sealed class BehaviorOperatorFixture : IDisposable
{
    private const string CapabilityOverridesEnv = "OPENDAL_TEST_CAPABILITY_OVERRIDES";
    private readonly Operator? op;
    private readonly Capability capability;

    public string? Scheme { get; }

    public BehaviorOperatorFixture()
    {
        Scheme = Environment.GetEnvironmentVariable("OPENDAL_TEST");
        if (string.IsNullOrWhiteSpace(Scheme))
        {
            return;
        }

        var scheme = Scheme.ToLowerInvariant().Replace('_', '-');
        var options = BuildConfigFromEnvironment(Scheme);

        // Align with other bindings: isolate behavior tests with random roots by default.
        if (!IsRandomRootDisabled())
        {
            var baseRoot = options.TryGetValue("root", out var root) && !string.IsNullOrWhiteSpace(root)
                ? root!
                : "/";
            options["root"] = BuildRandomRoot(baseRoot);
        }

        op = new Operator(scheme, options).WithLayer(new RetryLayer());
        capability = ApplyCapabilityOverrides(op.Info.FullCapability);
    }

    public bool IsEnabled => op is not null;

    public Operator Op => op ?? throw new InvalidOperationException("Behavior operator is not initialized.");

    public Capability Capability => capability;

    public void Dispose()
    {
        op?.Dispose();
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

    private static Capability ApplyCapabilityOverrides(Capability capability)
    {
        var input = Environment.GetEnvironmentVariable(CapabilityOverridesEnv);
        if (string.IsNullOrWhiteSpace(input))
        {
            return capability;
        }

        foreach (var token in input.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries))
        {
            var (name, value) = ParseCapabilityOverride(token);
            ApplyCapabilityOverride(ref capability, name, value);
        }

        return capability;
    }

    private static (string Name, object? Value) ParseCapabilityOverride(string token)
    {
        if (token.StartsWith('+'))
        {
            return (token[1..].Trim(), true);
        }

        if (token.StartsWith('-'))
        {
            return (token[1..].Trim(), false);
        }

        var parts = token.Split('=', 2, StringSplitOptions.TrimEntries);
        if (parts.Length != 2)
        {
            throw new InvalidOperationException(
                $"invalid {CapabilityOverridesEnv} entry `{token}`: expected `+capability`, `-capability`, or `capability=value`");
        }

        return (parts[0], ParseCapabilityValue(token, parts[1]));
    }

    private static object? ParseCapabilityValue(string token, string value)
    {
        return value.ToLowerInvariant() switch
        {
            "true" or "on" or "yes" => true,
            "false" or "off" or "no" => false,
            "none" or "null" or "unset" => null,
            _ when ulong.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var result) => result,
            _ => throw new InvalidOperationException(
                $"invalid {CapabilityOverridesEnv} entry `{token}`: expected a boolean, non-negative integer, or `none`")
        };
    }

    private static void ApplyCapabilityOverride(ref Capability capability, string name, object? value)
    {
        var propertyName = ToCapabilityPropertyName(name);
        var property = typeof(Capability).GetProperty(propertyName, BindingFlags.Instance | BindingFlags.Public)
            ?? throw new InvalidOperationException(
                $"invalid {CapabilityOverridesEnv} entry `{name}`: unknown capability");

        var targetType = Nullable.GetUnderlyingType(property.PropertyType) ?? property.PropertyType;
        if (value is null)
        {
            if (Nullable.GetUnderlyingType(property.PropertyType) is null)
            {
                throw new InvalidOperationException(
                    $"invalid {CapabilityOverridesEnv} entry `{name}`: capability does not accept null");
            }
        }
        else if (value.GetType() != targetType)
        {
            throw new InvalidOperationException(
                $"invalid {CapabilityOverridesEnv} entry `{name}`: capability expects {targetType.Name}");
        }

        var setter = property.GetSetMethod(nonPublic: true)
            ?? throw new InvalidOperationException(
                $"invalid {CapabilityOverridesEnv} entry `{name}`: capability is not writable");

        object boxed = capability;
        setter.Invoke(boxed, new[] { value });
        capability = (Capability)boxed;
    }

    private static string ToCapabilityPropertyName(string name)
    {
        var builder = new StringBuilder(name.Length);
        var upper = true;

        foreach (var c in name.Trim())
        {
            if (c == '_')
            {
                upper = true;
                continue;
            }

            builder.Append(upper ? char.ToUpperInvariant(c) : c);
            upper = false;
        }

        return builder.ToString();
    }
}
