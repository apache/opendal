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

// Generated from bindings/java ServiceConfig definitions.

using OpenDAL.ServiceConfig.Abstractions;

namespace OpenDAL.ServiceConfig
{
    /// <summary>
    /// Configuration for service vercel_artifacts.
    /// </summary>
    public sealed class VercelArtifactsServiceConfig : IServiceConfig
    {
        /// <summary>
        /// The access token for Vercel.
        /// </summary>
        public string? AccessToken { get; init; }

        public string Scheme => "vercel_artifacts";

        public IReadOnlyDictionary<string, string> ToOptions()
        {
            var map = new Dictionary<string, string>();
            if (AccessToken is not null)
            {
                map["access_token"] = Utilities.ToOptionString(AccessToken);
            }
            return map;
        }
    }

}
