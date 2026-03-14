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

using DotOpenDAL.ServiceConfig.Abstractions;

namespace DotOpenDAL.ServiceConfig
{
    /// <summary>
    /// Configuration for service sftp.
    /// </summary>
    public sealed class SftpServiceConfig : IServiceConfig
    {
        /// <summary>
        /// enable_copy of this backend
        /// </summary>
        public bool? EnableCopy { get; init; }
        /// <summary>
        /// endpoint of this backend
        /// </summary>
        public string? Endpoint { get; init; }
        /// <summary>
        /// key of this backend
        /// </summary>
        public string? Key { get; init; }
        /// <summary>
        /// known_hosts_strategy of this backend
        /// </summary>
        public string? KnownHostsStrategy { get; init; }
        /// <summary>
        /// root of this backend
        /// </summary>
        public string? Root { get; init; }
        /// <summary>
        /// user of this backend
        /// </summary>
        public string? User { get; init; }

        public string Scheme => "sftp";

        public IReadOnlyDictionary<string, string> ToOptions()
        {
            var map = new Dictionary<string, string>();
            if (EnableCopy is not null)
            {
                map["enable_copy"] = Utilities.ToOptionString(EnableCopy);
            }
            if (Endpoint is not null)
            {
                map["endpoint"] = Utilities.ToOptionString(Endpoint);
            }
            if (Key is not null)
            {
                map["key"] = Utilities.ToOptionString(Key);
            }
            if (KnownHostsStrategy is not null)
            {
                map["known_hosts_strategy"] = Utilities.ToOptionString(KnownHostsStrategy);
            }
            if (Root is not null)
            {
                map["root"] = Utilities.ToOptionString(Root);
            }
            if (User is not null)
            {
                map["user"] = Utilities.ToOptionString(User);
            }
            return map;
        }
    }

}
