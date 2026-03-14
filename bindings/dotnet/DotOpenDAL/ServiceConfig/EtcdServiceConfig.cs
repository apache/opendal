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
    /// Configuration for service etcd.
    /// </summary>
    public sealed class EtcdServiceConfig : IServiceConfig
    {
        /// <summary>
        /// certificate authority file path default is None
        /// </summary>
        public string? CaPath { get; init; }
        /// <summary>
        /// cert path default is None
        /// </summary>
        public string? CertPath { get; init; }
        /// <summary>
        /// network address of the Etcd services. If use https, must set TLS options: ca_path, cert_path, key_path. e.g. "127.0.0.1:23790,127.0.0.1:23791,127.0.0.1:23792" or "http://127.0.0.1:23790,http://127.0.0.1:23791,http://127.0.0.1:23792" or "https://127.0.0.1:23790,https://127.0.0.1:23791,https://127.0.0.1:23792" default is "http://127.0.0.1:2379"
        /// </summary>
        public string? Endpoints { get; init; }
        /// <summary>
        /// key path default is None
        /// </summary>
        public string? KeyPath { get; init; }
        /// <summary>
        /// the password for authentication default is None
        /// </summary>
        public string? Password { get; init; }
        /// <summary>
        /// the working directory of the etcd service. Can be "/path/to/dir" default is "/"
        /// </summary>
        public string? Root { get; init; }
        /// <summary>
        /// the username to connect etcd service. default is None
        /// </summary>
        public string? Username { get; init; }

        public string Scheme => "etcd";

        public IReadOnlyDictionary<string, string> ToOptions()
        {
            var map = new Dictionary<string, string>();
            if (CaPath is not null)
            {
                map["ca_path"] = Utilities.ToOptionString(CaPath);
            }
            if (CertPath is not null)
            {
                map["cert_path"] = Utilities.ToOptionString(CertPath);
            }
            if (Endpoints is not null)
            {
                map["endpoints"] = Utilities.ToOptionString(Endpoints);
            }
            if (KeyPath is not null)
            {
                map["key_path"] = Utilities.ToOptionString(KeyPath);
            }
            if (Password is not null)
            {
                map["password"] = Utilities.ToOptionString(Password);
            }
            if (Root is not null)
            {
                map["root"] = Utilities.ToOptionString(Root);
            }
            if (Username is not null)
            {
                map["username"] = Utilities.ToOptionString(Username);
            }
            return map;
        }
    }

}
