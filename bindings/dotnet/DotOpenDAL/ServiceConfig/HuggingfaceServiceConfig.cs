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
    /// Configuration for service huggingface.
    /// </summary>
    public sealed class HuggingfaceServiceConfig : IServiceConfig
    {
        /// <summary>
        /// Repo id of this backend. This is required.
        /// </summary>
        public string? RepoId { get; init; }
        /// <summary>
        /// Repo type of this backend. Default is model. Available values: model dataset
        /// </summary>
        public string? RepoType { get; init; }
        /// <summary>
        /// Revision of this backend. Default is main.
        /// </summary>
        public string? Revision { get; init; }
        /// <summary>
        /// Root of this backend. Can be "/path/to/dir". Default is "/".
        /// </summary>
        public string? Root { get; init; }
        /// <summary>
        /// Token of this backend. This is optional.
        /// </summary>
        public string? Token { get; init; }

        public string Scheme => "huggingface";

        public IReadOnlyDictionary<string, string> ToOptions()
        {
            var map = new Dictionary<string, string>();
            if (RepoId is not null)
            {
                map["repo_id"] = Utilities.ToOptionString(RepoId);
            }
            if (RepoType is not null)
            {
                map["repo_type"] = Utilities.ToOptionString(RepoType);
            }
            if (Revision is not null)
            {
                map["revision"] = Utilities.ToOptionString(Revision);
            }
            if (Root is not null)
            {
                map["root"] = Utilities.ToOptionString(Root);
            }
            if (Token is not null)
            {
                map["token"] = Utilities.ToOptionString(Token);
            }
            return map;
        }
    }

}
