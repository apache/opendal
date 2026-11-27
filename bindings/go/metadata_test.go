// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package opendal

import "time"

// The Metadata methods released before metadata.go was generated from core.
// The generator must keep each of them with this exact signature.
var (
	_ func(*Metadata) (string, bool)    = (*Metadata).CacheControl
	_ func(*Metadata) (string, bool)    = (*Metadata).ContentDisposition
	_ func(*Metadata) (string, bool)    = (*Metadata).ContentEncoding
	_ func(*Metadata) (string, bool)    = (*Metadata).ContentMD5
	_ func(*Metadata) (string, bool)    = (*Metadata).ContentType
	_ func(*Metadata) (string, bool)    = (*Metadata).ETag
	_ func(*Metadata) uint64            = (*Metadata).ContentLength
	_ func(*Metadata) bool              = (*Metadata).IsFile
	_ func(*Metadata) bool              = (*Metadata).IsDir
	_ func(*Metadata) (bool, bool)      = (*Metadata).IsCurrent
	_ func(*Metadata) bool              = (*Metadata).IsDeleted
	_ func(*Metadata) time.Time         = (*Metadata).LastModified
	_ func(*Metadata) EntryMode         = (*Metadata).Mode
	_ func(*Metadata) (string, bool)    = (*Metadata).Version
	_ func(*Metadata) map[string]string = (*Metadata).UserMetadata
)
