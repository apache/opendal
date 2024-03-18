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

package config

type Config struct {
	ModCfg string
	Name   string
	Desc   string
	Fields []Field
}

type Field struct {
	Name string
	Desc string

	Type        FieldType
	RefinedType RefinedFieldType

	Required  bool
	Sensitive bool

	Default   string   // Default is the default value for the config field if not set.
	Available []string // Available is a list of available values for the config field.
	Example   []string // Example is a list of example values for the config field.
}

type FieldType uint8

const (
	fileTypeNone FieldType = iota
	fileTypeBool
	fileTypeInt
	fieldTypeLong
	fieldTypeFloat
	fieldTypeDouble
	fileTypeString
)

type RefinedFieldType uint8

const (
	refinedTypeNone RefinedFieldType = iota
	refinedTypeRustUsize
)
