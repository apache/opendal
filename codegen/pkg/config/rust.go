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

import (
	"bufio"
	"fmt"
	"strings"

	"github.com/iancoleman/strcase"
)

func (c Config) RustStructName() string {
	return fmt.Sprintf("%sConfig", strcase.ToCamel(c.Name))
}

func (f Field) RustType() string {
	typ := f.rustType()
	if f.Required {
		return typ
	}
	return fmt.Sprintf("Option<%s>", typ)
}

func (f Field) rustType() string {
	switch f.RefinedType {
	case refinedTypeNone:
		break
	case refinedTypeRustUsize:
		return "usize"
	}

	switch f.Type {
	case fileTypeNone:
		panic("field type must be set")
	case fileTypeBool:
		return "bool"
	case fileTypeInt:
		return "i32"
	case fieldTypeLong:
		return "i64"
	case fieldTypeFloat:
		return "f32"
	case fieldTypeDouble:
		return "f64"
	case fileTypeString:
		return "String"
	}

	panic("unreachable: field types are all handled above")
}

func (f Field) RustComment() string {
	res := ""

	desc := strings.TrimSpace(f.Desc)
	scanner := bufio.NewScanner(strings.NewReader(desc))
	for scanner.Scan() {
		if scanner.Text() != "" {
			res += fmt.Sprintf("/// %s\n", scanner.Text())
		} else {
			res += fmt.Sprintln("///")
		}
	}

	if f.Default != "" {
		res += fmt.Sprintln("///")
		res += fmt.Sprintf("/// Default to `%s` if not set.\n", f.Default)
	}

	if f.Required {
		res += fmt.Sprintln("///")
		res += fmt.Sprintln("/// Required.")
	}

	if len(f.Example) > 0 {
		res += fmt.Sprintln("///")
		res += fmt.Sprintln("/// For examples:")
		for _, e := range f.Example {
			res += fmt.Sprintf("/// - %s\n", e)
		}
	}

	if len(f.Available) > 0 {
		res += fmt.Sprintln("///")
		res += fmt.Sprintln("/// Available values:")
		for _, a := range f.Available {
			res += fmt.Sprintf("/// - %s\n", a)
		}
	}

	return res
}

func (f Field) RustDebugField() string {
	if !f.Sensitive {
		return fmt.Sprintf("&self.%s", f.Name)
	}

	if f.Required {
		return fmt.Sprintf("desensitize_secret(&self.%s)", f.Name)
	}

	return fmt.Sprintf("&self.%s.as_deref().map(desensitize_secret)", f.Name)
}
