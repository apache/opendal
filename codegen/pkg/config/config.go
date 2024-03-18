package config

import (
	"bufio"
	"fmt"
	"strings"
)

type Config struct {
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

func (f Field) RustType() string {
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
			res += fmt.Sprintf("///\n")
		}
	}

	if f.Default != "" {
		res += fmt.Sprintf("///\n")
		res += fmt.Sprintf("/// default to `%s` if not set.\n", f.Default)
	}

	return res
}

var S3 = Config{
	Name: "S3",
	Desc: "AWS S3 and compatible services (including minio, digitalocean space, Tencent Cloud Object Storage(COS) and so on) support.",
	Fields: []Field{
		{
			Name: "",
			Desc: `
root of this backend.

All operations will happen under this root.
`,
			Type:    fileTypeString,
			Default: "/",
		},
	},
}
