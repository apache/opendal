package main

import (
	"github.com/apache/opendal/codegen/pkg/config"
	"os"
	"path/filepath"
	"text/template"
)

func main() {
	if err := generate(); err != nil {
		panic(err.Error())
	}
}

func generate() error {
	p, err := os.Executable()
	if err != nil {
		return err
	}
	basedir := filepath.Dir(p)

	tmpl, err := template.ParseFiles(filepath.Join(basedir, "..", "tmpl/rust.tmpl"))
	if err != nil {
		return err
	}

	if err = tmpl.Execute(os.Stdout, config.S3); err != nil {
		return err
	}

	return nil
}
