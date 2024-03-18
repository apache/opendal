package main

import (
	"flag"
	"github.com/apache/opendal/codegen/pkg/config"
	"io"
	"log"
	"os"
	"path/filepath"
	"text/template"
)

var templates = flag.String("templates", "", "path to template files")

var outputRust = flag.String("output-rs", "", "path to output Rust files")

func main() {
	flag.Parse()
	if *templates == "" {
		log.Fatalf("fatal: templates is required")
	}
	if err := generateRustFiles(*templates, *outputRust); err != nil {
		log.Fatalf("fatal: %s", err)
	}
}

func generateRustFiles(templates string, output string) error {
	log.Printf("Generating Rust files from templates in %s to %s\n", templates, output)

	tmpl, err := template.ParseFiles(filepath.Join(templates, "rust.tmpl"))
	if err != nil {
		return err
	}

	var w io.Writer
	if output != "" {
		w, err = os.Create(output)
		if err != nil {
			return err
		}
	} else {
		w = os.Stdout
	}

	if err = tmpl.Execute(w, []config.Config{config.S3}); err != nil {
		return err
	}

	return nil
}
