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

package main

import (
	"flag"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"text/template"

	"github.com/apache/opendal/codegen/pkg/config"
	"github.com/cli/safeexec"
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

	if output != "" {
		rustfmt, err := safeexec.LookPath("rustfmt")
		if err != nil {
			return err
		}
		if err := exec.Command(rustfmt, output).Run(); err != nil {
			return err
		}
	}

	return nil
}
