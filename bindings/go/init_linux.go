//go:build linux

package opendal

import "github.com/ebitengine/purego"

func openLibrary() (uintptr, error) {
	return purego.Dlopen("libopendal_c.so", purego.RTLD_NOW|purego.RTLD_GLOBAL)
}
