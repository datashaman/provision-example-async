package main

import (
	"archive/tar"
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

func main() {
	input := flag.String("input", "", "built Linux executable")
	name := flag.String("name", "", "executable name inside the bundle")
	output := flag.String("output", "", "bundle destination")
	flag.Parse()
	if *input == "" || *name == "" || *output == "" || flag.NArg() != 0 {
		fail("usage: package --input EXECUTABLE --name NAME --output BUNDLE.tar.gz")
	}
	if err := pack(*input, *name, *output); err != nil {
		fail("package asynchronous example: %v", err)
	}
}

func pack(input, name, output string) error {
	if filepath.Base(name) != name || name == "." || name == ".." {
		return fmt.Errorf("bundle executable name must be a base name")
	}
	source, err := os.Open(input)
	if err != nil {
		return err
	}
	defer source.Close()
	info, err := source.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("%s is not a regular file", input)
	}
	temp, err := os.CreateTemp(filepath.Dir(output), ".provision-example-async-*")
	if err != nil {
		return err
	}
	defer os.Remove(temp.Name())
	gzipWriter := gzip.NewWriter(temp)
	gzipWriter.Header.ModTime = time.Unix(0, 0)
	gzipWriter.Header.OS = 255
	tarWriter := tar.NewWriter(gzipWriter)
	header := &tar.Header{Name: name, Mode: 0755, Size: info.Size(), ModTime: time.Unix(0, 0), Format: tar.FormatUSTAR}
	if err := tarWriter.WriteHeader(header); err != nil {
		return err
	}
	if _, err := io.Copy(tarWriter, source); err != nil {
		return err
	}
	if err := tarWriter.Close(); err != nil {
		return err
	}
	if err := gzipWriter.Close(); err != nil {
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	if err := os.Chmod(temp.Name(), 0644); err != nil {
		return err
	}
	return os.Rename(temp.Name(), output)
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
