// Package main provides a simple utility for generating index files for directories.
// The utility creates a CDB index file (index.cdb) in each directory. Each index file contains:
// - A JSON-serialized Metadata object for each immediate child (file or subdirectory).
// - A "__ALL__" entry listing all immediate child filenames.
package main

import (
	"encoding/json"
	"fmt"
	"github.com/colinmarc/cdb"
	"github.com/varnish/fluffh/httpfs"
	"log"
	"os"
	"path/filepath"
	"syscall"
)

// statToMetadata gathers metadata for a file or directory
func statToMetadata(path string, info os.FileInfo) (*httpfs.FileMeta, error) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return nil, fmt.Errorf("failed to get raw syscall.Stat_t for %s", path)
	}

	metadata := &httpfs.FileMeta{
		Size: uint64(info.Size()),
		UID:  stat.Uid,
		GID:  stat.Gid,
		Mode: uint32(info.Mode()),
		INO:  stat.Ino,
	}

	if info.IsDir() {
		metadata.Mode |= syscall.S_IFDIR
		metadata.Size = 0
	}

	return metadata, nil
}

// createIndexCDB creates a CDB index file for a single directory, listing only its immediate children.
func createIndexCDB(directory string) error {
	indexPath := filepath.Join(directory, "index.cdb")
	tempPath := indexPath + ".tmp"

	entries, err := os.ReadDir(directory)
	if err != nil {
		return fmt.Errorf("ReadDir(%s): %w", directory, err)
	}

	writer, err := cdb.Create(tempPath)
	if err != nil {
		return fmt.Errorf("cdb.Create(%s): %w", tempPath, err)
	}
	defer writer.Close()

	var fileList []string

	for _, entry := range entries {
		name := entry.Name()
		p := filepath.Join(directory, name)

		info, err := os.Stat(p)
		if err != nil {
			return fmt.Errorf("Stat(%s): %w", p, err)
		}

		// Skip the index.cdb file itself if it exists.
		if name == "index.cdb" {
			continue
		}

		metadata, err := statToMetadata(p, info)
		if err != nil {
			return fmt.Errorf("statToMetadata(%s): %w", p, err)
		}

		jsonData, err := json.Marshal(metadata)
		if err != nil {
			return fmt.Errorf("json.Marshal: %w", err)
		}

		// Add the entry to the CDB under the child's name (not path)
		if err := writer.Put([]byte(name), jsonData); err != nil {
			return fmt.Errorf("writer.Put(%s): %w", name, err)
		}

		fileList = append(fileList, name)
	}

	// Add __ALL__ entry listing all filenames
	allData, err := json.Marshal(fileList)
	if err != nil {
		return fmt.Errorf("json.Marshal(fileList): %w", err)
	}
	if err := writer.Put([]byte("__ALL__"), allData); err != nil {
		return fmt.Errorf("writer.Put(__ALL__): %w", err)
	}

	// Finalize the CDB file
	if _, err := writer.Freeze(); err != nil {
		return fmt.Errorf("failed to finalize CDB: %w", err)
	}

	// Rename the temporary file to the final index file
	if err := os.Rename(tempPath, indexPath); err != nil {
		return fmt.Errorf("failed to rename temp file: %w", err)
	}

	return nil
}

// recurseDirectoriesAndIndex recursively creates index files for all directories
func recurseDirectoriesAndIndex(root string) error {
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return fmt.Errorf("filepath.Walk: %w", err)
		}

		// For each directory, create an index.cdb
		if info.IsDir() {
			fmt.Printf("Indexing directory: %s\n", path)
			if err := createIndexCDB(path); err != nil {
				return fmt.Errorf("createIndexCDB(%s): %w", path, err)
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("error walking root directory: %w", err)
	}
	return nil
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run indexgen.go <directory>")
		os.Exit(1)
	}

	rootDir := os.Args[1]
	if _, err := os.Stat(rootDir); os.IsNotExist(err) {
		log.Fatalf("Directory '%s' does not exist\n", rootDir)
	}

	if err := recurseDirectoriesAndIndex(rootDir); err != nil {
		log.Fatalf("Error indexing directory tree: %v\n", err)
	}
	fmt.Println("Indexing complete!")
}
