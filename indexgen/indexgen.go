// Package main provides a simple utility for generating index files for directories.
// The utility creates a CDB index file (index.cdb) in each directory. Each index file contains:
// - A messagepacks-serialized Metadata object for each immediate child (file or subdirectory).

package main

import (
	"fmt"
	"github.com/colinmarc/cdb"
	"github.com/varnish/fluffh/httpfs"
	"log"
	"os"
	"path/filepath"
	"syscall"
)

// statToMetadata gathers metadata for a file or directory
// and converts it to a FileMeta object. it gets the inode from the caller.
func statToMetadata(path string, info os.FileInfo, inodeNo uint64) (*httpfs.FileMeta, error) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return nil, fmt.Errorf("failed to get raw syscall.Stat_t for %s", path)
	}

	metadata := &httpfs.FileMeta{
		Size:  uint64(info.Size()),
		UID:   stat.Uid,
		GID:   stat.Gid,
		Mode:  uint32(info.Mode()),
		INO:   inodeNo,
		Atime: uint64(stat.Atimespec.Sec),
		Mtime: uint64(stat.Mtimespec.Sec),
		Ctime: uint64(stat.Ctimespec.Sec),
	}

	if info.IsDir() {
		metadata.Mode |= syscall.S_IFDIR
		metadata.Size = 0
	}

	return metadata, nil
}

// createIndexCDB creates a CDB index file for a single directory, listing only its immediate children.
func createIndexCDB(directory string, inode *uint64, bufSize int) error {
	indexPath := filepath.Join(directory, httpfs.IndexFile)
	tempPath := indexPath + ".tmp"

	entries, err := os.ReadDir(directory)
	if err != nil {
		return fmt.Errorf("os.ReadDir(%s): %w", directory, err)
	}

	constantDatabase, err := cdb.Create(tempPath)
	if err != nil {
		return fmt.Errorf("cdb.Create(%s): %w", tempPath, err)
	}
	defer constantDatabase.Close()

	// var fileList []string // Needed if we want a list of all filenames somewhere.
	// we use a pre-allocated buffer when marshalling the metadata to avoid reallocations
	buffer := make([]byte, bufSize)

	for _, entry := range entries {
		name := entry.Name()
		p := filepath.Join(directory, name)

		info, err := os.Stat(p)
		if err != nil {
			return fmt.Errorf("os.Stat(%s): %w", p, err)
		}

		// Skip the index file itself if it exists.
		if name == httpfs.IndexFile {
			continue
		}

		metadata, err := statToMetadata(p, info, *inode)
		if err != nil {
			return fmt.Errorf("statToMetadata(%s): %w", p, err)
		}

		*inode++ // Increment the inode number

		encodedBytes, err := metadata.MarshalMsg(buffer)
		if err != nil {
			return fmt.Errorf("json.Marshal: %w", err)
		}

		// Add the entry to the CDB under the child's name (not path)
		if err := constantDatabase.Put([]byte(name), encodedBytes); err != nil {
			return fmt.Errorf("constantDatabase.Put(%s): %w", name, err)
		}
		// fileList = append(fileList, name)
	}

	// We could consider adding a "__ALL__" entry that contains all the filenames to make it faster to list all files.

	// Finalize the CDB file
	if _, err := constantDatabase.Freeze(); err != nil {
		return fmt.Errorf("failed to finalize CDB: %w", err)
	}

	// Rename the temporary file to the final index file
	if err := os.Rename(tempPath, indexPath); err != nil {
		return fmt.Errorf("failed to rename temp file: %w", err)
	}

	return nil
}

// recurseDirectoriesAndIndex recursively creates index files for all directories
// Todo: Make this performant.
func recurseDirectoriesAndIndex(root string, bufsize int) error {
	// We start with inode number 3 because 0, 1, and 2 are reserved.
	inode := uint64(3) // We assign a unique inode number to each file or directory.

	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		// (this is a closure)
		if err != nil {
			return fmt.Errorf("filepath.Walk: %w", err)
		}
		// For each directory, create an index.cdb
		if info.IsDir() {
			fmt.Printf("Indexing directory: %s\n", path)
			if err := createIndexCDB(path, &inode, bufsize); err != nil {
				return fmt.Errorf("createIndexCDB(%s): %w", path, err)
			}
		}
		// Skip files
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
	// find the message size so we can preallocate a buffer:
	dummy := &httpfs.FileMeta{}
	msgSize := dummy.Msgsize()

	if err := recurseDirectoriesAndIndex(rootDir, msgSize); err != nil {
		log.Fatalf("Error indexing directory tree: %v\n", err)
	}
	fmt.Println("Indexing complete!")
}
