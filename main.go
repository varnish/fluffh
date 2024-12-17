// Copyright 2016 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// This main program mounts a loopback filesystem and forwards operations
// to an underlying file system. It now uses the newly rewritten loopback package.

package main

import (
	"flag"
	"fmt"
	"github.com/varnish/fluffh/httpfs"
	"log"
	"os"
	"os/signal"
	"path"
	"runtime/pprof"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// writeMemProfile writes memory profiles on SIGUSR1 signals.
func writeMemProfile(baseFilename string, signals <-chan os.Signal) {
	i := 0
	for range signals {
		filename := fmt.Sprintf("%s-%d.memprof", baseFilename, i)
		i++
		log.Printf("Writing memory profile to %s\n", filename)

		f, err := os.Create(filename)
		if err != nil {
			log.Printf("Create: %v", err)
			continue
		}
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Printf("WriteHeapProfile failed: %v", err)
		}
		if err := f.Close(); err != nil {
			log.Printf("close %v", err)
		}
	}
}

func main() {
	log.SetFlags(log.Lmicroseconds)

	// Define command-line flags
	debug := flag.Bool("debug", false, "print debugging messages")
	allowOther := flag.Bool("allow-other", false, "mount with -o allowother")
	quiet := flag.Bool("q", false, "quiet mode, no additional logging")
	ro := flag.Bool("ro", false, "mount read-only")
	directmount := flag.Bool("directmount", false, "use the mount syscall directly instead of fusermount")
	directmountstrict := flag.Bool("directmountstrict", false, "like directmount, but never fall back to fusermount")
	cpuprofile := flag.String("cpuprofile", "", "write CPU profile to file")
	memprofile := flag.String("memprofile", "", "write memory profile to file")

	flag.Parse()

	// Validate command-line arguments
	if flag.NArg() < 2 {
		fmt.Printf("usage: %s MOUNTPOINT BASEURL\n", path.Base(os.Args[0]))
		fmt.Println("\noptions:")
		flag.PrintDefaults()
		os.Exit(2)
	}

	mountpoint := flag.Arg(0)
	baseURL := flag.Arg(1)

	// Set up CPU profiling if requested
	if *cpuprofile != "" {
		if !*quiet {
			fmt.Printf("Writing CPU profile to %s\n", *cpuprofile)
		}
		f, err := os.Create(*cpuprofile)
		if err != nil {
			fmt.Println(err)
			os.Exit(3)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	// Set up memory profiling if requested
	if *memprofile != "" {
		if !*quiet {
			log.Printf("send SIGUSR1 to %d to dump memory profile", os.Getpid())
		}
		profSig := make(chan os.Signal, 1)
		signal.Notify(profSig, syscall.SIGUSR1)
		go writeMemProfile(*memprofile, profSig)
	}

	// Warn about needing a graceful unmount for profiling
	if *cpuprofile != "" || *memprofile != "" {
		if !*quiet {
			fmt.Println("Note: You must unmount gracefully, otherwise the profile file(s) will remain empty!")
		}
	}

	// Create the loopback root using the newly rewritten package
	loopbackRoot, err := httpfs.NewHttpRoot(baseURL)
	if err != nil {
		log.Fatalf("NewLoopbackRoot(%s): %v\n", baseURL, err)
	}

	// Setup filesystem options
	timeout := time.Second
	opts := &fs.Options{
		AttrTimeout:     &timeout,
		EntryTimeout:    &timeout,
		NullPermissions: true, // keep file permissions as-is

		MountOptions: fuse.MountOptions{
			AllowOther:        *allowOther,
			Debug:             *debug,
			DirectMount:       *directmount,
			DirectMountStrict: *directmountstrict,
			FsName:            baseURL,    // shown in "df -T"
			Name:              "loopback", // shown as fuse.loopback in "df -T"
		},
	}

	// If allow-other is set, ensure kernel performs permission checks
	if opts.AllowOther {
		opts.MountOptions.Options = append(opts.MountOptions.Options, "default_permissions")
	}

	// If read-only is requested, mount the filesystem as read-only
	if *ro {
		opts.MountOptions.Options = append(opts.MountOptions.Options, "ro")
	}

	// Setup logging if not in quiet mode
	if !*quiet {
		opts.Logger = log.New(os.Stderr, "", 0)
	}

	// Mount the filesystem
	server, err := fs.Mount(mountpoint, loopbackRoot, opts)
	if err != nil {
		log.Fatalf("Mount fail: %v\n", err)
	}

	if !*quiet {
		fmt.Println("Mounted!")
	}

	// Handle graceful shutdown on interrupt signals
	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-stopChan
		server.Unmount()
	}()

	// Wait for the filesystem to unmount
	server.Wait()
}
