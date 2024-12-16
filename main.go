// Copyright 2016 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// This program is the analogon of libfuse's hello.c, a a program that
// exposes a single file "file.txt" in the root directory.
package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/varnish/fluffh/httpfs"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	err := run(ctx, os.Args[1:])
	if err != nil {
		fmt.Println("Error:", err)
		os.Exit(1)
	}
	fmt.Println("Clean exit")
	os.Exit(0)
}

func run(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	// can add more flags here.

	err := fs.Parse(args)
	if err != nil {
		return fmt.Errorf("flag.Parse: %w\n", err)
	}
	// first argument is the url to mount, second is the mountpoint.
	if fs.NArg() != 2 {
		return fmt.Errorf("invalid number of arguments(%d), expected 2\n", fs.NArg())
	}

	server, err := httpfs.Mount(ctx, fs.Arg(0), fs.Arg(1))
	if err != nil {
		return fmt.Errorf("fluffh.Mount: %w\n", err)
	}
	defer server.Unmount()
	err = server.Wait(ctx)
	if err != nil {
		return fmt.Errorf("server.Wait: %w\n", err)
	}
	return nil
}
