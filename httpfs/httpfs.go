package httpfs

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"net/http"
	"net/url"
	"syscall"

	"github.com/colinmarc/cdb"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

const (
	cdbIndexFile = "index.cdb"
)

type Filesystem struct {
	baseUrl    string // points to a text file containing a URL to the root CDB file
	mountPoint string
	root       *RootNode
	client     *http.Client
}

type RootNode struct {
	filesystem *Filesystem
	inode      fs.Inode
}

// OnAdd is called when the root node is added to the filesystem.
// it will populate the root directory with the contents of the CDB file
// pointed at by the text file at the base URL.
func (r *RootNode) OnAdd(ctx context.Context) {
	// get the root CDB file:
	// dir, err := r.filesystem.GetDirNode(ctx, r.filesystem.baseUrl)
	// if err != nil {
	// TODO: figure how we gonna deal with errors.
	//	panic(err)
	//}
	panic("not implemented")
}

type FileNode struct {
	inode fs.Inode
	data  []byte // Example: File data, we will fetch this from the HTTP server on request.
}

type DirNode struct {
	inode   *fs.Inode
	parent  *fs.Inode
	entries map[string]*fs.Inode
}

func (r *RootNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, error) {
	// Example: Lookup a file or directory under the root.
	return nil, syscall.ENOENT
}

func (r *RootNode) Getattr(ctx context.Context, out *fuse.AttrOut) error {
	out.Mode = fuse.S_IFDIR | 0755 // Directory with read/write/execute for owner
	return nil
}

func (d *DirNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	entries := make([]fuse.DirEntry, 0)
	// Example: Add "hello.txt" as a directory entry
	entries = append(entries, fuse.DirEntry{
		Name: "hello.txt",
		Mode: fuse.S_IFREG,
	})
	return fs.NewListDirStream(entries), 0
}

func (f *FileNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	return nil, fuse.FOPEN_KEEP_CACHE, 0
}

func (f *FileNode) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	if off >= int64(len(f.data)) {
		return fuse.ReadResultData(nil), 0
	}
	end := off + int64(len(dest))
	if end > int64(len(f.data)) {
		end = int64(len(f.data))
	}
	return fuse.ReadResultData(f.data[off:end]), 0
}

func Mount(ctx context.Context, baseUrl, mountPoint string) (*Filesystem, error) {
	client := &http.Client{Timeout: 5}
	_, err := url.Parse(baseUrl)
	if err != nil {
		return nil, fmt.Errorf("url.Parse(baseUrl): %w", err)
	}
	// retrieve the base URL:
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseUrl, nil)
	if err != nil {
		return nil, fmt.Errorf("http.NewRequestWithContext(root pointer): %w", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http.Get(root pointer): %w", err)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		resp.Body.Close()
		return nil, fmt.Errorf("io.ReadAll(root pointer): %w", err)
	}
	err = resp.Body.Close()
	if err != nil {
		return nil, fmt.Errorf("resp.Body.Close(root pointer): %w", err)
	}

	// parse the base URL so we know it is valid.
	rootCDBPointer := string(data)
	_, err = url.Parse(rootCDBPointer)
	if err != nil {
		return nil, fmt.Errorf("url.Parse(root pointer): %w", err)
	}

	f := &Filesystem{
		baseUrl:    baseUrl,
		mountPoint: mountPoint,
		root:       nil,
		client:     client,
	}
	f.root = &RootNode{
		filesystem: f, // this will allow the Root Node to get the rootCDBPointer
	}
	return f, nil
}

func (fluff *Filesystem) Unmount() {
	panic("not implemented")
}

func (fluff *Filesystem) Wait(ctx context.Context) error {
	panic("not implemented")
	return nil
}

// GetDirNode fetches the CDB file at the given URL and returns a DirNode
func (fluff *Filesystem) GetDirNode(ctx context.Context, u string) (*DirNode, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, fmt.Errorf("http.NewRequestWithContext(%s): %w", u, err)
	}
	resp, err := fluff.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http.Get(%s): %w", u, err)
	}
	defer resp.Body.Close()
	// get all the bytes:
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("io.ReadAll: %w", err)
	}
	// make a reader:
	rd := bytes.NewReader(data)
	// open the CDB file:
	c, err := cdb.New(rd, nil)
	if err != nil {
		return nil, fmt.Errorf("cdb.New: %w", err)
	}
	entries := make(map[string]*fs.Inode)
	iter := c.Iter()
	for {
		ok := iter.Next()
		if !ok {
			break
		}
		key := string(iter.Key())
		entries[key] = decode(iter.Value())

	}
	return nil, nil
}

// decode will take a byte slice and return an Inode
// the Inode is JSON encoded.
func decode(value []byte) *fs.Inode {
	var m fs.Inode
	err := json.Unmarshal(value, &m)
	if err != nil {
		panic(err)
	}
	return &m
}

func hashStringToInode(name string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(name))
	return h.Sum64()
}
