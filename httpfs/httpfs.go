package httpfs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/colinmarc/cdb"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"syscall"
)

//go:generate go run github.com/tinylib/msgp

const (
	IndexFile       = "index.cdb"
	chunkSize int64 = 1 << 20 // 1MB
)

// httpRoot represents the root of the HTTP-based filesystem.
type httpRoot struct {
	BaseURL string
	// NewNode creates a new node given a URL and metadata.
	NewNode func(root *httpRoot, parent *fs.Inode, name, url string, meta *FileMeta) fs.InodeEmbedder
}

// FileMeta holds the metadata for a file or directory as read from the index CDB
// Exported because we need to marshal/unmarshal it.
type FileMeta struct {
	Size uint64 `json:"size" msg:"size"`
	UID  uint32 `json:"uid" msg:"uid"`
	GID  uint32 `json:"gid" msg:"gid"`
	Mode uint32 `json:"mode" msg:"mode"`
	INO  uint64 `json:"ino" msg:"ino"` // Will be computed from URL if zero.
}

// Directory listing structure: map of filename to FileMeta.
type dirListing map[string]FileMeta

// httpNode represents a file or directory node in the HTTP filesystem.
type httpNode struct {
	fs.Inode
	RootData *httpRoot
	URL      string
	IsDir    bool
	Meta     *FileMeta
}

// Ensure httpNode implements certain interfaces:
var _ fs.NodeStatfser = (*httpNode)(nil)
var _ fs.NodeLookuper = (*httpNode)(nil)
var _ fs.NodeGetattrer = (*httpNode)(nil)
var _ fs.NodeReaddirer = (*httpNode)(nil)
var _ fs.NodeOpener = (*httpNode)(nil)

func getCDB(u string) (*cdb.CDB, error) {
	slog.Debug("getCDB: fetching CDB", "url", u)
	resp, err := http.Get(u)
	if err != nil {
		slog.Error("http.Get failed", "url", u, "error", err)
		return nil, fmt.Errorf("http.Get: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		slog.Warn("Non-200 response code while fetching CDB", "url", u, "status_code", resp.StatusCode)
		return nil, syscall.ENOENT
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		slog.Error("Error reading response body", "url", u, "error", err)
		return nil, fmt.Errorf("io.ReadAll: %w", err)
	}
	// make reader
	readerBytes := bytes.NewReader(bodyBytes)
	// create cdb
	dbase, err := cdb.New(readerBytes, nil)
	if err != nil {
		slog.Error("Error creating CDB database", "url", u, "error", err)
		return nil, fmt.Errorf("cdb.New: %w", err)
	}

	slog.Debug("getCDB: successfully created CDB", "url", u)
	return dbase, nil
}

// fetchDirListing fetches the directory listing from the index file.
// used by Readdir
func fetchDirListing(u string) (dirListing, error) {
	slog.Debug("fetchDirListing: start", "url", u)
	var err error
	u, err = url.JoinPath(u, IndexFile)
	if err != nil {
		slog.Error("Error joining path for index file", "base_url", u, "error", err)
		return nil, fmt.Errorf("url.JoinPath: %w", err)
	}

	dbase, err := getCDB(u)
	if err != nil {
		slog.Error("fetchDirListing: getCDB failed", "url", u, "error", err)
		return nil, fmt.Errorf("getCDB: %w", err)
	}
	// iterate over the cdb
	iter := dbase.Iter()
	var listing = make(dirListing)
	i := 0
	for {
		if !iter.Next() {
			break
		}
		var meta FileMeta
		if _, err := meta.UnmarshalMsg(iter.Value()); err != nil {
			slog.Error("Failed to unmarshal file metadata", "key", string(iter.Key()), "error", err)
			return nil, fmt.Errorf("unmarshal metadata: %w", err)
		}
		listing[string(iter.Key())] = meta
		i++
		// advance to the next record
		if !iter.Next() {
			break // no more records
		}
	}

	slog.Debug("fetchDirListing: fetched directory listing", "url", u, "count", i)
	return listing, nil
}

func lookupInDir(u, filename string) (*FileMeta, error) {
	slog.Debug("lookupInDir: start", "dir_url", u, "filename", filename)
	var err error
	u, err = url.JoinPath(u, IndexFile)
	if err != nil {
		slog.Error("Error joining path to index file", "base_url", u, "filename", filename, "error", err)
		return nil, fmt.Errorf("url.JoinPath: %w", err)
	}

	dbase, err := getCDB(u)
	if err != nil {
		slog.Error("lookupInDir: getCDB failed", "url", u, "filename", filename, "error", err)
		return nil, fmt.Errorf("getCDB: %w", err)
	}

	// lookup in the cdb
	value, err := dbase.Get([]byte(filename))
	if err != nil {
		slog.Error("CDB Get failed", "url", u, "filename", filename, "error", err)
		return nil, fmt.Errorf("cdb.Get: %w", err)
	}
	if value == nil {
		slog.Warn("File not found in directory index", "url", u, "filename", filename)
		return nil, syscall.ENOENT
	}

	var meta FileMeta
	if _, err := meta.UnmarshalMsg(value); err != nil {
		slog.Error("Failed to unmarshal file metadata", "url", u, "filename", filename, "error", err)
		return nil, fmt.Errorf("unmarshal metadata: %w", err)
	}

	slog.Debug("lookupInDir: found file metadata", "url", u, "filename", filename, "meta", meta)
	return &meta, nil
}

// NewHttpRoot initializes the HTTP root node.
func NewHttpRoot(baseURL string) (fs.InodeEmbedder, error) {
	slog.Info("NewHttpRoot: initializing HTTP root", "baseURL", baseURL)
	if !strings.HasSuffix(baseURL, "/") {
		baseURL += "/"
	}

	root := &httpRoot{
		BaseURL: baseURL,
	}
	// Default NewNode function if none provided
	root.NewNode = func(r *httpRoot, parent *fs.Inode, name, url string, meta *FileMeta) fs.InodeEmbedder {
		slog.Debug("NewHttpRoot: creating new node", "name", name, "url", url, "meta", meta)
		return &httpNode{
			RootData: r,
			URL:      url,
			IsDir:    meta.Mode&syscall.S_IFDIR != 0,
			Meta:     meta,
		}
	}

	// Fetch root dir metadata to ensure it's a directory.
	_, err := fetchDirListing(baseURL)
	if err != nil {
		slog.Error("Failed to fetch root directory listing", "baseURL", baseURL, "error", err)
		return nil, err
	}

	// The root node metadata can be taken from the directory itself if provided, or synthesized.
	// We'll assume the directory entry for '.' is not explicitly given, so we synthesize:
	meta := &FileMeta{
		Size: 0,
		UID:  0,
		GID:  0,
		Mode: uint32(syscall.S_IFDIR | 0755),
		INO:  hashStringToUint64(baseURL),
	}

	rootNode := &httpNode{
		RootData: root,
		URL:      baseURL,
		IsDir:    true,
		Meta:     meta,
	}

	slog.Info("NewHttpRoot: initialized root node successfully", "baseURL", baseURL)
	return rootNode, nil
}

func (n *httpNode) Statfs(ctx context.Context, out *fuse.StatfsOut) syscall.Errno {
	// Just return zeros as suggested.
	return 0
}

// Getattr retrieves attributes for this node.
func (n *httpNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	slog.Debug("Getattr called", "url", n.URL, "isDir", n.IsDir, "meta", n.Meta)
	out.Mode = n.Meta.Mode
	out.Size = n.Meta.Size
	out.Uid = n.Meta.UID
	out.Gid = n.Meta.GID
	out.Ino = n.Meta.INO
	// Set some sensible times
	out.SetTimes(nil, nil, nil)
	return fs.OK
}

// Lookup a child node by name.
func (n *httpNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	slog.Debug("Lookup called", "dir_url", n.URL, "name", name)

	if !n.IsDir {
		slog.Warn("Attempted lookup on a file node", "url", n.URL, "name", name)
		return nil, syscall.ENOENT
	}

	// Get entry from directory listing

	meta, err := lookupInDir(n.URL, name)
	if err != nil {
		slog.Error("Lookup: failed to lookup entry in directory", "url", n.URL, "name", name, "error", err)
		return nil, fs.ToErrno(err)
	}

	// Construct URL for this entry
	entryURL := n.URL + name
	if meta.INO == 0 {
		meta.INO = hashStringToUint64(entryURL)
	}

	childNode := n.RootData.NewNode(n.RootData, n.EmbeddedInode(), name, entryURL, meta)
	ch := n.NewInode(ctx, childNode, fs.StableAttr{
		Mode: meta.Mode,
		Ino:  meta.INO,
	})

	out.Attr.Mode = meta.Mode
	out.Attr.Size = meta.Size
	out.Attr.Uid = meta.UID
	out.Attr.Gid = meta.GID
	out.Attr.Ino = meta.INO
	slog.Debug("Lookup: child node created successfully", "url", entryURL)
	return ch, 0
}

// Readdir reads the directory contents.
func (n *httpNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	slog.Debug("Readdir called", "url", n.URL, "isDir", n.IsDir)
	if !n.IsDir {
		slog.Warn("Readdir attempted on a file node", "url", n.URL)
		return nil, syscall.ENOTDIR
	}

	listing, err := fetchDirListing(n.URL)
	if err != nil {
		slog.Error("Readdir: failed to fetch directory listing", "url", n.URL, "error", err)
		return nil, fs.ToErrno(err)
	}

	entries := make([]fuse.DirEntry, 0, len(listing))
	for name, meta := range listing {
		entries = append(entries, fuse.DirEntry{
			Name: name,
			Ino:  meta.INO,
			Mode: meta.Mode,
		})
	}

	slog.Debug("Readdir: returning directory entries", "url", n.URL, "count", len(entries))
	return fs.NewListDirStream(entries), fs.OK
}

// Open opens a file. For directories, this might be used to read directories if needed.
// For files, it will initiate an HTTP GET request.
func (n *httpNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	slog.Debug("Open called", "url", n.URL, "isDir", n.IsDir)
	if n.IsDir {
		slog.Debug("Open: directory node opened, returning nil file handle", "url", n.URL)
		return nil, 0, 0
	}

	// Create a file handle that wraps resp.Body
	fh, err := newHttpFileHandle(n.URL, http.DefaultClient)
	if err != nil {
		slog.Error("Open: failed to open file", "url", n.URL, "error", err)
		return nil, 0, fs.ToErrno(err)
	}

	slog.Debug("Open: file handle created successfully", "url", n.URL)
	return fh, 0, 0
}

var _ fs.FileHandle = (*httpFileHandle)(nil)
var _ fs.FileReader = (*httpFileHandle)(nil)
var _ fs.FileReleaser = (*httpFileHandle)(nil)

// httpFileHandle represents a file handle for an HTTP-based file.
type httpFileHandle struct {
	URL        string
	HTTPClient *http.Client
	FileSize   int64

	mu       sync.Mutex
	chunkBuf map[int64][]byte
}

// newHttpFileHandle initializes a new httpFileHandle.
// It performs a HEAD request to determine the file size.
func newHttpFileHandle(url string, client *http.Client) (*httpFileHandle, error) {
	slog.Debug("newHttpFileHandle: initializing", "url", url)
	if client == nil {
		client = http.DefaultClient
	}

	// Perform a HEAD request to get the file size
	req, err := http.NewRequest("HEAD", url, nil)
	if err != nil {
		slog.Error("newHttpFileHandle: creating HEAD request failed", "url", url, "error", err)
		return nil, fmt.Errorf("creating HEAD request: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		slog.Error("newHttpFileHandle: HEAD request failed", "url", url, "error", err)
		return nil, fmt.Errorf("performing HEAD request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		slog.Warn("newHttpFileHandle: non-200 HEAD response", "url", url, "status_code", resp.StatusCode)
		return nil, fmt.Errorf("unexpected status code %d on HEAD request", resp.StatusCode)
	}

	// Extract the Content-Length header to determine file size
	contentLength := resp.Header.Get("Content-Length")
	if contentLength == "" {
		slog.Error("newHttpFileHandle: no Content-Length in HEAD response", "url", url)
		return nil, errors.New("Content-Length header is missing")
	}

	var fileSize int64
	_, err = fmt.Sscanf(contentLength, "%d", &fileSize)
	if err != nil {
		slog.Error("newHttpFileHandle: parsing Content-Length failed", "url", url, "error", err)
		return nil, fmt.Errorf("parsing Content-Length: %w", err)
	}

	slog.Debug("newHttpFileHandle: file handle initialized", "url", url, "size", fileSize)
	return &httpFileHandle{
		URL:        url,
		HTTPClient: client,
		FileSize:   fileSize,
		chunkBuf:   make(map[int64][]byte),
	}, nil
}

// Read fetches a specific range of bytes from the remote file.
// It uses chunk-based HTTP range requests to optimize memory usage and performance.
func (f *httpFileHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	slog.Debug("Read called", "url", f.URL, "offset", off, "bytes_requested", len(dest))

	if off < 0 || off >= f.FileSize {
		slog.Warn("Read: invalid offset", "url", f.URL, "offset", off, "file_size", f.FileSize)
		return nil, syscall.EINVAL
	}

	// Determine the number of bytes to read, shortening it if it goes past the end of the file.
	readLength := int64(len(dest))
	if off+readLength > f.FileSize {
		readLength = f.FileSize - off
	}

	firstChunk := off / chunkSize // index of the FIRST chunk to read from
	lastChunk := (off + readLength - 1) / chunkSize

	// Read the data into memory:
	for i := firstChunk; i <= lastChunk; i++ {
		_, exists := f.chunkBuf[i]
		if !exists {
			if err := f.readChunkIntoCache(i); err != nil {
				slog.Error("Read: readChunkIntoCache failed", "url", f.URL, "offset", off, "error", err)
				return nil, fs.ToErrno(err)
			}
		}
	}

	// we assume all the chunks are present in the cache now.
	// Now we're ready to copy the data into the destination buffer.
	for i := firstChunk; i <= lastChunk; i++ {
		if i == firstChunk {
			copy(dest, f.chunkBuf[i][off%chunkSize:])
			slog.Debug("Read: copied first chunk", "url", f.URL, "offset", off, "bytes_read", len(f.chunkBuf[i][off%chunkSize:]))
			continue
		}
		if i == lastChunk {
			bytesToRead := readLength - (chunkSize - off%chunkSize)
			copy(dest[chunkSize-off%chunkSize:], f.chunkBuf[i][:bytesToRead])
			slog.Debug("Read: copied last chunk", "url", f.URL, "offset", off, "bytes_read", bytesToRead)
			continue
		}
		// # chunk is in the middle, copy the whole chunk
		copy(dest[(i-firstChunk)*chunkSize-off%chunkSize:], f.chunkBuf[i])
		slog.Debug("Read: copied middle chunk", "url", f.URL, "offset", off)
	}
	slog.Debug("Read: completed", "url", f.URL, "offset", off, "bytes_read", readLength)
	return fuse.ReadResultData(dest), fs.OK
}

// readChunkIntoCache retrieves a specific chunk either from the cache or by fetching it via HTTP.
func (f *httpFileHandle) readChunkIntoCache(chunkIndex int64) error {

	// Check if the chunk is already cached
	f.mu.Lock()
	if _, exists := f.chunkBuf[chunkIndex]; exists {
		f.mu.Unlock()
		slog.Debug("readChunkIntoCache: returning cached chunk", "url", f.URL, "chunk_index", chunkIndex)
		return nil
	}
	f.mu.Unlock()

	// Calculate the byte range for the chunk
	startByte := chunkIndex * chunkSize
	endByte := startByte + chunkSize - 1
	if endByte >= f.FileSize {
		endByte = f.FileSize - 1
	}

	// Create a new HTTP request with the Range header
	req, err := http.NewRequest(http.MethodGet, f.URL, nil)
	if err != nil {
		slog.Error("readChunkIntoCache: creating GET request failed", "url", f.URL, "chunk_index", chunkIndex, "error", err)
		return fmt.Errorf("creating GET request: %w", err)
	}
	rangeHeader := fmt.Sprintf("bytes=%d-%d", startByte, endByte)
	req.Header.Set("Range", rangeHeader)

	slog.Debug("readChunkIntoCache: fetching chunk", "url", f.URL, "range", rangeHeader)
	resp, err := f.HTTPClient.Do(req)
	if err != nil {
		slog.Error("readChunkIntoCache: GET request failed", "url", f.URL, "range", rangeHeader, "error", err)
		return fmt.Errorf("performing GET request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusOK {
		slog.Warn("readChunkIntoCache: unexpected status code", "url", f.URL, "range", rangeHeader, "status_code", resp.StatusCode)
		return fmt.Errorf("unexpected status code %d for range request", resp.StatusCode)
	}

	// Read the response body
	chunkData, err := io.ReadAll(resp.Body)
	if err != nil {
		slog.Error("readChunkIntoCache: reading response body failed", "url", f.URL, "range", rangeHeader, "error", err)
		return fmt.Errorf("reading response body: %w", err)
	}

	// Cache the chunk data
	f.mu.Lock()
	f.chunkBuf[chunkIndex] = chunkData
	f.mu.Unlock()
	slog.Debug("readChunkIntoCache: chunk fetched and cached", "url", f.URL, "chunk_index", chunkIndex, "chunk_size", len(chunkData))
	return nil
}

func (f *httpFileHandle) Release(ctx context.Context) syscall.Errno {
	slog.Debug("Release called", "url", f.URL)
	return fs.OK
}
