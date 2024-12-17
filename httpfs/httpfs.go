package httpfs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"syscall"
)

//go:generate go run github.com/tinylib/msgp

const (
	indexFile       = "index.json"
	chunkSize int64 = 1 << 20 // 1MB
)

// HttpRoot represents the root of the HTTP-based filesystem.
type HttpRoot struct {
	BaseURL string
	// NewNode creates a new node given a URL and metadata.
	NewNode func(root *HttpRoot, parent *fs.Inode, name, url string, meta FileMeta) fs.InodeEmbedder
}

// FileMeta holds the metadata for a file or directory as read from index.json.
type FileMeta struct {
	Size uint64 `json:"size" msg:"size"`
	UID  uint32 `json:"uid" msg:"uid"`
	GID  uint32 `json:"gid" msg:"gid"`
	Mode uint32 `json:"mode" msg:"mode"`
	INO  uint64 `json:"ino" msg:"ino"` // Will be computed from URL if zero.
}

// Directory listing structure: map of filename to FileMeta.
type DirListing map[string]FileMeta

// HttpNode represents a file or directory node in the HTTP filesystem.
type HttpNode struct {
	fs.Inode
	RootData *HttpRoot
	URL      string
	IsDir    bool
	Meta     FileMeta
}

// Ensure HttpNode implements certain interfaces:
var _ fs.NodeStatfser = (*HttpNode)(nil)
var _ fs.NodeLookuper = (*HttpNode)(nil)
var _ fs.NodeGetattrer = (*HttpNode)(nil)
var _ fs.NodeReaddirer = (*HttpNode)(nil)
var _ fs.NodeOpener = (*HttpNode)(nil)

// fetchDirListing fetches and parses an index.json for a directory.
func fetchDirListing(u string) (DirListing, error) {
	var err error
	u, err = url.JoinPath(u, indexFile)
	if err != nil {
		return nil, fmt.Errorf("url.JoinPath: %w", err)
	}

	resp, err := http.Get(u)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, syscall.ENOENT
	}
	var listing DirListing
	if err := json.NewDecoder(resp.Body).Decode(&listing); err != nil {
		return nil, fmt.Errorf("json.Decode'%s': %w", u, err)
	}
	// Compute INO from URL if not set
	for name, meta := range listing {
		entryURL := u + name
		if meta.INO == 0 {
			meta.INO = hashStringToUint64(entryURL)
			listing[name] = meta
		}
	}
	return listing, nil
}

// NewHttpRoot initializes the HTTP root node.
func NewHttpRoot(baseURL string) (fs.InodeEmbedder, error) {
	if !strings.HasSuffix(baseURL, "/") {
		baseURL += "/"
	}
	root := &HttpRoot{
		BaseURL: baseURL,
	}
	// Default NewNode function if none provided
	root.NewNode = func(r *HttpRoot, parent *fs.Inode, name, url string, meta FileMeta) fs.InodeEmbedder {
		return &HttpNode{
			RootData: r,
			URL:      url,
			IsDir:    (meta.Mode&syscall.S_IFDIR != 0),
			Meta:     meta,
		}
	}

	// Fetch root dir metadata from index.json (or set default if needed) to see that it's a directory.
	_, err := fetchDirListing(baseURL)
	if err != nil {
		// No index.json at root means something's wrong.
		return nil, err
	}

	// The root node metadata can be taken from the directory itself if provided, or synthesized.
	// We'll assume the directory entry for '.' is not explicitly given, so we synthesize:
	meta := FileMeta{
		Size: 0,
		UID:  0,
		GID:  0,
		Mode: uint32(syscall.S_IFDIR | 0755),
		INO:  hashStringToUint64(baseURL),
	}

	rootNode := &HttpNode{
		RootData: root,
		URL:      baseURL,
		IsDir:    true,
		Meta:     meta,
	}

	return rootNode, nil
}

func (n *HttpNode) Statfs(ctx context.Context, out *fuse.StatfsOut) syscall.Errno {
	// Just return zeros as suggested.
	return 0
}

// Getattr retrieves attributes for this node.
func (n *HttpNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	// If it's a directory, we have Meta already. If it's a file, we also have Meta.
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
func (n *HttpNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if !n.IsDir {
		// Not a directory, no children.
		return nil, syscall.ENOENT
	}

	// Fetch directory listing
	listing, err := fetchDirListing(n.URL)
	if err != nil {
		return nil, fs.ToErrno(err)
	}

	meta, ok := listing[name]
	if !ok {
		// Maybe it's a directory name that ends with '/', try that
		if !strings.HasSuffix(name, "/") {
			nameDir := name + "/"
			meta, ok = listing[nameDir]
			name = nameDir
		}
		if !ok {
			return nil, syscall.ENOENT
		}
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
	return ch, 0
}

// Readdir reads the directory contents.
func (n *HttpNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	if !n.IsDir {
		return nil, syscall.ENOTDIR
	}

	listing, err := fetchDirListing(n.URL)
	if err != nil {
		return nil, fs.ToErrno(err)
	}

	entries := make([]fuse.DirEntry, 0, len(listing))
	for name, meta := range listing {
		e := fuse.DirEntry{
			Name: name,
			Ino:  meta.INO,
			Mode: meta.Mode,
		}
		entries = append(entries, e)
	}

	return fs.NewListDirStream(entries), fs.OK
}

// Open opens a file. For directories, this might be used to read directories if needed.
// For files, it will initiate an HTTP GET request.
func (n *HttpNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	if n.IsDir {
		// Directories: no special handle needed, unless we want to stream index.json.
		// Just return nil and OK.
		return nil, 0, 0
	}

	// Create a file handle that wraps resp.Body
	fh, err := NewHttpFileHandle(n.URL, http.DefaultClient)
	if err != nil {
		return nil, 0, fs.ToErrno(err)
	}

	return fh, 0, 0
}

var _ fs.FileHandle = (*HttpFileHandle)(nil)
var _ fs.FileReader = (*HttpFileHandle)(nil)
var _ fs.FileReleaser = (*HttpFileHandle)(nil)

// HttpFileHandle represents a file handle for an HTTP-based file.
type HttpFileHandle struct {
	URL        string       // URL of the remote file
	HTTPClient *http.Client // HTTP client to make requests
	FileSize   int64        // Size of the remote file

	mu       sync.Mutex       // Mutex to protect concurrent access
	chunkBuf map[int64][]byte // Cache for chunks
}

// NewHttpFileHandle initializes a new HttpFileHandle.
// It performs a HEAD request to determine the file size.
func NewHttpFileHandle(url string, client *http.Client) (*HttpFileHandle, error) {
	if client == nil {
		client = http.DefaultClient
	}

	// Perform a HEAD request to get the file size
	req, err := http.NewRequest("HEAD", url, nil)
	if err != nil {
		return nil, fmt.Errorf("creating HEAD request: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("performing HEAD request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code %d on HEAD request", resp.StatusCode)
	}

	// Extract the Content-Length header to determine file size
	contentLength := resp.Header.Get("Content-Length")
	if contentLength == "" {
		return nil, errors.New("Content-Length header is missing")
	}

	var fileSize int64
	_, err = fmt.Sscanf(contentLength, "%d", &fileSize)
	if err != nil {
		return nil, fmt.Errorf("parsing Content-Length: %w", err)
	}

	return &HttpFileHandle{
		URL:        url,
		HTTPClient: client,
		FileSize:   fileSize,
		chunkBuf:   make(map[int64][]byte),
	}, nil
}

// Read fetches a specific range of bytes from the remote file.
// It uses chunk-based HTTP range requests to optimize memory usage and performance.
func (f *HttpFileHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	// Validate the offset
	if off < 0 || off >= f.FileSize {
		return nil, syscall.EINVAL
	}

	// Determine the number of bytes to read
	readLength := int64(len(dest))
	if off+readLength > f.FileSize {
		readLength = f.FileSize - off
	}

	// Calculate the starting and ending byte positions
	startByte := off
	// 	endByte := off + readLength - 1 // not used

	// Identify the chunk index based on the starting byte
	chunkIndex := startByte / chunkSize
	chunkStart := chunkIndex * chunkSize
	chunkEnd := chunkStart + chunkSize - 1
	if chunkEnd >= f.FileSize {
		chunkEnd = f.FileSize - 1
	}

	// Calculate the relative offset within the chunk
	relativeOffset := startByte - chunkStart

	// Determine the number of bytes to read from the chunk
	bytesToRead := readLength
	if relativeOffset+bytesToRead > chunkSize {
		bytesToRead = chunkSize - relativeOffset
		if chunkStart+chunkSize > f.FileSize {
			bytesToRead = f.FileSize - chunkStart - relativeOffset
		}
	}

	// Fetch the required chunk (with caching)
	chunkData, err := f.getChunk(chunkIndex)
	if err != nil {
		return nil, fs.ToErrno(err)
	}

	// Ensure we don't read beyond the chunk data
	if relativeOffset+bytesToRead > int64(len(chunkData)) {
		bytesToRead = int64(len(chunkData)) - relativeOffset
	}

	// Copy the relevant part of the chunk into dest
	copy(dest, chunkData[relativeOffset:relativeOffset+bytesToRead])

	// Return the number of bytes read
	return fuse.ReadResultData(dest[:bytesToRead]), fs.OK
}

// getChunk retrieves a specific chunk either from the cache or by fetching it via HTTP.
func (f *HttpFileHandle) getChunk(chunkIndex int64) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Check if the chunk is already cached
	if data, exists := f.chunkBuf[chunkIndex]; exists {
		return data, nil
	}

	// Calculate the byte range for the chunk
	startByte := chunkIndex * chunkSize
	endByte := startByte + chunkSize - 1
	if endByte >= f.FileSize {
		endByte = f.FileSize - 1
	}

	// Create a new HTTP request with the Range header
	req, err := http.NewRequest("GET", f.URL, nil)
	if err != nil {
		return nil, fmt.Errorf("creating GET request: %w", err)
	}
	rangeHeader := fmt.Sprintf("bytes=%d-%d", startByte, endByte)
	req.Header.Set("Range", rangeHeader)

	// Execute the HTTP request
	resp, err := f.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("performing GET request: %w", err)
	}
	defer resp.Body.Close()

	// Expecting a 206 Partial Content response
	if resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code %d for range request", resp.StatusCode)
	}

	// Read the response body
	chunkData, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response body: %w", err)
	}

	// Cache the chunk data
	f.chunkBuf[chunkIndex] = chunkData

	return chunkData, nil
}

func (f *HttpFileHandle) Release(ctx context.Context) syscall.Errno {
	// f.Body.Close()
	// will the garbage collector take care of this?
	return fs.OK
}
