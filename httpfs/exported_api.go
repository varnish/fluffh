package httpfs

import (
	"context"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"log/slog"
	"net/http"
	"strings"
	"syscall"
)

const (
	IndexFile       = "index.cdb"
	chunkSize int64 = 1 << 20 // 1MB
)

// NewHttpRoot initializes the HTTP root node.
func NewHttpRoot(baseURL string) (fs.InodeEmbedder, error) {
	slog.Info("NewHttpRoot: initializing HTTP root", "baseURL", baseURL)
	if !strings.HasSuffix(baseURL, "/") {
		baseURL += "/"
	}

	root := &httpRoot{
		BaseURL: baseURL,
		NewNode: func(r *httpRoot, parent *fs.Inode, name, url string, meta *FileMeta) fs.InodeEmbedder {
			slog.Debug("NewNode(from NewHttpRoot): creating new node", "name", name, "url", url, "meta", meta)
			return &httpNode{
				RootData: r,
				URL:      url,
				IsDir:    meta.Mode&syscall.S_IFDIR != 0,
				Meta:     meta,
			}
		},
	}

	// Fetch root dir metadata to ensure it's a directory. This is a bit of an overkill, but it only happens on mount.
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
		INO:  2, // Root inode is almost always 2
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
// Todo: map stuff from HTTP headers to the attributes or perhaps store more metadata into the CDB files.
func (n *httpNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	slog.Debug("Getattr called", "url", n.URL, "isDir", n.IsDir, "meta", n.Meta)
	out.Mode = n.Meta.Mode
	out.Size = n.Meta.Size
	out.Uid = n.Meta.UID
	out.Gid = n.Meta.GID
	out.Ino = n.Meta.INO
	// Set some sensible times
	// Todo: embed this in the CDB metadata.
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

	childNode := n.RootData.NewNode(n.RootData, n.EmbeddedInode(), name, entryURL, meta)
	ch := n.NewInode(ctx, childNode, fs.StableAttr{
		Mode: meta.Mode,
		Ino:  meta.INO, // we set the inode number from the CDB here.
	})

	out.Attr.Mode = meta.Mode
	out.Attr.Size = meta.Size
	out.Attr.Uid = meta.UID
	out.Attr.Gid = meta.GID
	out.Attr.Ino = meta.INO
	slog.Debug("Lookup: child node created successfully", "url", entryURL)
	return ch, 0
}

// Opendir opens a directory. We could start to fetch the directory listing here if we wanna optimize.
func (n *httpNode) Opendir(ctx context.Context) syscall.Errno {
	slog.Debug("Opendir called", "url", n.URL, "isDir", n.IsDir, "inode", n.Meta.INO)
	if !n.IsDir {
		slog.Warn("Opendir attempted on a file node", "url", n.URL)
		return syscall.ENOTDIR
	}
	return 0
}

// Readdir reads the directory contents.
// Todo: consider caching the directory listing. This would likely happen by making an DirStream implementation.
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
	idx := uint64(0)
	for name, meta := range listing {
		entries = append(entries, fuse.DirEntry{
			Name: name,
			Ino:  meta.INO,
			Mode: meta.Mode,
			Off:  idx,
		})
		idx++
	}
	slog.Debug("Readdir: returning directory entries", "url", n.URL, "count", len(entries))
	return fs.NewListDirStream(entries), fs.OK
}

// Open opens a file. Directories don't go through this method.
// For files, it will initiate an HTTP GET request.
// Todo: sanity check the flags, we will only support a read only workload.
func (n *httpNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	slog.Debug("Open called", "url", n.URL, "isDir", n.IsDir)
	if n.IsDir {
		slog.Warn("Open: directory node opened, returning nil file handle", "url", n.URL)
		return nil, 0, 0
	}

	// Create a file handle that wraps resp.Body
	fileHandle, err := newHttpFileHandle(n, http.DefaultClient)
	if err != nil {
		slog.Error("Open: failed to open file", "url", n.URL, "error", err)
		return nil, 0, fs.ToErrno(err)
	}

	slog.Debug("Open: file handle created successfully", "url", n.URL)
	var fl uint32
	// fl = fuse.FOPEN_KEEP_CACHE // this would enable caching. consider invalidation if enabled.
	return fileHandle, fl, 0
}
