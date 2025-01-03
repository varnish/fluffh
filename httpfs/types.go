package httpfs

import (
	"github.com/hanwen/go-fuse/v2/fs"
	"net/http"
	"sync"
)

//go:generate go run github.com/tinylib/msgp -io false -marshal true

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

	// times:
	Atime uint64 `json:"atime" msg:"atime"`
	Mtime uint64 `json:"mtime" msg:"mtime"`
	Ctime uint64 `json:"ctime" msg:"ctime"`
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

// Ensure we satisfy the interface we wanna implement:
var _ fs.NodeStatfser = (*httpNode)(nil)  // statfs, to get stats about the filesystem
var _ fs.NodeLookuper = (*httpNode)(nil)  // Lookup a child node by name
var _ fs.NodeGetattrer = (*httpNode)(nil) // Get attributes of the node
var _ fs.NodeReaddirer = (*httpNode)(nil) // Directory listing
var _ fs.NodeOpener = (*httpNode)(nil)    // Open a file

// httpFileHandle represents a file handle for an HTTP-based file.
type httpFileHandle struct {
	URL        string
	HTTPClient *http.Client
	FileSize   int64

	mu       sync.Mutex
	chunkBuf map[int64][]byte
	node     *httpNode // the node this handle is associated with
}

// Ensure we satisfy the interface we wanna implement:
var _ fs.FileReader = (*httpFileHandle)(nil)   // Read from the file
var _ fs.FileReleaser = (*httpFileHandle)(nil) // Get attributes of the file
