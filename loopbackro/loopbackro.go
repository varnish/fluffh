// Package loopbackro implements a read-only loopback file system.
// We'll use this as a base to implement a HTTP based file system with CDB-indices for directories.
package loopbackro

import (
	"context"
	"github.com/hanwen/go-fuse/v2/fs"
	"path/filepath"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fuse"
	"golang.org/x/sys/unix"
)

// LoopbackRoot holds the parameters for creating a new loopback
// filesystem. Loopback filesystems delegate their operations to an
// underlying POSIX file system.
type LoopbackRoot struct {
	// The path to the root of the underlying file system.
	Path string // Todo: This should be a base URL for httpfs.

	// The device on which the Path resides. This must be set if
	// the underlying filesystem crosses file systems.
	Dev uint64 // Todo: I think we can ignore this for httpfs.

	// NewNode returns a new InodeEmbedder to be used to respond
	// to a LOOKUP opcode. If not set, use a LoopbackNode.
	NewNode func(rootData *LoopbackRoot, parent *fs.Inode, name string, st *syscall.Stat_t) fs.InodeEmbedder
	// Todo: We should probably have a NewNode for httpfs. This would be a function that creates a new InodeEmbedder
	//       for a given URL. The URL would be the base URL of the httpfs and the name would be the path to the file.

	// RootNode is the root of the Loopback. This must be set if
	// the Loopback file system is not the root of the FUSE
	// mount. It is set automatically by NewLoopbackRoot.
	RootNode fs.InodeEmbedder // Todo: I think we can ignore this for httpfs.
}

func (root *LoopbackRoot) newNode(parentInode *fs.Inode, nodeName string, stat *syscall.Stat_t) fs.InodeEmbedder {
	// Check if a custom node creation function is provided.
	// If so, use it to create and return a new InodeEmbedder.
	if root.NewNode != nil {
		return root.NewNode(root, parentInode, nodeName, stat)
	}

	// If no custom node creation function is provided,
	// create and return a default LoopbackNode.
	return &LoopbackNode{
		RootData: root, // Reference back to the root of the loopback filesystem.
	}
}

// idFromStat generates a unique StableAttr from a syscall.Stat_t.
// We could perhaps change this and use the absolute URL through a hash as the inode id.
func (root *LoopbackRoot) idFromStat(stat *syscall.Stat_t) fs.StableAttr {
	// Swap the high and low 32 bits of the device number to create a unique identifier.
	// This helps in generating a unique inode number by combining device and inode information.
	deviceSwapped := (uint64(stat.Dev) << 32) | (uint64(stat.Dev) >> 32)

	// Similarly, swap the high and low 32 bits of the root device number.
	rootDeviceSwapped := (root.Dev << 32) | (root.Dev >> 32)

	// Generate a unique inode number by XOR-ing the swapped device numbers with the original inode number.
	// This ensures that inode numbers are unique across different devices.
	uniqueInode := (deviceSwapped ^ rootDeviceSwapped) ^ stat.Ino

	// Construct and return the StableAttr struct with the computed attributes.
	return fs.StableAttr{
		Mode: uint32(stat.Mode), // File mode (permissions and file type).
		Gen:  1,                 // Generation number (can be used for versioning; set to 1 as a default).
		Ino:  uniqueInode,       // Unique inode number.
	}
}

// LoopbackNode is a filesystem node in a loopback file system.
// A node is a file, directory, symbolic link, or other filesystem object.
type LoopbackNode struct {
	fs.Inode

	// RootData points back to the root of the loopback filesystem.
	RootData *LoopbackRoot
}

var _ = (fs.NodeStatfser)((*LoopbackNode)(nil))

// Statfs fills in the filesystem information for the underlying file system.
// Todo: We should probably just return zeros here.
func (n *LoopbackNode) Statfs(ctx context.Context, out *fuse.StatfsOut) syscall.Errno {
	s := syscall.Statfs_t{}
	err := syscall.Statfs(n.path(), &s)
	if err != nil {
		return fs.ToErrno(err)
	}
	out.FromStatfsT(&s)
	return fs.OK
}

// root returns the root of the loopback filesystem.
func (n *LoopbackNode) root() *fs.Inode {
	var rootNode *fs.Inode
	if n.RootData.RootNode != nil {
		rootNode = n.RootData.RootNode.EmbeddedInode()
	} else {
		rootNode = n.Root()
	}
	return rootNode
}

// path returns the full path of the current node in the underlying filesystem.
func (n *LoopbackNode) path() string {
	path := n.Path(n.root())
	return filepath.Join(n.RootData.Path, path)
}

var _ = (fs.NodeLookuper)((*LoopbackNode)(nil))

// Lookup looks up a child node with the given name.
func (n *LoopbackNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	p := filepath.Join(n.path(), name)

	st := syscall.Stat_t{}
	err := syscall.Lstat(p, &st)
	if err != nil {
		return nil, fs.ToErrno(err)
	}

	out.Attr.FromStat(&st)
	node := n.RootData.newNode(n.EmbeddedInode(), name, &st)
	ch := n.NewInode(ctx, node, n.RootData.idFromStat(&st))
	return ch, 0
}

var _ = (fs.NodeReadlinker)((*LoopbackNode)(nil))

func (n *LoopbackNode) Readlink(ctx context.Context) ([]byte, syscall.Errno) {
	// Obtain the full underlying filesystem path of the symlink.
	p := n.path()
	// Start with a reasonably small buffer and keep doubling its size
	// until the entire target of the symlink fits.
	for l := 256; ; l *= 2 {
		// Create a buffer of size l.
		buf := make([]byte, l)
		// Read the symlink target into buf.
		sz, err := syscall.Readlink(p, buf)
		if err != nil {
			// If there's an error, convert it to a fuse.Errno and return.
			return nil, fs.ToErrno(err)
		}
		// If sz is smaller than the buffer length, we got the whole target.
		// Resize the buffer to the actual size and return it.
		if sz < len(buf) {
			return buf[:sz], 0
		}
		// Otherwise, increase l and try again with a bigger buffer.
	}
}

var _ = (fs.NodeOpener)((*LoopbackNode)(nil))

// Open opens the file or directory represented by the current node.
// It returns a file handle that can be used to read the file or directory.
// Todo: For httpfs we should construct a http request here and perhaps ask for the first chunk.
func (n *LoopbackNode) Open(ctx context.Context, flags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	flags = flags &^ syscall.O_APPEND
	p := n.path()
	f, err := syscall.Open(p, int(flags), 0)
	if err != nil {
		return nil, 0, fs.ToErrno(err)
	}
	lf := fs.NewLoopbackFile(f)
	return lf, 0, 0
}

var _ = (fs.NodeOpendirHandler)((*LoopbackNode)(nil))

func (n *LoopbackNode) OpendirHandle(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	ds, errno := fs.NewLoopbackDirStream(n.path())
	if errno != 0 {
		return nil, 0, errno
	}
	return ds, 0, errno
}

var _ = (fs.NodeReaddirer)((*LoopbackNode)(nil))

func (n *LoopbackNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	return fs.NewLoopbackDirStream(n.path())
}

var _ = (fs.NodeGetattrer)((*LoopbackNode)(nil))

func (node *LoopbackNode) Getattr(ctx context.Context, fileHandle fs.FileHandle, attrOut *fuse.AttrOut) syscall.Errno {
	// If a file handle is provided and it implements the FileGetattrer interface,
	// delegate the getattr operation to the file handle's Getattr method.
	if fileHandle != nil {
		if fileGetAttrer, ok := fileHandle.(fs.FileGetattrer); ok {
			return fileGetAttrer.Getattr(ctx, attrOut)
		}
	}

	// Retrieve the full path of the current file or directory in the underlying filesystem.
	filePath := node.path()

	var (
		err      error
		statInfo syscall.Stat_t
	)

	// Determine whether the current node is the root of the filesystem.
	// - If it is the root, use syscall.Stat to follow symbolic links.
	// - Otherwise, use syscall.Lstat to avoid following symbolic links.
	if &node.Inode == node.Root() {
		err = syscall.Stat(filePath, &statInfo)
	} else {
		err = syscall.Lstat(filePath, &statInfo)
	}

	// If an error occurred while retrieving file attributes, convert it to a FUSE errno and return.
	if err != nil {
		return fs.ToErrno(err)
	}

	// Populate the AttrOut structure with the retrieved file attributes.
	attrOut.FromStat(&statInfo)

	// Indicate successful completion of the getattr operation.
	return fs.OK
}

var _ = (fs.NodeGetxattrer)((*LoopbackNode)(nil))

func (n *LoopbackNode) Getxattr(ctx context.Context, attr string, dest []byte) (uint32, syscall.Errno) {
	sz, err := unix.Lgetxattr(n.path(), attr, dest)
	return uint32(sz), fs.ToErrno(err)
}

// NewLoopbackRoot returns a root node for a loopback file system whose
// root is at the given root. This node implements read-only operations.
func NewLoopbackRoot(rootPath string) (fs.InodeEmbedder, error) {
	var st syscall.Stat_t
	err := syscall.Stat(rootPath, &st)
	if err != nil {
		return nil, err
	}

	root := &LoopbackRoot{
		Path: rootPath,
		Dev:  uint64(st.Dev),
	}

	rootNode := root.newNode(nil, "", &st)
	root.RootNode = rootNode
	return rootNode, nil
}
