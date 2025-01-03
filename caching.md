# ExplicitDataCacheControl

ExplicitDataCacheControl is a mount option that allows the file system to
control the caching of file data in the kernel. No idea how to use it.



# FUSE/Kernel caching

See https://docs.kernel.org/filesystems/fuse-io.html

From go-fuse source:
The kernel caches several pieces of information from the FUSE process:

1. File contents: enabled with the fuse.FOPEN_KEEP_CACHE return flag
in Open, manipulated with ReadCache and WriteCache, and invalidated
with Inode.NotifyContent

2. File Attributes (size, mtime, etc.): controlled with the
attribute timeout fields in fuse.AttrOut and fuse.EntryOut, which
get be populated from Getattr and Lookup

3. Directory entries (parent/child relations in the FS tree):
controlled with the timeout fields in fuse.EntryOut, and
invalidated with Inode.NotifyEntry and Inode.NotifyDelete.

Without Directory Entry timeouts, every operation on file "a/b/c"
must first do lookups for "a", "a/b" and "a/b/c", which is
expensive because of context switches between the kernel and the
FUSE process.

Unsuccessful entry lookups can also be cached by setting an entry
timeout when Lookup returns ENOENT.

The libfuse C library specifies 1 second timeouts for both
attribute and directory entries, but no timeout for negative
entries. by default. This can be achieve in go-fuse by setting
options on mount, eg.

	sec := time.Second
	opts := fs.Options{
	  EntryTimeout: &sec,
	  AttrTimeout: &sec,
	}
