package httpfs

// Metadata structure for storing file and directory metadata
type Metadata struct {
	Inode      uint64 `json:"inode"`
	Type       string `json:"type"`       // "file" or "dir"
	AccessBits uint32 `json:"accessBits"` // Permissions
	UID        uint32 `json:"uid"`
	GID        uint32 `json:"gid"`
	Size       int64  `json:"size"`
	Modified   int64  `json:"modified"`
	Created    int64  `json:"created"`
}
