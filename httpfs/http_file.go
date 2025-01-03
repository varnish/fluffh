package httpfs

import (
	"context"
	"fmt"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"io"
	"log/slog"
	"net/http"
	"syscall"
)

var _ fs.FileHandle = (*httpFileHandle)(nil)
var _ fs.FileReader = (*httpFileHandle)(nil)
var _ fs.FileReleaser = (*httpFileHandle)(nil)

// Read fetches a specific range of bytes from the remote file.
// It uses chunk-based HTTP range requests to optimize memory usage and performance.
// Todo: Rewrite this to use a shared LRUCache like github.com/hashicorp/golang-lru
// Todo: ReadResult contains a channel which can be closed to signal the end of the read operation. This allows us to keep reading in the background after the Read call returns.
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

// Release is called when the file handle is closed.
// We can release the cache
func (f *httpFileHandle) Release(ctx context.Context) syscall.Errno {
	slog.Debug("Release called", "url", f.URL)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.chunkBuf = nil
	return fs.OK
}

// readChunkIntoCache retrieves a specific chunk either from the cache or by fetching it via HTTP.
// The chunk is stored in the chunkBuf map. This code can be called concurrently.
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

// Cache Invalidation stuff:
// To invalidate an entry from the page cache we need a reference to the inode it belongs to.
// since the ingestion server can track the inode number, we can use that to invalidate the cache
/*
func (f *httpFileHandle) NotifyContentChange() {
	// Todo: figure out the path to the inode...
	if err := f.node.Inode.NotifyEntry("content"); err != nil {
		slog.Error("NotifyContentChange: failed to notify entry", "url", f.URL, "error", err)
	}

}
*/
