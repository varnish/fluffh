package httpfs

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/colinmarc/cdb"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"syscall"
)

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

// newHttpFileHandle initializes a new httpFileHandle.
// It performs a HEAD request to determine the file size.
func newHttpFileHandle(node *httpNode, client *http.Client) (*httpFileHandle, error) {
	slog.Debug("newHttpFileHandle: initializing", "url", node.URL)
	if client == nil {
		client = http.DefaultClient
	}

	// Perform a HEAD request to get the file size
	req, err := http.NewRequest("HEAD", node.URL, nil)
	if err != nil {
		slog.Error("newHttpFileHandle: creating HEAD request failed", "url", node.URL, "error", err)
		return nil, fmt.Errorf("creating HEAD request: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		slog.Error("newHttpFileHandle: HEAD request failed", "url", node.URL, "error", err)
		return nil, fmt.Errorf("performing HEAD request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		slog.Warn("newHttpFileHandle: non-200 HEAD response", "url", node.URL, "status_code", resp.StatusCode)
		return nil, fmt.Errorf("unexpected status code %d on HEAD request", resp.StatusCode)
	}

	// Extract the Content-Length header to determine file size
	contentLength := resp.Header.Get("Content-Length")
	if contentLength == "" {
		slog.Error("newHttpFileHandle: no Content-Length in HEAD response", "url", node.URL)
		return nil, errors.New("Content-Length header is missing")
	}

	var fileSize int64
	_, err = fmt.Sscanf(contentLength, "%d", &fileSize)
	if err != nil {
		slog.Error("newHttpFileHandle: parsing Content-Length failed", "url", node.URL, "error", err)
		return nil, fmt.Errorf("parsing Content-Length: %w", err)
	}

	slog.Debug("newHttpFileHandle: file handle initialized", "url", node.URL, "size", fileSize)
	return &httpFileHandle{
		URL:        node.URL,
		HTTPClient: client,
		FileSize:   fileSize,
		chunkBuf:   make(map[int64][]byte),
		node:       node, // We give the file handle a reference to the node it belongs to, so we can invalidate the cache if needed.
	}, nil
}
