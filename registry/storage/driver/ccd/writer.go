package ccd

import (
	"bufio"
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"os"

	v1 "github.com/distribution/distribution/v3/registry/storage/driver/ccd/client/v1"
)

// writer is a storagedriver.FileWriter backed by Unity Cloud Content Delivery.
type writer struct {
	ctx context.Context

	// File
	file *os.File
	size int64
	bw   *bufio.Writer

	closed    bool
	committed bool
	cancelled bool

	// CCD
	client v1.ClientWithResponsesInterface
	bucket string
	path   string
}

// newWriter returns a new writer which will write content to file and then
// upload to CCD on commit.
func newWriter(ctx context.Context, file *os.File, client v1.ClientWithResponsesInterface, size int64, bucket, path string) *writer {
	return &writer{
		ctx:    ctx,
		file:   file,
		client: client,
		size:   size,
		bw:     bufio.NewWriter(file),
		bucket: bucket,
		path:   path,
	}
}

// Write implements io.WriteCloser
func (w *writer) Write(p []byte) (int, error) {
	if w.closed {
		return 0, fmt.Errorf("already closed")
	} else if w.committed {
		return 0, fmt.Errorf("already committed")
	} else if w.cancelled {
		return 0, fmt.Errorf("already cancelled")
	}
	n, err := w.bw.Write(p)
	w.size += int64(n)
	return n, err
}

// Size returns the number of bytes written to this FileWriter.
func (w *writer) Size() int64 {
	return w.size
}

// Close implements io.WriteCloser
func (w *writer) Close() error {
	if w.closed {
		return fmt.Errorf("already closed")
	}

	if err := w.bw.Flush(); err != nil {
		return err
	}

	if err := w.file.Sync(); err != nil {
		return err
	}

	if err := w.file.Close(); err != nil {
		return err
	}
	w.closed = true
	return nil
}

// Cancel removes any written content from this FileWriter.
func (w *writer) Cancel() error {
	if w.closed {
		return fmt.Errorf("already closed")
	}

	w.cancelled = true
	w.file.Close()

	if err := os.Remove(w.file.Name()); err != nil && !os.IsNotExist(err) {
		return err
	}

	return nil
}

// Commit flushes all content written to this FileWriter and makes it
// available for future calls to StorageDriver.GetContent and
// StorageDriver.Reader.
func (w *writer) Commit() error {
	if w.closed {
		return fmt.Errorf("already closed")
	} else if w.committed {
		return fmt.Errorf("already committed")
	} else if w.cancelled {
		return fmt.Errorf("already cancelled")
	}

	if err := w.bw.Flush(); err != nil {
		return fmt.Errorf("flush file: %w", err)
	}

	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("sync file: %w", err)
	}

	fi, err := w.file.Stat()
	if err != nil {
		return fmt.Errorf("stat file: %w", err)
	}

	if _, err = w.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("rewind for hash: %w", err)
	}

	h := md5.New()
	if _, err = io.Copy(h, w.file); err != nil {
		return fmt.Errorf("file hash: %w", err)
	}

	fileHash := hex.EncodeToString(h.Sum(nil))
	size := int(fi.Size())

	if _, err = w.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("rewind for upload: %w", err)
	}

	// CCD uploads are a two stage process.

	// First we create an "entry" to hold the content.
	entryID, err := createOrUpdateEntry(
		w.ctx, w.client, w.bucket, w.path, fileHash, size,
	)
	if err != nil {
		return fmt.Errorf("create/update entry: %w", err)
	}

	// Then we upload the content.
	if err = uploadContent(w.ctx, w.client, w.bucket, entryID, fileHash, w.file); err != nil {
		return fmt.Errorf("upload content: %w", err)
	}

	w.committed = true

	// Remove the temporary file.
	return os.Remove(w.file.Name())
}
