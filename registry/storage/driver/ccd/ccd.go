// Package ccd provides a storagedriver.StorageDriver implementation to
// store blobs in Unity Cloud Content Delivery.
//
//go:build include_ccd
// +build include_ccd

package ccd

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"

	dcontext "github.com/distribution/distribution/v3/context"
	storagedriver "github.com/distribution/distribution/v3/registry/storage/driver"
	"github.com/distribution/distribution/v3/registry/storage/driver/base"
	v1 "github.com/distribution/distribution/v3/registry/storage/driver/ccd/client/v1"
	"github.com/distribution/distribution/v3/registry/storage/driver/ccd/utils"
	"github.com/distribution/distribution/v3/registry/storage/driver/factory"
)

const (
	driverName             = "ccd"
	contentTypeOctetStream = "application/offset+octet-stream"
)

var baseURLs = map[string]string{
	"prod":  "https://content-api.cloud.unity3d.com",
	"stage": "https://content-api-stg.cloud.unity3d.com",
}

func init() {
	factory.Register(driverName, &ccdDriverFactory{})
}

// ccdDriverFactory implements the factory.StorageDriverFactory interface.
type ccdDriverFactory struct{}

func (factory *ccdDriverFactory) Create(parameters map[string]interface{}) (storagedriver.StorageDriver, error) {
	return New(parameters)
}

type driver struct {
	client  v1.ClientWithResponsesInterface
	baseURL string
	apiKey  string
}

// baseEmbed allows us to hide the Base embed.
type baseEmbed struct {
	base.Base
}

// Driver is a storagedriver.StorageDriver implementation backed by Unity Cloud
// Content Delivery.
type Driver struct {
	baseEmbed // embedded, hidden base driver.
}

var _ storagedriver.StorageDriver = &Driver{}

// New constructs a new Driver.
func New(parameters map[string]interface{}) (*Driver, error) {
	// Parameter validation.
	apiKey, ok := parameters["apikey"]
	if !ok || fmt.Sprint(apiKey) == "" {
		return nil, errors.New("no api key parameter provided")
	}
	env, ok := parameters["environment"]
	if !ok || fmt.Sprint(env) == "" {
		return nil, errors.New("no environment parameter provided")
	}

	baseURL, ok := baseURLs[fmt.Sprint(env)]
	if !ok {
		return nil, errors.New("invalid environment parameter provided")
	}

	// CCD client.
	client, err := v1.NewClientWithResponses(
		baseURL,
		v1.WithRequestEditorFn(func(ctx context.Context, req *http.Request) error {
			req.SetBasicAuth("", fmt.Sprint(apiKey))
			return nil
		}),
	)
	if err != nil {
		return nil, err
	}

	return &Driver{
		baseEmbed: baseEmbed{
			Base: base.Base{
				StorageDriver: &driver{
					client:  client,
					baseURL: baseURL,
					apiKey:  fmt.Sprint(apiKey),
				},
			},
		},
	}, nil
}

// Name returns the human-readable "name" of the driver, useful in error
// messages and logging. By convention, this will just be the registration
// name, but drivers may provide other information here.
func (d *driver) Name() string {
	return driverName
}

// GetContent retrieves the content stored at "path" as a []byte.
// This should primarily be used for small objects.
func (d *driver) GetContent(ctx context.Context, path string) ([]byte, error) {
	bucketID, err := bucketIDFromContext(ctx)
	if err != nil {
		return nil, err
	}

	// CCD content is referenced by entry ID.
	entry, err := getEntryByPath(ctx, d.client, bucketID, path)
	if err != nil {
		return nil, err
	}

	return getEntryContent(ctx, d.client, bucketID, *entry.Entryid)
}

// PutContent stores the []byte content at a location designated by "path".
// This should primarily be used for small objects.
func (d *driver) PutContent(ctx context.Context, path string, content []byte) error {
	bucketID, err := bucketIDFromContext(ctx)
	if err != nil {
		return err
	}

	// CCD needs the hash of the content before we upload it.
	h := md5.New()
	h.Write(content)
	fileHash := hex.EncodeToString(h.Sum(nil))

	// CCD uploads are a two stage process.

	// First we create an "entry" to hold the content.
	entryID, err := createOrUpdateEntry(ctx, d.client, bucketID, path, fileHash, len(content))
	if err != nil {
		return err
	}

	// Then we upload the content.
	return uploadContent(
		ctx, d.client, bucketID, entryID, fileHash, bytes.NewReader(content),
	)
}

// Reader retrieves an io.ReadCloser for the content stored at "path"
// with a given byte offset.
// May be used to resume reading a stream by providing a nonzero offset.
func (d *driver) Reader(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {
	bucketID, err := bucketIDFromContext(ctx)
	if err != nil {
		return nil, err
	}

	// CCD content is referenced by entry ID.
	entry, err := getEntryByPath(ctx, d.client, bucketID, path)
	if err != nil {
		return nil, err
	}

	// TODO(dr): This should ideally be using the CCD client API in order to
	// pull the content from the CDN. This does rely on bucket access tokens
	// though, which would need to be created/retrieved on a bucket-by-bucket
	// basis.
	req, err := v1.NewGetContentRequest(d.baseURL, bucketID, *entry.Entryid, &v1.GetContentParams{})
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth("", d.apiKey)

	if offset > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%v-", offset))
	}

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	return res.Body, nil
}

// Writer returns a FileWriter which will store the content written to it
// at the location designated by "path" after the call to Commit.
func (d *driver) Writer(ctx context.Context, dataPath string, append bool) (storagedriver.FileWriter, error) {
	bucketID, err := bucketIDFromContext(ctx)
	if err != nil {
		return nil, err
	}

	rootDir := filepath.Join(os.TempDir(), "ccd")

	// CCD requires the hash and length of an entry before it can be uploaded.
	// Meaning we cannot stream bytes, in chunks, to a CCD entry unless we know
	// the full content up front.
	//
	// TODO(dr): Revisit this when CCD implements streaming upload support.
	if err = os.MkdirAll(filepath.Join(rootDir, path.Dir(dataPath)), 0777); err != nil {
		return nil, err
	}

	f, err := os.OpenFile(filepath.Join(rootDir, dataPath), os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}

	// Append to the end of an existing file, or just truncate.
	var offset int64
	if append {
		n, err := f.Seek(0, io.SeekEnd)
		if err != nil {
			f.Close()
			return nil, err
		}
		offset = n
	} else {
		if err = f.Truncate(0); err != nil {
			f.Close()
			return nil, err
		}
	}

	return newWriter(ctx, f, d.client, offset, bucketID, dataPath), nil
}

// Stat retrieves the FileInfo for the given path, including the current
// size in bytes and the creation time.
func (d *driver) Stat(ctx context.Context, path string) (storagedriver.FileInfo, error) {
	bucketID, err := bucketIDFromContext(ctx)
	if err != nil {
		return nil, err
	}

	// CCD content is referenced by entry ID.
	entry, err := getEntryByPath(ctx, d.client, bucketID, path)
	if err != nil {
		return nil, err
	}
	size := *entry.ContentSize

	fi := storagedriver.FileInfoFields{
		Path:    path,
		Size:    int64(size),
		ModTime: *entry.LastModified,
		IsDir:   false,
	}
	return storagedriver.FileInfoInternal{FileInfoFields: fi}, nil
}

// List returns a list of the objects that are direct descendants of the
// given path.
func (d *driver) List(ctx context.Context, listPath string) ([]string, error) {
	bucketID, err := bucketIDFromContext(ctx)
	if err != nil {
		return nil, err
	}

	entries, err := getEntries(ctx, d.client, bucketID, listPath)
	if err != nil {
		return nil, err
	}

	paths := make([]string, 0, len(entries))
	for _, e := range entries {
		// CCD paths have the leading slash missing.
		paths = append(paths, filepath.Join("/", *e.Path))
	}

	return paths, nil
}

// Move moves an object stored at sourcePath to destPath, removing the
// original object.
func (d *driver) Move(ctx context.Context, sourcePath string, destPath string) error {
	// CCD has no copy or move feature. So all we can do is download, upload and
	// delete.
	content, err := d.GetContent(ctx, sourcePath)
	if err != nil {
		return err
	}

	if err = d.PutContent(ctx, destPath, content); err != nil {
		return err
	}

	return d.Delete(ctx, sourcePath)
}

// Delete recursively deletes all objects stored at "path" and its subpaths.
func (d *driver) Delete(ctx context.Context, path string) error {
	bucketID, err := bucketIDFromContext(ctx)
	if err != nil {
		return err
	}

	entries, err := getEntries(ctx, d.client, bucketID, path)
	if err != nil {
		return nil
	}

	for _, e := range entries {
		res, err := d.client.DeleteEntryWithResponse(ctx, bucketID, *e.Entryid)
		if err != nil {
			return err
		} else if res.JSON500 != nil {
			return fmt.Errorf("unexpected error: %q", *res.JSON500.Reason)
		} else if sc := res.StatusCode(); sc != http.StatusNoContent {
			return fmt.Errorf("unexpected response: %q", sc)
		}
	}

	return nil
}

// URLFor returns a URL which may be used to retrieve the content stored at
// the given path, possibly using the given options.
// May return an ErrUnsupportedMethod in certain StorageDriver
// implementations.
func (d *driver) URLFor(ctx context.Context, path string, options map[string]interface{}) (string, error) {
	// Downloading from CCD should be done via a signed URL to Akamai, the CDN
	// that powers CCD.
	//
	// In order to get the signed URL you need to call the URL indicated by the
	// ContentLink on a CCD entry. However, calling that URL requires an access
	// token for the bucket. The Docker client will not have this access token.
	//
	// Ergo we mark this as unsupported and force the caller to fall back to the
	// GetContent and Reader methods instead.
	return "", storagedriver.ErrUnsupportedMethod{}
}

// Walk traverses a filesystem defined within driver, starting
// from the given path, calling f on each file.
// If the returned error from the WalkFn is ErrSkipDir and fileInfo refers
// to a directory, the directory will not be entered and Walk
// will continue the traversal.  If fileInfo refers to a normal file, processing stops
func (d *driver) Walk(ctx context.Context, path string, f storagedriver.WalkFn) error {
	panic(fmt.Sprintf("Walk: path: %q", path))
}

// createOrUpdateEntry returns the CCD entry ID for the given path in the bucket.
// An entry will be created at that path if it does not exist, or updated if
// it does.
func createOrUpdateEntry(ctx context.Context, client v1.ClientWithResponsesInterface, bucket, path, hash string, contentLength int) (string, error) {
	res, err := client.CreateOrUpdateEntryByPathWithResponse(
		ctx,
		bucket,
		&v1.CreateOrUpdateEntryByPathParams{
			Path:           path,
			UpdateIfExists: utils.BoolPtr(true),
		},
		v1.CreateOrUpdateEntryByPathJSONRequestBody{
			ContentSize: contentLength,
			ContentHash: hash,
			ContentType: utils.StringPtr(contentTypeOctetStream),
		},
	)
	if err != nil {
		return "", err
	} else if res.JSON200 == nil {
		return "", errors.New("empty response from CCD")
	} else if res.JSON200.Entryid == nil {
		return "", errors.New("failed to determine entry ID")
	}

	return *res.JSON200.Entryid, nil
}

// uploadContent uploads to the given content to the CCD entry ID.
func uploadContent(ctx context.Context, client v1.ClientWithResponsesInterface, bucket, entryID, hash string, content io.Reader) error {
	res, err := client.UploadContentWithBodyWithResponse(
		ctx,
		bucket,
		entryID,
		&v1.UploadContentParams{},
		contentTypeOctetStream,
		content,
	)
	if err != nil {
		return err
	} else if uh := res.HTTPResponse.Header.Get("Upload-Hash"); uh != hash {
		return fmt.Errorf("upload hash mismatch: expected %q got: %q", hash, uh)
	}

	return nil
}

// getEntryByPath returns the CCD entry ID for the given path in the bucket.
func getEntryByPath(ctx context.Context, client v1.ClientWithResponsesInterface, bucket, path string) (*v1.Entry, error) {
	res, err := client.GetEntryByPathWithResponse(ctx, bucket, &v1.GetEntryByPathParams{
		Path: path,
	})
	if err != nil {
		return nil, err
	} else if res.JSON404 != nil {
		dcontext.GetLoggerWithFields(ctx, map[interface{}]interface{}{
			"bucket": bucket,
			"path":   path,
		}).Warn("no entry found")

		return nil, storagedriver.PathNotFoundError{Path: path}
	} else if res.JSON500 != nil {
		return nil, fmt.Errorf("unexpected error: %q", *res.JSON500.Reason)
	} else if res.JSON200 == nil {
		return nil, fmt.Errorf(
			"failed to determine entry for bucket: %q path: %q",
			bucket,
			path,
		)
	}

	return res.JSON200, nil
}

// getEntryContent returns the bytes stored in the bucket entry.
//
// TODO(dr): This should ideally be using the CCD client API in order to pull
// the content from the CDN. This does rely on bucket access tokens though,
// which would need to be created/retrieved on a bucket-by-bucket basis.
func getEntryContent(ctx context.Context, client v1.ClientWithResponsesInterface, bucket, entryID string) ([]byte, error) {
	res, err := client.GetContentWithResponse(ctx, bucket, entryID, &v1.GetContentParams{})
	if err != nil {
		return nil, err
	}

	return res.Body, nil
}

func getEntries(ctx context.Context, client v1.ClientWithResponsesInterface, bucketID, listPath string) ([]v1.Releaseentry, error) {
	var (
		entries []v1.Releaseentry
		page    = 1
	)

	// CCD path search omits the leading forward slash.
	listPath = strings.TrimPrefix(listPath, "/")

	// CCD entries are returned from a paginated API.
	for {
		// TODO(dr): This is relying on the bucket not having a release. Might
		// not be the right thing to do.
		res, err := client.GetDiffEntriesWithResponse(ctx, bucketID, &v1.GetDiffEntriesParams{
			Path:    utils.StringPtr(listPath),
			Page:    utils.IntPtr(page),
			PerPage: utils.IntPtr(1),
		})
		if err != nil {
			return nil, err
		} else if res.JSON404 != nil {
			return nil, storagedriver.PathNotFoundError{Path: listPath}
		} else if res.JSON500 != nil {
			return nil, fmt.Errorf("unexpected error: %q", *res.JSON500.Reason)
		} else if res.JSON200 == nil || len(*res.JSON200) == 0 {
			// No more items.
			break
		}

		for _, e := range *res.JSON200 {
			entries = append(entries, e)
		}

		page++
	}

	return entries, nil
}

func bucketIDFromContext(ctx context.Context) (string, error) {
	name := dcontext.GetStringValue(ctx, "vars.name")
	if name == "" {
		return "", errors.New("could not determine bucket ID from context")
	}

	return name, nil
}
