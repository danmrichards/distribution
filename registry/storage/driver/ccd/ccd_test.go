package ccd

import (
	"errors"
	"testing"
)

func TestBucketPath(t *testing.T) {
	tests := []struct {
		name      string
		path      string
		expBucket string
		expError  error
	}{
		{
			name:      "whole path",
			path:      "/docker/registry/v2/repositories/foo/_uploads/bar/baz",
			expBucket: "foo",
		},
		{
			name:      "trailing slash",
			path:      "/docker/registry/v2/repositories/foo/",
			expBucket: "foo",
		},
		{
			name:      "partial path",
			path:      "/docker/registry/v2/repositories/foo",
			expBucket: "foo",
		},
		{
			name:     "missing bucket",
			path:     "/docker/registry/v2/repositories/",
			expError: errors.New(`could not parse bucket from path: "/docker/registry/v2/repositories/"`),
		},
		{
			name:     "empty path",
			path:     "",
			expError: errors.New(`could not parse bucket from path: ""`),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bucket, err := bucketFromPath(tt.path)
			switch {
			case tt.expError != nil:
				if tt.expError.Error() != err.Error() {
					t.Fatalf("expected error: %v got: %v", tt.expError, err)
				}
				return
			case err != nil:
				t.Fatalf("uexpected error: %v", err)
				return
			case tt.expBucket != bucket:
				t.Fatalf("expected bucket: %v got: %v", tt.expBucket, bucket)
				return
			}
		})
	}
}
