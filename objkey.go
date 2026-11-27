package main

// Object keys are user input from the S3 path, decoded (%2F → '/') and,
// because the endpoint has no ServeMux, never cleaned of '.' / '..'
// segments. They must never be used to build a filesystem path: staged
// and restored copies are named by tapeberg (obj-*, restore-<id>), not
// by key. checkObjectKey is the second line of defense — it refuses
// keys that would be dangerous anywhere a path is built from them, and
// runs before any filesystem or catalog work.

import (
	"net/http"
	"strings"
)

const maxObjectKeyBytes = 1024 // the S3 limit

func checkObjectKey(key string) error {
	if len(key) > maxObjectKeyBytes {
		return errS3("KeyTooLongError", "object key longer than 1024 bytes", http.StatusBadRequest)
	}
	if strings.ContainsRune(key, 0) {
		return errS3("InvalidArgument", "object key contains a NUL byte", http.StatusBadRequest)
	}
	if strings.HasPrefix(key, "/") {
		return errS3("InvalidArgument", "object key must not start with '/'", http.StatusBadRequest)
	}
	for _, seg := range strings.Split(key, "/") {
		if seg == "." || seg == ".." {
			return errS3("InvalidArgument", "object key must not contain '.' or '..' path segments", http.StatusBadRequest)
		}
	}
	return nil
}
