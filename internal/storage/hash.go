package storage

import (
	"context"
	"io/ioutil"
	"path/filepath"

	cloudStorage "cloud.google.com/go/storage"
)

// GetHash gets a hash string of a dag from Cloud Storage.
func GetHash(bucket string, src string, workflow string) (string, error) {
	ctx := context.Background()
	client, err := cloudStorage.NewClient(ctx)
	if err != nil {
		return "", err
	}

	path := filepath.Join(src, workflow, ".trinity")
	rc, err := client.Bucket(bucket).Object(path).NewReader(ctx)
	if err != nil {
		return "", err
	}
	defer rc.Close()

	h, err := ioutil.ReadAll(rc)
	if err != nil {
		return "", err
	}
	return string(h), nil
}
