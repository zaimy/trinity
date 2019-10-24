package storage

import (
	cloudStorage "cloud.google.com/go/storage"
	"context"
	"path/filepath"
	"io/ioutil"
)

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
