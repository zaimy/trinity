package storage

import (
	"context"
	"log"
	"path/filepath"
	"os"
	"fmt"
	"io"
	cloudStorage "cloud.google.com/go/storage"
)

func UploadWorkflow(bucket string, src string, workflow string) error {
	ctx := context.Background()
	client, err := cloudStorage.NewClient(ctx)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	workflowPath := filepath.Join(src, workflow)
	err = filepath.Walk(workflowPath, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}

		fmt.Fprintln(os.Stdout, path)
		wc := client.Bucket(bucket).Object(path).NewWriter(ctx)
		f, err := os.Open(path)
		if err != nil {
						return err
		}
		defer f.Close()

		if _, err = io.Copy(wc, f); err != nil {
						return err
		}
		if err := wc.Close(); err != nil {
						return err
		}

		return nil
	})

	if err != nil {
		log.Fatal(err)
	}

	return nil
}
