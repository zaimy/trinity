package storage

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	cloudStorage "cloud.google.com/go/storage"
)

// UploadWorkflow uploads a dag to Cloud Storage.
func UploadWorkflow(bucket string, src string, workflow string) error {
	ctx := context.Background()
	client, err := cloudStorage.NewClient(ctx)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	definitionWorkflowPath := filepath.Join(src, workflow)
	err = filepath.Walk(definitionWorkflowPath, func(definitionPath string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}

		fmt.Fprintln(os.Stdout, definitionPath)
		storagePath := strings.Replace(definitionPath, src, "dags", 1)
		wc := client.Bucket(bucket).Object(storagePath).NewWriter(ctx)
		f, err := os.Open(definitionPath)
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
