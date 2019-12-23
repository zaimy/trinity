package storage

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	cloudStorage "cloud.google.com/go/storage"
)

// UploadWorkflow uploads a dag to Cloud Storage.
func UploadWorkflow(bucket string, dagDirectory string, src string, workflow string) error {
	ctx := context.Background()
	client, err := cloudStorage.NewClient(ctx)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	err = filepath.Walk(src, func(definitionPath string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}

		dirRep := fmt.Sprintf("^%s/%s/", src, workflow)
		fileRep := fmt.Sprintf("^%s/%s\\.(py|trinity)$", src, workflow)
		rep := regexp.MustCompile(fmt.Sprintf("%s|%s", dirRep, fileRep))
		if !rep.MatchString(definitionPath) {
			return nil
		}

		storagePath := strings.Replace(definitionPath, src, dagDirectory, 1)
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

		log.Printf("%s uploaded to %s", definitionPath, storagePath)

		return nil
	})

	if err != nil {
		log.Fatal(err)
	}

	return nil
}
