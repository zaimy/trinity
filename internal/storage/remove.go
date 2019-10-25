package storage

import (
	"context"
	"path/filepath"

	cloudStorage "cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

// RemoveWorkflow removes a dag from Cloud Storage.
func RemoveWorkflow(bucket string, workflow string) error {
	ctx := context.Background()
	client, err := cloudStorage.NewClient(ctx)
	if err != nil {
		return err
	}

	workflowPath := filepath.Join("dags", workflow)
	it := client.Bucket(bucket).Objects(ctx, &cloudStorage.Query{Prefix: workflowPath})
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}

		o := client.Bucket(bucket).Object(attrs.Name)
		if err := o.Delete(ctx); err != nil {
			return err
		}
	}

	return nil
}
