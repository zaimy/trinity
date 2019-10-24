package storage

import (
	"context"
	cloudStorage "cloud.google.com/go/storage"
	"google.golang.org/api/iterator"

	"path/filepath"
)

func RemoveWorkflow(bucket string, src string, workflow string) error {
	ctx := context.Background()
	client, err := cloudStorage.NewClient(ctx)
	if err != nil {
		return err
	}

	workflowPath := filepath.Join(src, workflow)
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
