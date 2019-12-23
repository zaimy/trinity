package storage

import (
	"context"
	"fmt"

	cloudStorage "cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

// RemoveWorkflow removes a dag from Cloud Storage.
func RemoveWorkflow(bucket string, workflow string) error {
	removeBlobs(bucket, fmt.Sprintf("dags/%s/", workflow))
	removeBlobs(bucket, fmt.Sprintf("dags/%s.py", workflow))
	removeBlobs(bucket, fmt.Sprintf("dags/%s.trinity", workflow))

	return nil
}

func removeBlobs(bucket string, prefix string) error {
	ctx := context.Background()
	client, err := cloudStorage.NewClient(ctx)
	if err != nil {
		return err
	}

	it := client.Bucket(bucket).Objects(ctx, &cloudStorage.Query{Prefix: prefix})
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
