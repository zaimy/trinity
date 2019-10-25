package storage

import (
	"context"
	"fmt"
	"log"
	"regexp"

	cloudStorage "cloud.google.com/go/storage"
	mapset "github.com/deckarep/golang-set"
	"google.golang.org/api/iterator"
)

// ListWorkflows lists dags on Cloud Storage.
func ListWorkflows(bucketName string, src string) (mapset.Set, error) {
	ctx := context.Background()
	client, err := cloudStorage.NewClient(ctx)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	workflowNames := mapset.NewSet()
	it := client.Bucket(bucketName).Objects(ctx, &cloudStorage.Query{Prefix: fmt.Sprintf("%s/", src)})
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		r := regexp.MustCompile(".trinity$")
		if r.MatchString(attrs.Name) {
			rep := regexp.MustCompile(`\s*/\s*`)
			result := rep.Split(attrs.Name, -1)
			workflowNames.Add(result[1])
			// fmt.Fprintln(os.Stdout, result[1])
		}
	}

	return workflowNames, nil
}
