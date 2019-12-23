package storage

import (
	"context"
	"log"
	"regexp"
	"strings"

	cloudStorage "cloud.google.com/go/storage"
	mapset "github.com/deckarep/golang-set"
	"google.golang.org/api/iterator"
)

// ListWorkflows lists dags on Cloud Storage.
func ListWorkflows(bucketName string) (mapset.Set, error) {
	ctx := context.Background()
	client, err := cloudStorage.NewClient(ctx)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	workflows := mapset.NewSet()
	it := client.Bucket(bucketName).Objects(ctx, &cloudStorage.Query{Prefix: "dags/"})
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		trinityRep := regexp.MustCompile(".trinity$")
		if trinityRep.MatchString(attrs.Name) {
			pathRep := regexp.MustCompile(`\s*/\s*`)
			pathElement := pathRep.Split(attrs.Name, -1)
			workflow := strings.Replace(pathElement[1], ".trinity", "", 1)
			workflows.Add(workflow)
		}
	}

	return workflows, nil
}
