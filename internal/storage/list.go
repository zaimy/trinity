package storage

import (
	"context"
	"fmt"
	"log"
	"regexp"
	"strings"

	cloudStorage "cloud.google.com/go/storage"
	mapset "github.com/deckarep/golang-set"
	"google.golang.org/api/iterator"
)

// ListWorkflows lists dags on Cloud Storage.
func ListWorkflows(bucket string, dagDirectory string) (mapset.Set, error) {
	ctx := context.Background()
	client, err := cloudStorage.NewClient(ctx)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	workflows := mapset.NewSet()
	dagDirectoryDepth := len(strings.Split(dagDirectory, "/"))
	it := client.Bucket(bucket).Objects(ctx, &cloudStorage.Query{Prefix: fmt.Sprintf("%s/", dagDirectory)})
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
			pathElement := strings.Split(attrs.Name, "/")
			workflow := strings.Replace(pathElement[dagDirectoryDepth], ".trinity", "", 1)
			workflows.Add(workflow)
		}
	}

	return workflows, nil
}
