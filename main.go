package main

import (
	"fmt"
	"flag"
	"github.com/zaimy/trinity/internal/airflow"
	"github.com/zaimy/trinity/internal/definition"
	"github.com/zaimy/trinity/internal/storage"
	"log"
)

func main() {
	var src = flag.String("src", "dags", "dags directory")
	var bucket = flag.String("bucket", "", "Cloud Storage bucket name")
	var composerEnv = flag.String("composer-env", "", "Cloud Composer environment name")
	var composerLocation = flag.String("composer-location", "us-central1", "Cloud Composer environment location")
	flag.Parse()

	if err := definition.SaveHash(*src); err != nil {
		log.Fatal(err)
	}

	cloudStorageWorkflows, err := storage.ListWorkflows(*bucket, *src)
	if err != nil {
		log.Fatal(err)
	}
	localStorageWorkflows, err := definition.ListWorkflows(*src)
	if err != nil {
		log.Fatal(err)
	}

	// Exists only definition.
	d := localStorageWorkflows.Difference(cloudStorageWorkflows)
	it := d.Iterator()
	for w := range it.C {
		if err := storage.UploadWorkflow(*bucket, *src, fmt.Sprintf("%v", w)); err != nil {
			log.Fatal(err)
		}
	}

	// Exists only storage.
	d = cloudStorageWorkflows.Difference(localStorageWorkflows)
	it = d.Iterator()
	for w := range it.C {
		// Remove from storage
		if err := storage.RemoveWorkflow(*bucket, *src, fmt.Sprintf("%v", w)); err != nil {
			log.Fatal(err)
		}

		// Remove from airflow
		if err = airflow.RemoveWorkflow(*composerEnv, *composerLocation, fmt.Sprintf("%v", w)); err != nil {
			log.Fatal(err)
		}
	}

	// Exists in both.
	d = cloudStorageWorkflows.Intersect(localStorageWorkflows)
	it = d.Iterator()
	for w := range it.C {
		// Compare hash
		definitionHash, err := definition.GetHash(*src, fmt.Sprintf("%v", w))
		if err != nil {
			log.Fatal(err)
		}

		storageHash, err := storage.GetHash(*bucket, *src, fmt.Sprintf("%v", w))
		if err != nil {
			log.Fatal(err)
		}

		if definitionHash == storageHash {
			// Do nothing.
		} else {
			// Remove from storage
			if err := storage.RemoveWorkflow(*bucket, *src, fmt.Sprintf("%v", w)); err != nil {
				log.Fatal(err)
			}

			// Upload to storage
			if err = storage.UploadWorkflow(*bucket, *src, fmt.Sprintf("%v", w)); err != nil {
				log.Fatal(err)
			}
		}
	}
}
