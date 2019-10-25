package trinity

import (
	"flag"
	"fmt"
	"io"
	"log"

	"github.com/zaimy/trinity/internal/airflow"
	"github.com/zaimy/trinity/internal/definition"
	"github.com/zaimy/trinity/internal/storage"
)

// Run runs the trinity.
func Run(args []string, outStream, errStream io.Writer) error {
	fs := flag.NewFlagSet("trinity", flag.ContinueOnError)
	var (
		src              string
		bucket           string
		composerEnv      string
		composerLocation string
	)
	fs.StringVar(&src, "src", "dags", "dags directory")
	fs.StringVar(&bucket, "bucket", "", "Cloud Storage bucket name")
	fs.StringVar(&composerEnv, "composer-env", "", "Cloud Composer environment name")
	fs.StringVar(&composerLocation, "composer-location", "us-central1", "Cloud Composer environment location")
	fs.SetOutput(errStream)
	if err := fs.Parse(args); err != nil {
		return err
	}

	if err := definition.SaveHash(src); err != nil {
		log.Fatal(err)
	}

	cloudStorageWorkflows, err := storage.ListWorkflows(bucket)
	if err != nil {
		log.Fatal(err)
	}
	localStorageWorkflows, err := definition.ListWorkflows(src)
	if err != nil {
		log.Fatal(err)
	}

	// Exists only definition
	d := localStorageWorkflows.Difference(cloudStorageWorkflows)
	it := d.Iterator()
	for w := range it.C {
		if err := storage.UploadWorkflow(bucket, src, fmt.Sprintf("%v", w)); err != nil {
			log.Fatal(err)
		}
	}

	// Exists only storage
	d = cloudStorageWorkflows.Difference(localStorageWorkflows)
	it = d.Iterator()
	for w := range it.C {
		// Remove from storage
		if err := storage.RemoveWorkflow(bucket, fmt.Sprintf("%v", w)); err != nil {
			log.Fatal(err)
		}

		// Remove from airflow
		if err = airflow.RemoveWorkflow(composerEnv, composerLocation, fmt.Sprintf("%v", w)); err != nil {
			log.Fatal(err)
		}
	}

	// Exists in both
	d = cloudStorageWorkflows.Intersect(localStorageWorkflows)
	it = d.Iterator()
	for w := range it.C {
		// Compare hash
		definitionHash, err := definition.GetHash(src, fmt.Sprintf("%v", w))
		if err != nil {
			log.Fatal(err)
		}

		storageHash, err := storage.GetHash(bucket, fmt.Sprintf("%v", w))
		if err != nil {
			log.Fatal(err)
		}

		if definitionHash == storageHash {
			// Do nothing
		} else {
			// Remove from storage
			if err := storage.RemoveWorkflow(bucket, fmt.Sprintf("%v", w)); err != nil {
				log.Fatal(err)
			}

			// Upload to storage
			if err = storage.UploadWorkflow(bucket, src, fmt.Sprintf("%v", w)); err != nil {
				log.Fatal(err)
			}
		}
	}

	return nil
}
