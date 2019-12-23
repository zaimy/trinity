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
		gcsDagDirectory  string
		gcsBucket        string
		composerEnv      string
		composerLocation string
	)
	fs.StringVar(&src, "src", "dags", "dags directory")
	fs.StringVar(&gcsBucket, "gcs-bucket", "", "Cloud Storage bucket name")
	fs.StringVar(&gcsDagDirectory, "gcs-dag-directory", "dags", "Cloud Storage DAG directory")
	fs.StringVar(&composerEnv, "composer-env", "", "Cloud Composer environment name")
	fs.StringVar(&composerLocation, "composer-location", "us-central1", "Cloud Composer environment location")
	fs.SetOutput(errStream)
	if err := fs.Parse(args); err != nil {
		return err
	}

	log.Print("------------------ 01. Save hash values representing workflows ------------------")
	if err := definition.OverwriteAllWorkflowHashes(src); err != nil {
		log.Fatal(err)
	}

	log.Print("------------------ 02. List workflows ------------------")
	cloudStorageWorkflows, err := storage.ListWorkflows(gcsBucket, gcsDagDirectory)
	log.Printf("Google Cloud Storage: %s", cloudStorageWorkflows)
	if err != nil {
		log.Fatal(err)
	}
	localStorageWorkflows, err := definition.ListWorkflows(src)
	log.Printf("definition: %s", localStorageWorkflows)
	if err != nil {
		log.Fatal(err)
	}

	log.Print("------------------ 03. Compare workflows ------------------")
	// Exists only definition
	d := localStorageWorkflows.Difference(cloudStorageWorkflows)
	it := d.Iterator()
	for w := range it.C {
		log.Printf("%s workflow exists only definition. Will Upload it.", w)
		if err := storage.UploadWorkflow(gcsBucket, gcsDagDirectory, src, fmt.Sprintf("%v", w)); err != nil {
			log.Fatal(err)
		}
	}

	// Exists only storage
	d = cloudStorageWorkflows.Difference(localStorageWorkflows)
	it = d.Iterator()
	for w := range it.C {
		log.Printf("%s workflow exists only Google Cloud Storage. Will remove it.", w)
		// Remove from storage
		if err := storage.RemoveWorkflow(gcsBucket, gcsDagDirectory, fmt.Sprintf("%v", w)); err != nil {
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

		storageHash, err := storage.GetHash(gcsBucket, gcsDagDirectory, fmt.Sprintf("%v", w))
		if err != nil {
			log.Fatal(err)
		}

		if definitionHash == storageHash {
			// Do nothing
			log.Printf("%s workflow exists both definition and Google Cloud Storage. These hash values matched. Do nothing.", w)
		} else {
			// Remove from storage
			log.Printf("%s workflow exists both definition and Google Cloud Storage. These hash values NOT matched. Will update it.", w)
			if err := storage.RemoveWorkflow(gcsBucket, gcsDagDirectory, fmt.Sprintf("%v", w)); err != nil {
				log.Fatal(err)
			}

			// Upload to storage
			if err = storage.UploadWorkflow(gcsBucket, gcsDagDirectory, src, fmt.Sprintf("%v", w)); err != nil {
				log.Fatal(err)
			}
		}
	}

	return nil
}
