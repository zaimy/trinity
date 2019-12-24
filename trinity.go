package trinity

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	mapset "github.com/deckarep/golang-set"

	airflow "github.com/zaimy/trinity/internal/cloud-composer"
	storage "github.com/zaimy/trinity/internal/cloud-storage"
	definition "github.com/zaimy/trinity/internal/codebase"
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
		saveHash         bool
		syncCloudStorage bool
		syncComposer     bool
	)
	fs.StringVar(&src, "src", "dags", "dags directory")
	fs.StringVar(&gcsBucket, "gcs-bucket", "", "Cloud Storage bucket name")
	fs.StringVar(&gcsDagDirectory, "gcs-dag-directory", "dags", "Cloud Storage DAG directory")
	fs.StringVar(&composerEnv, "composer-env", "", "Cloud Composer environment name")
	fs.StringVar(&composerLocation, "composer-location", "us-central1", "Cloud Composer environment location")
	fs.BoolVar(&saveHash, "save-hash", true, "If false, skip saving hash values to .trinity files.")
	fs.BoolVar(&syncCloudStorage, "sync-cloud-storage", true, "If false, skip syncing with Cloud Storage.")
	fs.BoolVar(&syncComposer, "sync-composer", true, "If false, skip syncing with Cloud Composer.")
	fs.SetOutput(errStream)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if !syncCloudStorage && syncComposer {
		log.Fatal("You cannot set true only --sync-composer. Please set true --sync-cloud-storage too.")
	}

	if saveHash {
		log.Print("------------------ Save hash values representing workflows ------------------")
		if err := definition.OverwriteAllWorkflowHashes(src); err != nil {
			log.Fatal(err)
		}
	}

	log.Print("------------------ List workflows ------------------")
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

	if syncCloudStorage {
		updatedWorkflows := mapset.NewSet()

		log.Print("------------------ Compare workflows ------------------")
		// Exists only definition
		d := localStorageWorkflows.Difference(cloudStorageWorkflows)
		it := d.Iterator()
		for w := range it.C {
			log.Printf("%s workflow exists only definition. Will Upload it.", w)
			if err := storage.UploadWorkflow(gcsBucket, gcsDagDirectory, src, fmt.Sprintf("%v", w)); err != nil {
				log.Fatal(err)
			}
			updatedWorkflows.Add(w)
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

			if syncComposer {
				// Remove from airflow
				if err = airflow.RemoveWorkflow(composerEnv, composerLocation, fmt.Sprintf("%v", w)); err != nil {
					log.Fatal(err)
				}
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
				updatedWorkflows.Add(w)
			}
		}

		it = updatedWorkflows.Iterator()
		for w := range it.C {
			fmt.Fprintln(os.Stdout, w)
		}
	}

	return nil
}
