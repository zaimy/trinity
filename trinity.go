package trinity

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	mapset "github.com/deckarep/golang-set"

	composer "github.com/zaimy/trinity/internal/cloud-composer"
	storage "github.com/zaimy/trinity/internal/cloud-storage"
	"github.com/zaimy/trinity/internal/codebase"
)

// Run runs the trinity.
func Run(args []string, outStream, errStream io.Writer) error {
	fs := flag.NewFlagSet("trinity", flag.ContinueOnError)
	var (
		codebaseDagDirectory     string
		cloudStorageDagDirectory string
		cloudStorageBucket       string
		cloudComposerEnvironment string
		cloudComposerLocation    string
		saveHash                 bool
		syncCloudStorage         bool
		syncCloudComposer        bool
		dryRun                   bool
	)
	fs.StringVar(&codebaseDagDirectory, "codebase-dag-directory", "dags", "codebase DAG directory path")
	fs.StringVar(&cloudStorageBucket, "cloud-storage-bucket", "", "Cloud Storage bucket name")
	fs.StringVar(&cloudStorageDagDirectory, "cloud-storage-dag-dir", "dags", "Cloud Storage DAG directory path")
	fs.StringVar(&cloudComposerEnvironment, "cloud-composer-env", "", "Cloud Composer environment name")
	fs.StringVar(&cloudComposerLocation, "cloud-composer-location", "us-central1", "Cloud Composer environment location")
	fs.BoolVar(&saveHash, "save-hash", true, "If false, skip saving hash values to .trinity files.")
	fs.BoolVar(&syncCloudStorage, "sync-cloud-storage", true, "If false, skip syncing with Cloud Storage.")
	fs.BoolVar(&syncCloudComposer, "sync-cloud-composer", true, "If false, skip syncing with Cloud Composer.")
	fs.BoolVar(&dryRun, "dry-run", false, "If true, only shows DAGs to be processed to stdout.")
	fs.SetOutput(errStream)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if !syncCloudStorage && syncCloudComposer {
		log.Fatal("You cannot set true only --sync-cloud-composer. Please set true --sync-cloud-storage too.")
	}

	if saveHash {
		log.Print("====> Save hash values representing workflows...")
		if err := codebase.SaveAllWorkflowHashes(codebaseDagDirectory); err != nil {
			log.Fatal(err)
		}
	}

	if syncCloudStorage {
		cloudStorageWorkflows, err := storage.ListWorkflows(cloudStorageBucket, cloudStorageDagDirectory)
		if err != nil {
			log.Fatal(err)
		}
		codebaseWorkflow, err := codebase.ListWorkflows(codebaseDagDirectory)
		if err != nil {
			log.Fatal(err)
		}

		updatedWorkflows := mapset.NewSet()

		log.Print("====> Compare workflows...")
		// Exists only codebase
		d := codebaseWorkflow.Difference(cloudStorageWorkflows)
		it := d.Iterator()
		for w := range it.C {
			if !dryRun {
				log.Printf("%s workflow exists only codebase. Adding...", w)
				if err := storage.UploadWorkflow(cloudStorageBucket, cloudStorageDagDirectory, codebaseDagDirectory, fmt.Sprintf("%v", w)); err != nil {
					log.Fatal(err)
				}
			} else {
				log.Printf("%s workflow exists only codebase. (dry-run)", w)
			}
			updatedWorkflows.Add(w)
		}

		// Exists only Cloud Storage
		d = cloudStorageWorkflows.Difference(codebaseWorkflow)
		it = d.Iterator()
		for w := range it.C {
			if !dryRun {
				log.Printf("%s workflow exists only Cloud Storage. Deleting...", w)
				// Remove from Cloud Storage
				if err := storage.RemoveWorkflow(cloudStorageBucket, cloudStorageDagDirectory, fmt.Sprintf("%v", w)); err != nil {
					log.Fatal(err)
				}

				if syncCloudComposer {
					// Remove from Cloud Composer
					if err = composer.RemoveWorkflow(cloudComposerEnvironment, cloudComposerLocation, fmt.Sprintf("%v", w)); err != nil {
						log.Fatal(err)
					}
				}
			} else {
				log.Printf("%s workflow exists only Cloud Storage. (dry-run)", w)
			}
		}

		// Exists in both
		d = cloudStorageWorkflows.Intersect(codebaseWorkflow)
		it = d.Iterator()
		for w := range it.C {
			// Compare hash
			codebaseHash, err := codebase.GetHash(codebaseDagDirectory, fmt.Sprintf("%v", w))
			if err != nil {
				log.Fatal(err)
			}

			cloudStorageHash, err := storage.GetHash(cloudStorageBucket, cloudStorageDagDirectory, fmt.Sprintf("%v", w))
			if err != nil {
				log.Fatal(err)
			}

			if codebaseHash == cloudStorageHash {
				// Do nothing
			} else {
				if !dryRun {
					// Remove from Cloud Storage
					log.Printf("%s workflow exists both codebase and Cloud Storage. These hash values NOT matched. Updating...", w)
					if err := storage.RemoveWorkflow(cloudStorageBucket, cloudStorageDagDirectory, fmt.Sprintf("%v", w)); err != nil {
						log.Fatal(err)
					}

					// Upload to Cloud Storage
					if err = storage.UploadWorkflow(cloudStorageBucket, cloudStorageDagDirectory, codebaseDagDirectory, fmt.Sprintf("%v", w)); err != nil {
						log.Fatal(err)
					}
				} else {
					log.Printf("%s workflow exists both codebase and Cloud Storage. These hash values NOT matched. (dry-run)", w)
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
