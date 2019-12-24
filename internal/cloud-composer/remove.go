package composer

import (
	"fmt"
	"os/exec"
)

// RemoveWorkflow removes a dag from Cloud Composer (Airflow) metadata.
func RemoveWorkflow(cloudComposerEnvironment string, cloudComposerLocation string, workflow string) error {
	if _, err := exec.LookPath("gcloud"); err != nil {
		return err
	}

	cmd := exec.Command(fmt.Sprintf("gcloud composer environments run %s --location %s delete_dag -- \"%s\"", cloudComposerEnvironment, cloudComposerLocation, workflow))
	if err := cmd.Run(); err != nil {
		return err
	}

	return nil
}
