package airflow

import (
	"fmt"
	"os/exec"
)

func RemoveWorkflow(composerEnv string, composerLocation string, workflow string) error {
	if _, err := exec.LookPath("gcloud"); err != nil {
		return err
	}

	cmd := exec.Command(fmt.Sprintf("gcloud composer environments run %s --location %s delete_dag -- \"%s\"", composerEnv, composerLocation, workflow))
	if err := cmd.Run(); err != nil {
		return err
	}

	return nil
}
