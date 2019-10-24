package definition

import (
	"fmt"
	"path/filepath"
	"regexp"

	mapset "github.com/deckarep/golang-set"
)

func ListWorkflows(src string) (mapset.Set, error) {
	workflowNames := mapset.NewSet()
	files, _ := filepath.Glob(fmt.Sprintf("%s/*/.trinity", src))
	for _, f := range files {
		rep := regexp.MustCompile(`\s*/\s*`)
		result := rep.Split(f, -1)
		workflowNames.Add(result[1])
	}

	return workflowNames, nil
}
