package definition

import (
	"fmt"
	"path/filepath"
	"regexp"
	"strings"

	mapset "github.com/deckarep/golang-set"
)

// ListWorkflows lists dags on definition.
func ListWorkflows(src string) (mapset.Set, error) {
	workflows := mapset.NewSet()
	paths, _ := filepath.Glob(fmt.Sprintf("%s/*.trinity", src)) // TODO: Consider Windows file paths
	for _, path := range paths {
		rep := regexp.MustCompile(`\s*/\s*`)
		pathElement := rep.Split(path, -1)
		workflow := strings.Replace(pathElement[1], ".trinity", "", 1)
		workflows.Add(workflow)
	}

	return workflows, nil
}
