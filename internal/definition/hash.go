package definition

import (
	"archive/tar"
	"bytes"
	"crypto/sha1"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	mapset "github.com/deckarep/golang-set"
)

// GetHash gets a hash string of a dag from definition.
func GetHash(src string, workflow string) (string, error) {
	h, err := ioutil.ReadFile(filepath.Join(src, fmt.Sprintf("%s.trinity", workflow)))
	if err != nil {
		return "", err
	}
	return string(h), nil
}

// OverwriteAllWorkflowHashes saves hash value to definition.
func OverwriteAllWorkflowHashes(src string) error {
	infos, err := ioutil.ReadDir(src)
	if err != nil {
		return err
	}

	workflows := mapset.NewSet()

	rep := regexp.MustCompile(`^\w+\.py$`)
	for _, info := range infos {
		if rep.MatchString(info.Name()) {
			workflow := strings.Replace(info.Name(), ".py", "", 1)
			workflows.Add(workflow)
		} else if info.IsDir() {
			workflow := info.Name()
			workflows.Add(workflow)
		}
	}

	it := workflows.Iterator()
	for workflow := range it.C {
		saveHash(src, fmt.Sprintf("%s", workflow))
	}

	return nil
}

func saveHash(src string, workflow string) error {
	h, err := hashing(src, workflow)
	if err != nil {
		return err
	}

	trinityFile, err := os.Create(filepath.Join(src, fmt.Sprintf("%s.trinity", workflow)))
	if err != nil {
		return err
	}
	defer trinityFile.Close()

	_, err = trinityFile.WriteString(fmt.Sprintf("%x", h))
	if err != nil {
		return err
	}

	log.Printf("Saved hash value representing workflow: %v", workflow)

	return nil
}

func hashing(src string, workflow string) ([]byte, error) {
	var buf bytes.Buffer

	if err := taring(src, workflow, &buf); err != nil {
		return nil, err
	}

	h := sha1.New()
	h.Write(buf.Bytes())
	bs := h.Sum(nil)
	return bs, nil
}

func taring(src string, workflow string, buf *bytes.Buffer) error {
	tarWriter := tar.NewWriter(buf)
	defer tarWriter.Close()

	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		dirRep := fmt.Sprintf("^%s/%s/", src, workflow)
		pyRep := fmt.Sprintf("^%s/%s\\.py$", src, workflow)
		rep := regexp.MustCompile(fmt.Sprintf("%s|%s", dirRep, pyRep))
		if !rep.MatchString(path) {
			return nil
		}

		if !info.Mode().IsRegular() {
			return nil
		}

		if info.Name() == ".trinity" {
			return nil
		}

		// header
		header, err := tar.FileInfoHeader(info, info.Name())
		if err != nil {
			return err
		}
		if err := tarWriter.WriteHeader(header); err != nil {
			return err
		}

		// body
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		if _, err := io.Copy(tarWriter, file); err != nil {
			return err
		}
		file.Close()

		return nil
	})
}
