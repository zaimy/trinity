package definition

import (
	"archive/tar"
	"bytes"
	"crypto/sha1"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
)

// GetHash gets a hash string of a dag from definition.
func GetHash(src string, workflow string) (string, error) {
	h, err := ioutil.ReadFile(filepath.Join(src, workflow, ".trinity"))
	if err != nil {
		return "", err
	}
	return string(h), nil
}

// SaveHash saves hash value to definition.
func SaveHash(src string) error {
	fis, err := ioutil.ReadDir(src)
	if err != nil {
		return err
	}

	for _, fi := range fis {
		if fi.IsDir() {
			h, err := hashing(filepath.Join(src, fi.Name()))
			if err != nil {
				return err
			}

			file, err := os.Create(filepath.Join(src, fi.Name(), ".trinity"))
			if err != nil {
				return err
			}
			defer file.Close()

			_, err = file.WriteString(fmt.Sprintf("%x", h))
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func hashing(src string) ([]byte, error) {
	var buf bytes.Buffer

	if err := taring(src, &buf); err != nil {
		return nil, err
	} else {
		h := sha1.New()
		h.Write(buf.Bytes())
		bs := h.Sum(nil)
		return bs, nil
	}
}

func taring(src string, buf *bytes.Buffer) error {
	if _, err := os.Stat(src); err != nil {
		return fmt.Errorf("%v", err.Error())
	}

	tarWriter := tar.NewWriter(buf)
	defer tarWriter.Close()

	return filepath.Walk(src, func(file string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !fi.Mode().IsRegular() {
			return nil
		}

		if fi.Name() == ".trinity" {
			return nil
		}

		// header
		header, err := tar.FileInfoHeader(fi, fi.Name())
		if err != nil {
			return err
		}
		if err := tarWriter.WriteHeader(header); err != nil {
			return err
		}

		// body
		f, err := os.Open(file)
		if err != nil {
			return err
		}
		if _, err := io.Copy(tarWriter, f); err != nil {
			return err
		}
		f.Close()

		return nil
	})
}
