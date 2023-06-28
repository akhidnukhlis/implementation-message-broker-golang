package helpers

import (
	"log"
	"os"
)

func GetCurrentDirectory() string {
	dir, err := os.Getwd()
	if err != nil {
		log.Fatalf("Failed read current directory: %s", err)
	}

	return dir
}
