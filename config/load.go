package config

import (
	"gopkg.in/yaml.v3"
	"implementation-message-broker-golang/helpers"
	"log"
	"os"
	"path/filepath"
)

type Config struct {
	Env                          string `yaml:"ENV"`
	KafkaEndpoint                string `yaml:"KAFKA_ENDPOINT"`
	GoogleApplicationCredentials string `yaml:"GOOGLE_APPLICATION_CREDENTIALS"`
}

func ReadConfigFromFile() (*Config, error) {
	var credentials Config

	dir := helpers.GetCurrentDirectory()
	filePath := filepath.Join(dir, "config/config.yml")

	yamlFile, err := os.ReadFile(filePath)
	if err != nil {
		log.Fatalf("Failed read file YAML: %s", err)
	}

	err = yaml.Unmarshal(yamlFile, &credentials)
	if err != nil {
		log.Fatalf("Failed unmarshal file YAML: %s", err)
	}

	return &credentials, nil
}
