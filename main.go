package main

import (
	"cdf/api"
	"cdf/core"
	"cdf/models"
	"encoding/json"
	"io"
	"os"
)

func main() {
	file, err := os.Open("./schema.json")
	if err != nil {
		panic(err)
	}

	data, err := io.ReadAll(file)
	if err != nil {
		panic(err)
	}

	var schema models.Schema
	err = json.Unmarshal(data, &schema)
	if err != nil {
		panic(err)
	}

	core.Start(&schema)
	api.Start()
}
