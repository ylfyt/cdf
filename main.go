package main

import (
	"cdf/db"
	"cdf/models"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/xwb1989/sqlparser"
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

	db.Start(&schema)

	query := `
		insert into m_users (
			id, username
		)
		select * from ho.m_users
	`

	stmt, err := sqlparser.Parse(query)
	if err != nil {
		fmt.Printf("Failed to parse query: %v\n", err)
		return
	}

	fmt.Printf("Data: %+v\n", stmt)
}
