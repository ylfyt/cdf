package main

import (
	// "cdf/api"
	"cdf/core"
	"cdf/models"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/xwb1989/sqlparser"
)

var INSERT_QUERY = `INSERT INTO orders (user_id, product_id) values (true, NULL)`
var DELETE_QUERY = `DELETE FROM orders WHERE _id = '64bb0284d07aaffcbe24aebb'`

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

	stmt, err := sqlparser.Parse(INSERT_QUERY)
	if err != nil {
		fmt.Println("err", err)
		return
	}

	res, err := core.Execute(stmt)
	if err != nil {
		fmt.Println("err", err)
		return
	}
	fmt.Printf("Data: %+v\n", res)
	// api.Start()
}
