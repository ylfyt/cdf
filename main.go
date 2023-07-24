package main

import (
	"cdf/core"
	"cdf/models"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/xwb1989/sqlparser"
)

var INSERT_QUERY = `INSERT INTO orders (user_id, product_id) values (1, NULL)`
var DELETE_QUERY = `DELETE FROM orders WHERE user_id = CAST('1' AS UNSIGNED)`
var UPDATE_QUERY = `UPDATE orders SET user_id = 'example@gmail.com' WHERE _id = '64bb05369b0011ef0942db1b'`
var SIMPLE_SELECT = `
	SELECT
		u.user_id as userid
	FROM
		users u
`

var SELECT_QUERY = `
	SELECT 
		o.*, users.id, users.name AS username 
	FROM 
		stores s
		JOIN users u ON s.user_id = u.id
		LEFT JOIN products p ON p.store_id = s.id AND 1000 > p.created_at 
`

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

	stmt, err := sqlparser.Parse(SELECT_QUERY)
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
