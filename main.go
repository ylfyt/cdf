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

var INSERT_QUERY = `INSERT INTO orders (name, email) values ('Yudi', 'yudi@gmail.com')`

var DELETE_QUERY = `DELETE FROM stores WHERE _id = '64bef4d717b947ddb0dc725f'`
var UPDATE_QUERY = `UPDATE orders SET user_id = 'example@gmail.com' WHERE _id = '64bef4d78c548ee82bc69fd3'`

var SIMPLE_SELECT = `
	SELECT
		*
	FROM
		store
	where 
		id = 1
`

var SELECT_QUERY = `
	SELECT 
		s.*,
		u.name as username,
		u.*
	FROM 
		store s
		JOIN users u ON s.user_id = u.id
		LEFT JOIN orders o ON o.user_id = u.id
	WHERE
		s.id = 3
`

var SELECT_QUERY2 = `
	SELECT 
		*
	FROM 
		store s
		JOIN users u ON u.id = s.user_id
		LEFT JOIN product p ON p.store_id = s.id
	WHERE
		u.id = 3
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

	stmt, err := sqlparser.Parse(SELECT_QUERY2)
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
