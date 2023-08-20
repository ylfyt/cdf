package core

import (
	"cdf/models"
	"encoding/json"
)

type ctxInfo struct {
	Db    string
	Table string
	Field string
}

func getAuth(ctx *ctxInfo) []models.Auth {
	if ctx.Table == "" && ctx.Field == "" {
		db, exist := schema.Databases[ctx.Db]
		if !exist {
			return nil
		}
		return db.Auths
	}
	if ctx.Field == "" {
		db, exist := schema.Databases[ctx.Db]
		if !exist {
			return nil
		}
		table, exist := db.Tables[ctx.Table]
		if !exist {
			return nil
		}
		return table.Auths
	}

	db, exist := schema.Databases[ctx.Db]
	if !exist {
		return nil
	}
	table, exist := db.Tables[ctx.Table]
	if !exist {
		return nil
	}
	field, exist := table.Fields[ctx.Field]
	if !exist {
		return nil
	}
	if field, ok := field.(map[string]any); ok {
		auth := field["auth"]
		if auth == nil {
			return nil
		}
		data, _ := json.Marshal(auth)
		var auths []models.Auth
		_ = json.Unmarshal(data, &auths)
		return auths
	}
	return nil
}
