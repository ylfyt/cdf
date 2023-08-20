package models

type Table struct {
	Name   string         `json:"name"`
	Fields map[string]any `json:"fields"`
	Auths  []Auth         `json:"auth"`
}
