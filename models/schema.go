package models

type Schema struct {
	Databases map[string]Database `json:"databases"`
}
