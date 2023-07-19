package models

type Database struct {
	Type             string           `json:"type"`
	Name             string           `json:"name"`
	ConnectionString string           `json:"connectionString"`
	Tables           map[string]Table `json:"tables"`
}
