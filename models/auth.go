package models

type Auth struct {
	Action string         `json:"action"`
	Rule   map[string]any `json:"rule"`
}
