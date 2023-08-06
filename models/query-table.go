package models

type QueryTable struct {
	Conds        []*Cond
	Name         string
	Join         string
	SelectFields map[string]any
}
