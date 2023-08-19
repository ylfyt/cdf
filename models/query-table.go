package models

type QueryTable struct {
	DepConds     []*Cond
	Conds        []*Cond
	Name         string
	Join         string
	SelectFields map[string]any
}
