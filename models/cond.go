package models

type CondInfo struct {
	Qualifier string
	Field     string
	Value     any
}

type Cond struct {
	Left  CondInfo
	Right CondInfo
	Op    string
}
