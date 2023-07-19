package models

type ReqField struct {
	Name   string
	Parent string
	Level  int
	Order  int64
	Filter any
	Out    bool
	Table  string
}
