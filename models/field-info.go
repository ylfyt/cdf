package models

type FieldInfo struct {
	Name string
	Type string
	Ref  struct {
		Table string
		Field string
	}
}
