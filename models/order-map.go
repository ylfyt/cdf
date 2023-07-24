package models

type OrderMap[T comparable, R any] struct {
	Keys   []T
	Values map[T]R
}

func (me *OrderMap[T, R]) Set(key T, val R) {
	if _, exist := me.Values[key]; exist {
		me.Values[key] = val
		return
	}
	me.Values[key] = val
	me.Keys = append(me.Keys, key)
}

func (me *OrderMap[T, R]) Get(key T) R {
	return me.Values[key]
}

func (me *OrderMap[T, R]) GetExist(key T) (R, bool) {
	val, exist := me.Values[key]
	return val, exist
}
