package utils

import (
	"reflect"
	"unsafe"
)

func UnsafeSlice(slice, data unsafe.Pointer, len int) {
	s := (*reflect.SliceHeader)(slice)
	s.Data = uintptr(data)
	s.Cap = len
	s.Len = len
}
