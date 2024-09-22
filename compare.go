package sparse

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"golang.org/x/exp/constraints"
)

func CreateComparatorAny[V any]() func(v1, v2 V) int {
	var empty V

	var res interface{}
	switch interface{}(empty).(type) {
	case int:
		res = CompareNumber[int]
	case int8:
		res = CompareNumber[int8]
	case int16:
		res = CompareNumber[int16]
	case int32:
		res = CompareNumber[int32]
	case int64:
		res = CompareNumber[int64]
	case uint:
		res = CompareNumber[uint]
	case uint8:
		res = CompareNumber[uint8]
	case uint16:
		res = CompareNumber[uint16]
	case uint32:
		res = CompareNumber[uint32]
	case uint64:
		res = CompareNumber[uint64]
	case float32:
		res = CompareNumber[float32]
	case float64:
		res = CompareNumber[float64]
	case time.Duration:
		res = CompareNumber[time.Duration]
	case time.Time:
		res = func(v1, v2 time.Time) int {
			return v1.Compare(v2)
		}
	case string:
		res = func(v1, v2 string) int {
			return strings.Compare(v1, v2)
		}
	default:
		typ := reflect.TypeOf((*V)(nil)).Elem()
		panic(fmt.Sprintf("cannot compare type %v: ", typ))
	}

	return res.(func(v1, v2 V) int)
}

func CompareNumber[ValType Number](a, b ValType) int {
	if a == b {
		return 0
	}
	if a < b {
		return -1
	}
	return 1
}

type Number interface {
	constraints.Integer | constraints.Float
}
