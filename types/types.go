package types

import "github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

func Bool(val bool) *Ydb.TypedValue {
	return &Ydb.TypedValue{
		Type:  &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_BOOL}},
		Value: &Ydb.Value{Value: &Ydb.Value_BoolValue{BoolValue: val}},
	}
}

func Int32(val int32) *Ydb.TypedValue {
	return &Ydb.TypedValue{
		Type:  &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_INT32}},
		Value: &Ydb.Value{Value: &Ydb.Value_Int32Value{Int32Value: val}},
	}
}

func Uint32(val uint32) *Ydb.TypedValue {
	return &Ydb.TypedValue{
		Type:  &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_UINT32}},
		Value: &Ydb.Value{Value: &Ydb.Value_Uint32Value{Uint32Value: val}},
	}
}

func Int64(val int64) *Ydb.TypedValue {
	return &Ydb.TypedValue{
		Type:  &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_INT64}},
		Value: &Ydb.Value{Value: &Ydb.Value_Int64Value{Int64Value: val}},
	}
}

func Uint64(val uint64) *Ydb.TypedValue {
	return &Ydb.TypedValue{
		Type:  &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_UINT64}},
		Value: &Ydb.Value{Value: &Ydb.Value_Uint64Value{Uint64Value: val}},
	}
}

func Float(val float32) *Ydb.TypedValue {
	return &Ydb.TypedValue{
		Type:  &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_FLOAT}},
		Value: &Ydb.Value{Value: &Ydb.Value_FloatValue{FloatValue: val}},
	}
}

func Double(val float64) *Ydb.TypedValue {
	return &Ydb.TypedValue{
		Type:  &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_DOUBLE}},
		Value: &Ydb.Value{Value: &Ydb.Value_DoubleValue{DoubleValue: val}},
	}
}

func UTF8(val string) *Ydb.TypedValue {
	return &Ydb.TypedValue{
		Type:  &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_UTF8}},
		Value: &Ydb.Value{Value: &Ydb.Value_TextValue{TextValue: val}},
	}
}

func Text(val string) *Ydb.TypedValue {
	return &Ydb.TypedValue{
		Type:  &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_STRING}},
		Value: &Ydb.Value{Value: &Ydb.Value_TextValue{TextValue: val}},
	}
}
