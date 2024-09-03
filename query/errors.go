package query

import "errors"

var (
	ErrExec   = errors.New("query execution failed")
	ErrResult = errors.New("result fetch error")
)
