package dispatcher

import "errors"

var (
	ErrCreateEndpoint = errors.New("cannot create endpoint")
	ErrNoConnections  = errors.New("no alive connections available")
)
