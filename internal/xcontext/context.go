package xcontext

import (
	"context"

	"google.golang.org/grpc"
)

type (
	transportPtr struct{}
)

func WithTransportPtr(ctx context.Context, epPtr *grpc.ClientConnInterface) context.Context {
	return context.WithValue(ctx, transportPtr{}, epPtr)
}

func GetTransportPtr(ctx context.Context) *grpc.ClientConnInterface {
	trPtr, ok := ctx.Value(transportPtr{}).(*grpc.ClientConnInterface)
	if ok {
		return trPtr
	}

	return nil
}
