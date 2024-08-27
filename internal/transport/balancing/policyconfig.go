package balancing

import (
	"errors"

	"github.com/adwski/ydb-go-query/v1/internal/transport/balancing/policy"
)

const (
	PolicyKindFirstReady = "FirstReady"
	PolicyKindRandom     = "Random"
	PolicyKindRoundRobin = "RoundRobin"
)

var ErrPolicyUnknown = errors.New("unknown balancing policy")

func validatePolicyKind(kind string) error {
	switch kind {
	case PolicyKindFirstReady, PolicyKindRandom, PolicyKindRoundRobin:
		return nil
	}
	return ErrPolicyUnknown
}

func newPolicy[PT policy.Egress[T], T any](kind string) (balancingPolicy[PT, T], error) {
	switch kind {
	case PolicyKindFirstReady:
		return policy.NewFirstReady[PT, T](), nil
	case PolicyKindRandom:
		return policy.NewRandom[PT, T](), nil
	case PolicyKindRoundRobin:
		return policy.NewRoundRobin[PT, T](), nil
	}
	return nil, ErrPolicyUnknown
}
