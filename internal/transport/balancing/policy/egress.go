package policy

type (
	// Egress is a generic egress node used for balancing decisions.
	Egress[T any] interface {
		*T

		Alive() bool
	}
)
