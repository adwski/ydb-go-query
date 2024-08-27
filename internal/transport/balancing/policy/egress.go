package policy

type (
	Egress[T any] interface {
		*T

		Alive() bool
	}
)
