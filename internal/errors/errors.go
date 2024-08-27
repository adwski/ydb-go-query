package errors

const (
	errLocalFailure = "local failure"
)

// LocalFailureError error is used globally to identify locally originated
// request errors from io errors or remote side errors.
type LocalFailureError struct {
}

func (e LocalFailureError) Error() string {
	return errLocalFailure
}
