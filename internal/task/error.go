package task

import "errors"

// PermanentError marks a handler failure as non-retryable, e.g. an invalid
// payload that will never succeed on retry. Wrap a cause with Permanent(err)
// so the worker dead-letters the task immediately instead of consuming
// retry attempts.
type PermanentError struct {
	Err error
}

func (e *PermanentError) Error() string { return e.Err.Error() }
func (e *PermanentError) Unwrap() error { return e.Err }

func Permanent(err error) error {
	return &PermanentError{Err: err}
}

func IsPermanent(err error) bool {
	var pe *PermanentError
	return errors.As(err, &pe)
}
