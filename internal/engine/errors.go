package engine

import "fmt"

type Error struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (e *Error) Error() string { return e.Message }
func fault(code, format string, args ...any) *Error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...)}
}
func invalid(err error) error {
	if err == nil {
		return nil
	}
	return fault("invalid", "%s", err)
}

// ReportError pairs a domain error with the structured report that explains
// the rejection, so callers can inspect conflicts and violations without
// re-deriving them from a free-text message.
type ReportError struct {
	Domain *Error `json:"-"`
	Report any    `json:"report"`
}

func (e *ReportError) Error() string { return e.Domain.Message }
func (e *ReportError) Unwrap() error { return e.Domain }
