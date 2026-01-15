package common

import "fmt"

type CodedError struct {
	Code int
	Msg  string
	Err  error
}

func (e CodedError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s: %v", e.Msg, e.Err)
	}
	return e.Msg
}

func (e CodedError) Unwrap() error { return e.Err }

func NewCodedError(code int, cause error) error {
	msg := ErrCodeMessage[code]
	if msg == "" {
		msg = "unknown error"
	}
	return CodedError{Code: code, Msg: msg, Err: cause}
}

func GetErrorCode(err error) (int, bool) {
	if ce, ok := err.(CodedError); ok {
		return ce.Code, true
	}
	return 0, false
}
