package domain

import "errors"

var (
	ErrUnmarshal       = errors.New("unmarshal error")
	ErrInvalidSchema   = errors.New("invalid event envelope schema")
	ErrPayloadTooLarge = errors.New("message exceeds size limit")
	ErrPanic           = errors.New("panic during processing")
)

func IsTerminalError(err error) bool {
	return errors.Is(err, ErrUnmarshal) ||
		errors.Is(err, ErrInvalidSchema) ||
		errors.Is(err, ErrPayloadTooLarge)
}
