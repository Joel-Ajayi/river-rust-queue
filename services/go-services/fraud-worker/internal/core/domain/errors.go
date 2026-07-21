package domain

import "errors"

var (
	ErrMerchantInactiveOrNotFound = errors.New("merchant inactive or not found")
	ErrUnmarshal                  = errors.New("unmarshal error")
	ErrPanic                     = errors.New("panic during processing")
)

func IsTerminalError(err error) bool {
	return errors.Is(err, ErrMerchantInactiveOrNotFound)
}
