package nsq

import "errors"

var (
	ErrInvalid      = errors.New("E_INVALID")
	ErrBadBody      = errors.New("E_BAD_BODY")
	ErrBadTopic     = errors.New("E_BAD_TOPIC")
	ErrBadChannel   = errors.New("E_BAD_CHANNEL")
	ErrBadMessage   = errors.New("E_BAD_MESSAGE")
	ErrPubFailed    = errors.New("E_PUB_FAILED")
	ErrMPubFailed   = errors.New("E_MPUB_FAILED")
	ErrFinFailed    = errors.New("E_FIN_FAILED")
	ErrReqFailed    = errors.New("E_REQ_FAILED")
	ErrTouchFailed  = errors.New("E_TOUCH_FAILED")
	ErrAuthFailed   = errors.New("E_AUTH_FAILED")
	ErrUnauthorized = errors.New("E_UNAUTHORIZED")
)
