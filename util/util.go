package util

import "errors"

var (
	ErrEmptyKey   = errors.New("passed key is empty")
	ErrEmptyValue = errors.New("passed value is empty")
)

// TODO: Add checking of bucket name
// CheckKeyAndValue returns an error if k == "" or if v == nil
func CheckKeyAndValue(k string, v interface{}) error {
	if err := CheckKey(k); err != nil {
		return err
	}
	return CheckVal(v)
}

// CheckKey returns an error if k == ""
func CheckKey(k string) error {
	if k == "" {
		return ErrEmptyKey
	}
	return nil
}

// CheckVal returns an error if v == nil
func CheckVal(v interface{}) error {
	if v == nil {
		return ErrEmptyValue
	}
	return nil
}
