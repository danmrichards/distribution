package utils

// BoolPtr returns the pointer to the supplied parameter.
func BoolPtr(b bool) *bool {
	return &b
}

// StringPtr returns the pointer to the supplied parameter.
func StringPtr(s string) *string {
	return &s
}

// IntPtr returns the pointer to the supplied parameter.
func IntPtr(i int) *int {
	return &i
}

// Int64Ptr returns the pointer to the supplied parameter.
func Int64Ptr(i int64) *int64 {
	return &i
}
