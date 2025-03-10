package parser

import (
	"bytes"
)

// Comparison: 0 for '==' and 1 for '!='
func FilterName(value []byte, referenceValues [][]byte) bool {
	if len(referenceValues) == 0 {
		return true
	}

	for i := 0; i < len(referenceValues); i++ {
		if bytes.Equal(value, referenceValues[i]) {
			return true
		}
	}

	return false
}
