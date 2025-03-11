package parser

import (
	"bytes"
	"flag"
)

type Filter interface {
	Filter(interface{}) bool
}

type NameFilter struct {
	Field           []byte
	ReferenceValues [][]byte
}

func (nameFilter *NameFilter) Filter(value interface{}) bool {
	val, ok := value.([]byte)
	if !ok {
		return false
	}

	if len(nameFilter.ReferenceValues) == 0 {
		return true
	}

	for i := 0; i < len(nameFilter.ReferenceValues); i++ {
		if bytes.Equal(val, nameFilter.ReferenceValues[i]) {
			return true
		}
	}

	return false
}

func ParseFilter() Filter {
	seller := flag.Bool("seller", false, "Filter by seller name")
	sponsor := flag.Bool("sponsor", false, "Filter by sponsor name")

	flag.Parse()

	if *sponsor || *seller {
		args := flag.Args()
		values := make([][]byte, 0, len(args))
		var column []byte

		if *sponsor {
			column = []byte("sponsor")
		} else {
			column = []byte("seller")
		}

		for i := 0; i < len(args); i++ {
			values = append(values, []byte(args[i]))
		}
		return &NameFilter{
			Field:           column,
			ReferenceValues: values,
		}
	}

	return nil
}
