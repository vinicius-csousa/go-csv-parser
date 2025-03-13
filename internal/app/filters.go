package parser

import (
	"bytes"
	"flag"
)

type Filter interface {
	Filter(interface{}) bool
}

type SponsorNameContainsFilter struct {
	ReferenceValue []byte
}

type SponsorGovernmentIdFilter struct {
	ReferenceValue [11]byte
}

type SellerGovernmentIdFilter struct {
	ReferenceValue [14]byte
}

func (governmentIdFilter *SponsorGovernmentIdFilter) Filter(value interface{}) bool {
	val, ok := value.(*[11]byte)
	if !ok {
		return false
	}

	if len(governmentIdFilter.ReferenceValue) == 0 {
		return true
	}

	if *val == governmentIdFilter.ReferenceValue {
		return true
	}

	return false
}

func (governmentIdFilter *SellerGovernmentIdFilter) Filter(value interface{}) bool {
	val, ok := value.(*[14]byte)
	if !ok {
		return false
	}

	if len(governmentIdFilter.ReferenceValue) == 0 {
		return true
	}

	if *val == governmentIdFilter.ReferenceValue {
		return true
	}

	return false
}

func (sponsorNameContainsFilter *SponsorNameContainsFilter) Filter(value interface{}) bool {
	val, ok := value.(*[]byte)
	if !ok {
		return false
	}

	if len(sponsorNameContainsFilter.ReferenceValue) == 0 {
		return true
	}

	byteSliceToLowerCase(val)
	return bytes.Contains(*val, sponsorNameContainsFilter.ReferenceValue)
}

func ParseFilter() Filter {
	sellerGovernmentId := flag.Bool("sellerGovernmentId", false, "Filter by seller governmentId")
	sponsorGovernmentId := flag.Bool("sponsorGovernmentId", false, "Filter by sponsor governmentId")
	sponsorNameContains := flag.Bool("sponsorNameContains", false, "Filter by substrings in the sponsor name")

	flag.Parse()

	args := flag.Args()

	if *sponsorGovernmentId {
		var refValue [11]byte
		parseSponsorGovernmentId([]byte(args[0]), &refValue)
		return &SponsorGovernmentIdFilter{
			ReferenceValue: refValue,
		}
	}

	if *sellerGovernmentId {
		var refValue [14]byte
		parseSellerGovernmentId([]byte(args[0]), &refValue)
		return &SellerGovernmentIdFilter{
			ReferenceValue: refValue,
		}
	}

	if *sponsorNameContains {
		refValue := []byte(args[0])
		byteSliceToLowerCase(&refValue)
		return &SponsorNameContainsFilter{
			ReferenceValue: refValue,
		}
	}
	return nil
}
