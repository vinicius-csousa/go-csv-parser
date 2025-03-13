package parser

import (
	"bufio"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"unicode"
)

// Fields from the csv along with their column indices in the csv
var Fields = struct {
	SellerGovernmentId  int8
	SellerName          int8
	SponsorName         int8
	SponsorGovernmentId int8
	NominalValue        int8
	PresentValue        int8
	AcquisitionValue    int8
	DocumentNumber      int8
}{
	SellerGovernmentId:  3,
	SellerName:          4,
	SponsorName:         6,
	SponsorGovernmentId: 7,
	NominalValue:        9,
	PresentValue:        10,
	AcquisitionValue:    11,
	DocumentNumber:      16,
}

type Aggregate struct {
	MinNominalValue     float32 // 4 bytes
	MaxNominalValue     float32 // 4 bytes
	SumNominalValue     float32 // 4 bytes
	MinPresentValue     float32 // 4 bytes
	MaxPresentValue     float32 // 4 bytes
	SumPresentValue     float32 // 4 bytes
	MinAcquisitionValue float32 // 4 bytes
	MaxAcquisitionValue float32 // 4 bytes
	SumAcquisitionValue float32 // 4 bytes
	DocumentNumber      int32   // 4 bytes
	AmountOfRecords     int8    // 1 byte
}

type SimplifiedAggregate struct {
	SponsorName         []byte   // 24 bytes
	SellerName          []byte   // 24 bytes
	SellerGovernmentId  [14]byte // 14 bytes
	SponsorGovernmentId [11]byte // 11 bytes
	NominalValue        float32  // 4 bytes
	PresentValue        float32  // 4 bytes
	AcquisitionValue    float32  // 4 bytes
	DocumentNumber      int32    // 4 bytes
}

func (aggregate *Aggregate) Sum(newAggregate *Aggregate) {
	aggregate.SumNominalValue += newAggregate.SumNominalValue
	aggregate.MaxNominalValue = max(aggregate.MaxNominalValue, newAggregate.MaxNominalValue)
	aggregate.MinNominalValue = min(aggregate.MinNominalValue, newAggregate.MinNominalValue)

	aggregate.SumPresentValue += newAggregate.SumPresentValue
	aggregate.MaxPresentValue = max(aggregate.MaxPresentValue, newAggregate.MaxPresentValue)
	aggregate.MinPresentValue = min(aggregate.MinPresentValue, newAggregate.MinPresentValue)

	aggregate.SumAcquisitionValue += newAggregate.SumAcquisitionValue
	aggregate.MaxAcquisitionValue = max(aggregate.MaxAcquisitionValue, newAggregate.MaxAcquisitionValue)
	aggregate.MinAcquisitionValue = min(aggregate.MinAcquisitionValue, newAggregate.MinAcquisitionValue)

	aggregate.AmountOfRecords += newAggregate.AmountOfRecords
}

func Parse(filepath string, separator byte, ch chan Aggregate, wg *sync.WaitGroup, filter Filter) {
	file, err := os.Open(filepath)
	if err != nil {
		log.Fatalf("Failed to open file: %s", filepath)
	}

	defer file.Close()
	defer wg.Done()

	var aggregate Aggregate
	var tmpAggregate SimplifiedAggregate

	reader := bufio.NewReader(file)
	reader.ReadSlice('\n') // skip header

	var shouldSendPrevious bool
	for {
		// Read csv line by line
		line, err := reader.ReadSlice('\n')

		if err != nil && err != io.EOF {
			log.Fatalf("Failed to read file: %s", filepath)
		}

		// Send last aggregate and break loop if the file has ended
		if err == io.EOF {
			if shouldSendPrevious {
				ch <- aggregate
			}
			break
		}

		shouldSend := parseLine(line, &tmpAggregate, separator, filter)

		if tmpAggregate.DocumentNumber == aggregate.DocumentNumber {
			aggregate.SumNominalValue += tmpAggregate.NominalValue
			aggregate.MaxNominalValue = max(aggregate.MaxNominalValue, tmpAggregate.NominalValue)
			aggregate.MinNominalValue = min(aggregate.MinNominalValue, tmpAggregate.NominalValue)

			aggregate.SumPresentValue += tmpAggregate.PresentValue
			aggregate.MaxPresentValue = max(aggregate.MaxPresentValue, tmpAggregate.PresentValue)
			aggregate.MinPresentValue = min(aggregate.MinPresentValue, tmpAggregate.PresentValue)

			aggregate.SumAcquisitionValue += tmpAggregate.AcquisitionValue
			aggregate.MaxAcquisitionValue = max(aggregate.MaxAcquisitionValue, tmpAggregate.AcquisitionValue)
			aggregate.MinAcquisitionValue = min(aggregate.MinAcquisitionValue, tmpAggregate.AcquisitionValue)

			aggregate.AmountOfRecords++

		} else {
			if aggregate.DocumentNumber != 0 && shouldSendPrevious {
				ch <- aggregate
			}

			aggregate.SumNominalValue = tmpAggregate.NominalValue
			aggregate.MaxNominalValue = tmpAggregate.NominalValue
			aggregate.MinNominalValue = tmpAggregate.NominalValue

			aggregate.SumPresentValue = tmpAggregate.PresentValue
			aggregate.MaxPresentValue = tmpAggregate.PresentValue
			aggregate.MinPresentValue = tmpAggregate.PresentValue

			aggregate.SumAcquisitionValue = tmpAggregate.AcquisitionValue
			aggregate.MaxAcquisitionValue = tmpAggregate.AcquisitionValue
			aggregate.MinAcquisitionValue = tmpAggregate.AcquisitionValue

			aggregate.AmountOfRecords = 1
			aggregate.DocumentNumber = tmpAggregate.DocumentNumber
		}

		shouldSendPrevious = shouldSend
	}
}

func parseLine(line []byte, tmpAggregate *SimplifiedAggregate, separator byte, filter Filter) bool {
	var startIndex int16
	var endIndex int16
	var fieldNum int8

	for i := 0; i < len(line); i++ {
		if line[i] != separator && line[i] != '\n' {
			continue
		}

		endIndex = int16(i)

		switch fieldNum {
		case Fields.SellerGovernmentId:
			parseSellerGovernmentId(line[startIndex:endIndex], &tmpAggregate.SellerGovernmentId)
		case Fields.SellerName:
			tmpAggregate.SellerName = line[startIndex:endIndex]
		case Fields.SponsorGovernmentId:
			parseSponsorGovernmentId(line[startIndex:endIndex], &tmpAggregate.SponsorGovernmentId)
		case Fields.SponsorName:
			tmpAggregate.SponsorName = line[startIndex:endIndex]
		case Fields.DocumentNumber:
			val, _ := strconv.ParseInt(string(line[startIndex:endIndex]), 0, 32)
			tmpAggregate.DocumentNumber = int32(val)
		case Fields.NominalValue:
			val, err := strconv.ParseFloat(strings.ReplaceAll(string(line[startIndex:endIndex]), ",", ""), 32)
			if err != nil {
				log.Fatal(err.Error())
			}
			tmpAggregate.NominalValue = float32(val)
		case Fields.PresentValue:
			val, err := strconv.ParseFloat(strings.ReplaceAll(string(line[startIndex:endIndex]), ",", ""), 32)
			if err != nil {
				log.Fatal(err.Error())
			}
			tmpAggregate.PresentValue = float32(val)
		case Fields.AcquisitionValue:
			val, err := strconv.ParseFloat(strings.ReplaceAll(string(line[startIndex:endIndex]), ",", ""), 32)
			if err != nil {
				log.Fatal(err.Error())
			}
			tmpAggregate.AcquisitionValue = float32(val)
		}

		fieldNum++

		// Stop parsing once the most distant columns has been parsed
		if fieldNum > Fields.DocumentNumber {
			return filterLine(filter, tmpAggregate)
		}

		startIndex = endIndex + 1
	}
	return filterLine(filter, tmpAggregate)
}

func filterLine(filter Filter, aggregate *SimplifiedAggregate) bool {
	_, ok := filter.(*SponsorGovernmentIdFilter)
	if ok {
		return filter.Filter(&aggregate.SponsorGovernmentId)
	}

	_, ok = filter.(*SellerGovernmentIdFilter)
	if ok {
		return filter.Filter(&aggregate.SellerGovernmentId)
	}

	_, ok = filter.(*SponsorNameContainsFilter)
	if ok {
		return filter.Filter(&aggregate.SponsorName)
	}

	return true
}

func parseSponsorGovernmentId(governmentId []byte, formattedGovernmentId *[11]byte) {
	counter := 0

	for i := 0; i < len(governmentId); i++ {
		if governmentId[i] != '.' && governmentId[i] != '-' {
			formattedGovernmentId[counter] = governmentId[i]
			counter++
		}
	}
}

func parseSellerGovernmentId(governmentId []byte, formattedGovernmentId *[14]byte) {
	counter := 0

	for i := 0; i < len(governmentId); i++ {
		if governmentId[i] != '.' && governmentId[i] != '-' && governmentId[i] != '/' {
			formattedGovernmentId[counter] = governmentId[i]
			counter++
		}
	}
}

func byteSliceToLowerCase(slice *[]byte) {
	for i := 0; i < len(*slice); i++ {
		(*slice)[i] = byte(unicode.ToLower(rune((*slice)[i])))
	}
}
