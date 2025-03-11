package parser

import (
	"bufio"
	"bytes"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
)

// Fields from the csv along with their column indices in the csv
var Fields = struct {
	SellerName          int8
	SponsorName         int8
	SponsorGovernmentId int8
	NominalValue        int8
	PresentValue        int8
	AcquisitionValue    int8
	DocumentNumber      int8
}{
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
	SponsorName      []byte  // 24 bytes
	SellerName       []byte  // 24 bytes
	NominalValue     float32 // 4 bytes
	PresentValue     float32 // 4 bytes
	AcquisitionValue float32 // 4 bytes
	DocumentNumber   int32   // 4 bytes
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
	shouldRead := true
	for shouldRead {
		// Read csv line by line
		line, err := reader.ReadSlice('\n')

		if err != nil && err != io.EOF {
			log.Fatalf("Failed to read file: %s", filepath)
		}
		if err == io.EOF {
			shouldRead = false
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

			// Send aggregate if the file has ended
			if !shouldRead && shouldSend {
				ch <- aggregate
			}

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
		case Fields.SellerName:
			tmpAggregate.SellerName = line[startIndex:endIndex]
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
	val, ok := filter.(*NameFilter)
	if ok {
		if bytes.Equal(val.Field, []byte("seller")) {
			return filter.Filter(aggregate.SellerName)
		}
		if bytes.Equal(val.Field, []byte("sponsor")) {
			return filter.Filter(aggregate.SponsorName)
		}
	}
	return true
}

// Remove special characters from governmentIds
func parseGovernmentId(governmentId []byte, formattedGovernmentId *[11]byte) {
	counter := 0

	for i := 0; i < len(governmentId); i++ {
		if governmentId[i] != '.' && governmentId[i] != '-' {
			formattedGovernmentId[counter] = governmentId[i]
			counter++
		}
	}
}

func copyBytes(receiver *[]byte, sender []byte) {
	if len(sender) > cap(*receiver) {
		*receiver = make([]byte, len(sender))
	} else {
		*receiver = (*receiver)[:len(sender)]
	}

	copy(*receiver, sender)
}

/*
* Removes the reference of a slice (needed for []byte fields when passing
* from the parsers to the consumer goroutine)
 */
func releaseOwnership(slice *[]byte) {
	*slice = nil
}
