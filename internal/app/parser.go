package parser

import (
	"bufio"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
)

// Fields from the csv along with their column indices in the csv
var Fields = struct {
	SellerName         int8
	SellerGovernmentId int8
	FutureValue        int8
	PresentValue       int8
	AcquisitionValue   int8
	DocumentNumber     int8
	MaxIndex           int8
}{
	SellerName:         6,
	SellerGovernmentId: 7,
	FutureValue:        9,
	PresentValue:       10,
	AcquisitionValue:   11,
	DocumentNumber:     16,
}

type Aggregate struct {
	SellerName          []byte   // 24 bytes
	MinFutureValue      float32  // 4 bytes
	MaxFutureValue      float32  // 4 bytes
	SumFutureValue      float32  // 4 bytes
	MinPresentValue     float32  // 4 bytes
	MaxPresentValue     float32  // 4 bytes
	SumPresentValue     float32  // 4 bytes
	MinAcquisitionValue float32  // 4 bytes
	MaxAcquisitionValue float32  // 4 bytes
	SumAcquisitionValue float32  // 4 bytes
	DocumentNumber      int32    // 4 bytes
	SellerGovernmentId  [11]byte // 11 bytes
	AmountOfRecords     int8     // 1 byte
}

type SimplifiedAggregate struct {
	SellerName         []byte   // 24 bytes
	FutureValue        float32  // 4 bytes
	PresentValue       float32  // 4 bytes
	AcquisitionValue   float32  // 4 bytes
	DocumentNumber     int32    // 4 bytes
	SellerGovernmentId [11]byte // 11 bytes
}

func (aggregate *Aggregate) Sum(newAggregate *Aggregate) {
	aggregate.SumFutureValue += newAggregate.SumFutureValue
	aggregate.MaxFutureValue = max(aggregate.MaxFutureValue, newAggregate.MaxFutureValue)
	aggregate.MinFutureValue = min(aggregate.MinFutureValue, newAggregate.MinFutureValue)

	aggregate.SumPresentValue += newAggregate.SumPresentValue
	aggregate.MaxPresentValue = max(aggregate.MaxPresentValue, newAggregate.MaxPresentValue)
	aggregate.MinPresentValue = min(aggregate.MinPresentValue, newAggregate.MinPresentValue)

	aggregate.SumAcquisitionValue += newAggregate.SumAcquisitionValue
	aggregate.MaxAcquisitionValue = max(aggregate.MaxAcquisitionValue, newAggregate.MaxAcquisitionValue)
	aggregate.MinAcquisitionValue = min(aggregate.MinAcquisitionValue, newAggregate.MinAcquisitionValue)

	aggregate.AmountOfRecords += newAggregate.AmountOfRecords
}

func Parse(filepath string, separator byte, ch chan Aggregate, wg *sync.WaitGroup) {
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

		parseLine(line, &tmpAggregate, separator)

		if tmpAggregate.DocumentNumber == aggregate.DocumentNumber {
			aggregate.SumFutureValue += tmpAggregate.FutureValue
			aggregate.MaxFutureValue = max(aggregate.MaxFutureValue, tmpAggregate.FutureValue)
			aggregate.MinFutureValue = min(aggregate.MinFutureValue, tmpAggregate.FutureValue)

			aggregate.SumPresentValue += tmpAggregate.PresentValue
			aggregate.MaxPresentValue = max(aggregate.MaxPresentValue, tmpAggregate.PresentValue)
			aggregate.MinPresentValue = min(aggregate.MinPresentValue, tmpAggregate.PresentValue)

			aggregate.SumAcquisitionValue += tmpAggregate.AcquisitionValue
			aggregate.MaxAcquisitionValue = max(aggregate.MaxAcquisitionValue, tmpAggregate.AcquisitionValue)
			aggregate.MinAcquisitionValue = min(aggregate.MinAcquisitionValue, tmpAggregate.AcquisitionValue)

			aggregate.AmountOfRecords++

			// Append aggregate to slice if the file has ended
			if !shouldRead {
				ch <- aggregate
				releaseOwnership(&aggregate.SellerName)
			}

		} else {
			if aggregate.DocumentNumber != 0 {
				ch <- aggregate
				releaseOwnership(&aggregate.SellerName)
			}

			aggregate.SumFutureValue = tmpAggregate.FutureValue
			aggregate.MaxFutureValue = tmpAggregate.FutureValue
			aggregate.MinFutureValue = tmpAggregate.FutureValue

			aggregate.SumPresentValue = tmpAggregate.PresentValue
			aggregate.MaxPresentValue = tmpAggregate.PresentValue
			aggregate.MinPresentValue = tmpAggregate.PresentValue

			aggregate.SumAcquisitionValue = tmpAggregate.AcquisitionValue
			aggregate.MaxAcquisitionValue = tmpAggregate.AcquisitionValue
			aggregate.MinAcquisitionValue = tmpAggregate.AcquisitionValue

			aggregate.AmountOfRecords = 1
			aggregate.DocumentNumber = tmpAggregate.DocumentNumber
			aggregate.SellerGovernmentId = tmpAggregate.SellerGovernmentId
			copyBytes(&aggregate.SellerName, tmpAggregate.SellerName)
		}
	}
}

func parseLine(line []byte, tmpAggregate *SimplifiedAggregate, separator byte) {
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
		case Fields.DocumentNumber:
			val, _ := strconv.ParseInt(string(line[startIndex:endIndex]), 0, 32)
			tmpAggregate.DocumentNumber = int32(val)
		case Fields.FutureValue:
			val, err := strconv.ParseFloat(strings.ReplaceAll(string(line[startIndex:endIndex]), ",", ""), 32)
			if err != nil {
				log.Fatal(err.Error())
			}
			tmpAggregate.FutureValue = float32(val)
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
		case Fields.SellerGovernmentId:
			parseGovernmentId(line[startIndex:endIndex], &tmpAggregate.SellerGovernmentId)
		}

		fieldNum++

		// Stop parsing once the most distant columns has been parsed
		if fieldNum > Fields.DocumentNumber {
			return
		}

		startIndex = endIndex + 1
	}
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
