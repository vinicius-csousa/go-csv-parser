package parser

import (
	"bufio"
	"io"
	"log"
	"os"
	"strconv"
	"sync"
)

// Size of the batch of aggregates to be sent to the main goroutine
const BatchSize = 10000

// Fields from the csv along with their column indices
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
	MaxIndex:           16,
}

// DocumentNumber should be [x]byte
// SellerName can be [255]byte or []byte
type Aggregate struct {
	DocumentNumber      string
	SellerName          string
	MinFutureValue      float32
	MaxFutureValue      float32
	SumFutureValue      float32
	MinPresentValue     float32
	MaxPresentValue     float32
	SumPresentValue     float32
	MinAcquisitionValue float32
	MaxAcquisitionValue float32
	SumAcquisitionValue float32
	SellerGovernmentId  [11]byte
	AmountOfRecords     int8
}

type SimplifiedAggregate struct {
	SellerName         string
	DocumentNumber     string
	FutureValue        float32
	PresentValue       float32
	AcquisitionValue   float32
	SellerGovernmentId [11]byte
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
			}

		} else {
			if aggregate.DocumentNumber != "" {
				ch <- aggregate
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
			aggregate.SellerName = tmpAggregate.SellerName
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
			tmpAggregate.SellerName = string(line[startIndex:endIndex])
		case Fields.DocumentNumber:
			tmpAggregate.DocumentNumber = string(line[startIndex:endIndex])
		case Fields.FutureValue:
			val, _ := strconv.ParseFloat(string(line[startIndex:endIndex]), 32)
			tmpAggregate.FutureValue = float32(val)
		case Fields.PresentValue:
			val, _ := strconv.ParseFloat(string(line[startIndex:endIndex]), 32)
			tmpAggregate.PresentValue = float32(val)
		case Fields.AcquisitionValue:
			val, _ := strconv.ParseFloat(string(line[startIndex:endIndex]), 32)
			tmpAggregate.AcquisitionValue = float32(val)
		case Fields.SellerGovernmentId:
			parseGovernmentId(line[startIndex:endIndex], &tmpAggregate.SellerGovernmentId)
		}

		fieldNum++

		// Stop parsing once the most distant columns has been parsed
		if fieldNum > Fields.MaxIndex {
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
