package parser

import (
	"bufio"
	"fmt"
	"log"
	"os"
)

const csvHeader = "DOCUMENT_NUMBER;AMOUNT_RECORDS;SUM_NOMINAL_VALUE;AVG_NOMINAL_VALUE;MAX_NOMINAL_VALUE;MIN_NOMINAL_VALUE;SUM_PRESENT_VALUE;AVG_PRESENT_VALUE;MAX_PRESENT_VALUE;MIN_PRESENT_VALUE;SUM_ACQUISITION_VALUE;AVG_ACQUISITION_VALUE; MAX_ACQUISITION_VALUE; MIN_ACQUISITION_VALUE\n"

var aggregateMap = make(map[int32]*Aggregate)

func Combine(aggregate *Aggregate) {
	if _, ok := aggregateMap[aggregate.DocumentNumber]; ok {
		aggregateMap[aggregate.DocumentNumber].Sum(aggregate)
	} else {
		aggregateMap[aggregate.DocumentNumber] = aggregate
	}
}

func Write(writer *bufio.Writer) {
	fmt.Fprintf(writer, "%s", csvHeader)

	var numRecords float32
	for _, aggregate := range aggregateMap {
		numRecords = float32(aggregate.AmountOfRecords)

		fmt.Fprintf(writer, "%d;%d;%.2f;%.2f;%.2f;%.2f;%.2f;%.2f;%.2f;%.2f;%.2f;%.2f;%.2f;%.2f\n",
			aggregate.DocumentNumber, aggregate.AmountOfRecords, aggregate.SumNominalValue, aggregate.SumNominalValue/numRecords,
			aggregate.MaxNominalValue, aggregate.MinNominalValue, aggregate.SumPresentValue, aggregate.SumPresentValue/numRecords, aggregate.MaxPresentValue,
			aggregate.MinPresentValue, aggregate.SumAcquisitionValue, aggregate.SumAcquisitionValue/numRecords, aggregate.MaxAcquisitionValue, aggregate.MinAcquisitionValue)

		delete(aggregateMap, aggregate.DocumentNumber)
	}

	// Writes remaining buffered lines
	writer.Flush()
}

func CreateOutputFile() (*os.File, *bufio.Writer) {
	file, err := os.Create("data/output.csv")
	if err != nil {
		log.Fatal("Could not create file for writing output\n.")
	}

	return file, bufio.NewWriter(file)
}
