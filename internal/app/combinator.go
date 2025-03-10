package parser

import (
	"bufio"
	"fmt"
	"log"
	"os"
)

const csvHeader = "DOCUMENT_NUMBER;SELLER_NAME;AMOUNT_RECORDS;SUM_NOMINAL_VALUE;AVG_NOMINAL_VALUE;MAX_NOMINAL_VALUE;MIN_NOMINAL_VALUE;SUM_PRESENT_VALUE;AVG_PRESENT_VALUE;MAX_PRESENT_VALUE;MIN_PRESENT_VALUE;SUM_ACQUISITION_VALUE;AVG_ACQUISITION_VALUE; MAX_ACQUISITION_VALUE; MIN_ACQUISITION_VALUE\n"

var combinedMap = make(map[int32]*Aggregate)

func Combine(aggregate *Aggregate) {
	if _, ok := combinedMap[aggregate.DocumentNumber]; ok {
		combinedMap[aggregate.DocumentNumber].Sum(aggregate)
	} else {
		combinedMap[aggregate.DocumentNumber] = aggregate
	}
}

func Write() {
	file, err := os.Create("data/output.csv")
	if err != nil {
		log.Fatal("Could not create file for writing output\n.")
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	writer.WriteString(csvHeader)

	var numRecords float32
	for _, aggregate := range combinedMap {
		numRecords = float32(aggregate.AmountOfRecords)

		fmt.Fprintf(writer, "%d;%s;%d;%.2f;%.2f;%.2f;%.2f;%.2f;%.2f;%.2f;%.2f;%.2f;%.2f;%.2f;%.2f\n",
			aggregate.DocumentNumber, aggregate.SponsorName, aggregate.AmountOfRecords, aggregate.SumNominalValue, aggregate.SumNominalValue/numRecords,
			aggregate.MaxNominalValue, aggregate.MinNominalValue, aggregate.SumPresentValue, aggregate.SumPresentValue/numRecords, aggregate.MaxPresentValue,
			aggregate.MinPresentValue, aggregate.SumAcquisitionValue, aggregate.SumAcquisitionValue/numRecords, aggregate.MaxAcquisitionValue, aggregate.MinAcquisitionValue)

		delete(combinedMap, aggregate.DocumentNumber)
	}

	// Writes remaining buffered lines
	writer.Flush()
}
