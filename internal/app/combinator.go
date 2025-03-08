package parser

import (
	"bufio"
	"fmt"
	"log"
	"os"
)

const csvHeader = "DOCUMENT_NUMBER;SELLER_NAME;AMOUNT_RECORDS;AVG_FUTURE_VALUE;MAX_FUTURE_VALUE;MIN_FUTURE_VALUE;AVG_FUTURE_VALUE;MAX_FUTURE_VALUE;MIN_FUTURE_VALUE;AVG_PRESENT_VALUE;MAX_PRESENT_VALUE;AVG_ACQUISITION_VALUE; MAX_ACQUISITION_VALUE; MIN_ACQUISITION_VALUE\n"

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

	for _, aggregate := range combinedMap {
		writer.WriteString(generateLine(aggregate))
	}

	// Writes remaining buffered lines
	writer.Flush()
}

func generateLine(aggregate *Aggregate) string {
	numRecords := float32(aggregate.AmountOfRecords)

	return fmt.Sprintf("%d;%s;%d;%f;%f;%f;%f;%f;%f;%f;%f;%f\n",
		aggregate.DocumentNumber, aggregate.SellerName, aggregate.AmountOfRecords, aggregate.SumFutureValue/numRecords, aggregate.MaxFutureValue, aggregate.MinFutureValue,
		aggregate.SumPresentValue/numRecords, aggregate.MaxPresentValue, aggregate.MinPresentValue,
		aggregate.SumAcquisitionValue/numRecords, aggregate.MaxAcquisitionValue, aggregate.MinAcquisitionValue)
}
