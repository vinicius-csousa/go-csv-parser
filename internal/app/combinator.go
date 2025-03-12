package parser

import (
	"bufio"
	"fmt"
	"log"
	"os"
)

const csvHeader = "NU_DOCUMENTO;VALOR_NOMINAL_SUM;VALOR_NOMINAL_AVG;VALOR_NOMINAL_MAX;VALOR_NOMINAL_MIN;VALOR_PRESENTE_SUM;VALOR_PRESENTE_AVG;VALOR_PRESENTE_MAX;VALOR_PRESENTE_MIN;VALOR_AQUISICAO_SUM;VALOR_AQUISICAO_AVG; VALOR_AQUISICAO_MAX; VALOR_AQUISICAO_MIN\n"

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

		fmt.Fprintf(writer, "%d;%.2f;%.2f;%.2f;%.2f;%.2f;%.2f;%.2f;%.2f;%.2f;%.2f;%.2f;%.2f\n",
			aggregate.DocumentNumber, aggregate.SumNominalValue, aggregate.SumNominalValue/numRecords,
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
