package parser

import (
	"bufio"
	"fmt"
	"log"
	"os"
)

const csvHeader = "NU_DOCUMENTO;VALOR_NOMINAL_SUM;VALOR_NOMINAL_AVG;VALOR_NOMINAL_MAX;VALOR_NOMINAL_MIN;VALOR_PRESENTE_SUM;VALOR_PRESENTE_AVG;VALOR_PRESENTE_MAX;VALOR_PRESENTE_MIN;VALOR_AQUISICAO_SUM;VALOR_AQUISICAO_AVG; VALOR_AQUISICAO_MAX; VALOR_AQUISICAO_MIN\n"
const expectedNumberOfRows = 1200000

var AggregateSlice = make([]*Aggregate, expectedNumberOfRows)

func AddToSlice(aggregate *Aggregate, index *int) {
	if *index < len(AggregateSlice) {
		AggregateSlice[*index] = aggregate
		return
	}
	AggregateSlice = append(AggregateSlice, aggregate)
}

func RemoveNils() {
	for i := 0; i < len(AggregateSlice); i++ {
		if (AggregateSlice)[i] == nil {
			AggregateSlice = (AggregateSlice)[:i]
			break
		}
	}
}

func CreateOutputFile() (*os.File, *bufio.Writer) {
	file, err := os.Create("data/output.csv")
	if err != nil {
		log.Fatal("Could not create file for writing output\n.")
	}

	return file, bufio.NewWriter(file)
}

func CombineAndWrite(writer *bufio.Writer) {
	writer.WriteString(csvHeader)

	if len(AggregateSlice) == 0 {
		writer.Flush()
		return
	}

	var aggregate Aggregate
	for i := 0; i < len(AggregateSlice); i++ {
		if aggregate.DocumentNumber == AggregateSlice[i].DocumentNumber {
			aggregate.Sum(AggregateSlice[i])
		} else {
			if aggregate.DocumentNumber != 0 {
				writeLine(&aggregate, writer)
			}
			aggregate = *AggregateSlice[i]
		}
	}

	// Write last record
	writeLine(&aggregate, writer)

	// Write remaining buffered lines
	writer.Flush()
}

func writeLine(aggregate *Aggregate, writer *bufio.Writer) {
	numRecords := float32(aggregate.AmountOfRecords)

	fmt.Fprintf(writer, "%d;%.2f;%.2f;%.2f;%.2f;%.2f;%.2f;%.2f;%.2f;%.2f;%.2f;%.2f;%.2f\n",
		aggregate.DocumentNumber, aggregate.SumNominalValue, aggregate.SumNominalValue/numRecords,
		aggregate.MaxNominalValue, aggregate.MinNominalValue, aggregate.SumPresentValue, aggregate.SumPresentValue/numRecords, aggregate.MaxPresentValue,
		aggregate.MinPresentValue, aggregate.SumAcquisitionValue, aggregate.SumAcquisitionValue/numRecords, aggregate.MaxAcquisitionValue, aggregate.MinAcquisitionValue)
}
