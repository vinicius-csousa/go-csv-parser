package main

import (
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime/pprof"
	"sync"

	parser "github.com/vinicius-csousa/go-csv-parser/internal/app"
)

func main() {
	// fCPU, err := os.Create("cpu.pprof")
	// if err != nil {
	// 	log.Fatal("could not create CPU profile: ", err)
	// }
	// defer fCPU.Close() // error handling omitted for example
	// if err := pprof.StartCPUProfile(fCPU); err != nil {
	// 	log.Fatal("could not start CPU profile: ", err)
	// }
	// defer pprof.StopCPUProfile()
	registerPProf()
	run()
	profileMemory()
}

func run() {
	files := [10]string{
		// "data/test.csv",
		"data/58148845000109_Estoque_PICPAY FGTS FIDC_001.csv",
		"data/58148845000109_Estoque_PICPAY FGTS FIDC_0039.csv",
		"data/58148845000109_Estoque_PICPAY FGTS FIDC_0040.csv",
		"data/58148845000109_Estoque_PICPAY FGTS FIDC_0041.csv",
		"data/58148845000109_Estoque_PICPAY FGTS FIDC_0042.csv",
		"data/58148845000109_Estoque_PICPAY FGTS FIDC_0043.csv",
		"data/58148845000109_Estoque_PICPAY FGTS FIDC_0044.csv",
		"data/58148845000109_Estoque_PICPAY FGTS FIDC_0045.csv",
		"data/58148845000109_Estoque_PICPAY FGTS FIDC_0046.csv",
		"data/58148845000109_Estoque_PICPAY FGTS FIDC_0047.csv",
	}

	// WaitGroup to sync parsers
	var wg sync.WaitGroup
	// Channel to receive data from parsers
	ch := make(chan parser.Aggregate)

	// Check if any filter is requested
	filter := parser.ParseFilter()

	// Call for goroutines to parse files
	for i := 0; i < len(files); i++ {
		wg.Add(1)
		go parser.Parse(files[i], ';', ch, &wg, filter)
	}

	go func() {
		wg.Wait()
		defer close(ch)
	}()

	file, writer := parser.CreateOutputFile()
	defer file.Close()

	// Add each aggregate to a global slice
	counter := 0
	for aggregate := range ch {
		parser.AddToSlice(&aggregate, &counter)
		counter++
	}

	// Remove nils, sort slice, aggregate and write output
	parser.RemoveNils()
	parser.QuickSort(parser.AggregateSlice)
	parser.CombineAndWrite(writer)
}

func registerPProf() {
	go func() {
		err := http.ListenAndServe("127.0.0.1:6060", nil)
		if err != nil {
			return
		}
	}()
}

func profileMemory() {
	f, err := os.Create("memprofile.out")
	if err != nil {
		log.Fatalf("could not create memory profile: %v", err)
	}
	defer f.Close()

	if err := pprof.WriteHeapProfile(f); err != nil {
		log.Fatalf("could not write memory profile: %v", err)
	}
}
