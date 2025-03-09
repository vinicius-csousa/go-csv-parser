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
	registerPProf()
	run()
	profileMemory()
}

func run() {
	files := [10]string{
		//"data/test.csv",
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

	// Wait group to sync parsers
	var wg sync.WaitGroup
	// Channel to receive data from parsers
	ch := make(chan parser.Aggregate)

	// Call for goroutines to parse files
	for i := 0; i < len(files); i++ {
		wg.Add(1)
		go parser.Parse(files[i], ';', ch, &wg)
	}

	go func() {
		wg.Wait()
		defer close(ch)
	}()

	for aggregate := range ch {
		parser.Combine(&aggregate)
	}

	parser.Write()
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

func profileCPU() {
	f, err := os.Create("cpuprofile.out")
	if err != nil {
		log.Fatal("could not create CPU profile:", err)
	}
	defer f.Close()

	// Start CPU profiling
	if err := pprof.StartCPUProfile(f); err != nil {
		log.Fatal("could not start CPU profiling:", err)
	}
	defer pprof.StopCPUProfile()
}
