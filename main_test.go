package main

import (
	"fmt"
	"testing"
)

func Benchmark(b *testing.B) {
	filepath := "./data/measurements-100000000.txt"
	//run(filepath)
	evaluate(filepath)
	fmt.Print("SD")
}
