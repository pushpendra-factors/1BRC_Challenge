package main

import (
	"bufio"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
)

func main() {
	filepath := "./data/measurements-3.txt"
	run(filepath)
}

type cityTempInfo struct {
	min float64
	max float64
	avg float64
	cnt uint32
}

var cityValues = map[string][]float64{}
var cityMap = map[string]cityTempInfo{}

func run(filepath string) {

	// now write it to file
	file, _ := os.Create("cityMap.json")
	writer := bufio.NewWriter(file)

	readFileLineByLine(filepath)
	// now read all cities and compute

	for key, value := range cityMap {

		_, _ = writer.WriteString(fmt.Sprintf("%s=%f/%f/%f/%d\n", key, value.min, value.avg, value.max, value.cnt))

	}

}

func readFileLineByLine(filepath string) {

	file, _ := os.Open(filepath)
	defer file.Close()
	ChanOwner := func() <-chan string {
		resultStream := make(chan string, 100)
		go func() {
			defer close(resultStream)
			scanner := bufio.NewScanner(file)
			for scanner.Scan() {
				text := scanner.Text()
				resultStream <- text
			}
		}()

		return resultStream
	}

	result := ChanOwner()

	var wg sync.WaitGroup
	var mx sync.Mutex

	for text := range result { // here it blocks

		wg.Add(1)
		go func(text string) {
			defer wg.Done()
			index := strings.Index(text, ";")
			city_name := text[:index]
			city_temp, _ := strconv.ParseFloat(text[index+1:], 64)

			mx.Lock()

			v, ok := cityMap[city_name]
			if ok {
				avg_temp := (v.avg*float64(v.cnt) + city_temp) / float64((v.cnt + 1))
				cityMap[city_name] = cityTempInfo{math.Min(v.min, city_temp), math.Max(v.max, city_temp), avg_temp, v.cnt + 1}
			} else {
				cityMap[city_name] = cityTempInfo{city_temp, city_temp, city_temp, 1}
			}
			mx.Unlock()
		}(text)
	}
	wg.Wait()
}
