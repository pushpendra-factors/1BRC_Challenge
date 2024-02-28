package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"strings"
	"sync"
	"time"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

var cityMap = make(map[string]map[string]float64)

func main() {
	// Record the start time
	startTime := time.Now()

	readFileAndBuildMap("./data/measurements-1000000.txt")

	// Record the end time
	endTime := time.Now()

	// Calculate and print the elapsed time
	elapsedTime := endTime.Sub(startTime)
	fmt.Printf("Execution time: %s\n", elapsedTime)

	// Write cityMap to a file
	err := writeMapToFile(cityMap, "cityMap.json")
	if err != nil {
		fmt.Println("Error writing to file:", err)
	}
}
func buildMap(tv map[string]float64, ok bool, city_temp float64, city_name string, mx *sync.Mutex) {
	mx.Lock()
	defer mx.Unlock()
	if ok {
		cityMap[city_name]["min"] = math.Min(tv["min"], city_temp)
		cityMap[city_name]["max"] = math.Max(tv["min"], city_temp)
		cityMap[city_name]["avg"] = (tv["avg"]*tv["cnt"] + city_temp) / (tv["cnt"] + 1)
		cityMap[city_name]["min"] = tv["cnt"] + 1

	} else {

		t := make(map[string]float64)
		t["min"] = city_temp
		t["max"] = city_temp
		t["avg"] = city_temp
		t["cnt"] = 1

		cityMap[city_name] = t
	}
}
func readFileAndBuildMap(filePath string) {
	file, err := os.Open(filePath)
	if err != nil {
		panic(err)
	}

	var wg sync.WaitGroup
	var mx sync.Mutex
	var result []string
	defer file.Close()
	scanner := bufio.NewScanner(file)
	tmpMap := make(map[string][]float64)
	for scanner.Scan() {
		text := scanner.Text()

		index := strings.Index(text, ";")
		key := text[:index]
		val := convertStringToFloat(text[index+1:])

		tmpMap[key] = append(tmpMap[key], val)
	}

	for key, vals := range tmpMap {
		wg.Add(1)

		go func(vals []float64, key string) {
			defer wg.Done()

			var ans float64
			for _, e := range vals {
				ans += e
			}
			mx.Lock()
			result = append(result, fmt.Sprintf("%s=%.1f", key, ans/float64(len(vals))))

			mx.Unlock()
		}(vals, key)
	}

	wg.Wait()
}
func writeMapToFile(data map[string]map[string]float64, filename string) error {
	// Marshal the map to JSON
	jsonData, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}

	// Write JSON data to file
	err = ioutil.WriteFile(filename, jsonData, 0644)
	if err != nil {
		return err
	}

	fmt.Println("Data written to", filename)
	return nil
}
