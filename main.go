package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"strconv"
	"strings"
	"time"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func readFile(filePath string) []byte {
	data, err := os.ReadFile(filePath)
	check(err)
	return data
}

func main() {
	// Record the start time

	startTime := time.Now()

	cityMap := make(map[string]map[string]float64)
	data := readFile("./data/" + os.Args[1])

	allData := strings.Split(string(data), "\n")
	allData = allData[:len(allData)-1]
	i := 0

	for i < len(allData) {
		ele := allData[i]
		tuple := strings.Split(ele, ";")
		city := tuple[0]
		temp, _ := strconv.ParseFloat(tuple[1], 64)

		val, ok := cityMap[city]

		if ok {
			tmpVal := (val["avg"]*val["cnt"] + temp) / (val["cnt"] + 1)
			val["min"] = math.Min(val["min"], temp)
			val["max"] = math.Max(val["max"], temp) // Fixed: should be math.Max instead of math.Min
			val["avg"] = tmpVal
			val["cnt"] = val["cnt"] + 1
		} else {
			t := make(map[string]float64)
			t["min"] = temp
			t["max"] = temp
			t["avg"] = temp
			t["cnt"] = 1
			cityMap[city] = t
		}
		i++
	}

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
