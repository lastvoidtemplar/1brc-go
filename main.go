package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
	"time"
)

const FILE_PATH = "/1brc/measurements.txt"
const MAX_BUFFER_SIZE = 2 * 1024 * 1024
const CHANNEL_CAPACITY = 100

type Station struct {
	Name  []byte
	Max   int
	Min   int
	Sum   int
	Count int
}

// there is small buf with the reading try 1 2 4 megabytes buffer differnt number of lines
func ReadFile() <-chan []byte {
	chunkChannel := make(chan []byte, CHANNEL_CAPACITY)
	go func() {
		defer close(chunkChannel)
		file, err := os.Open(FILE_PATH)
		if err != nil {
			log.Fatalln(err)
		}
		defer file.Close()
		buffer := make([]byte, MAX_BUFFER_SIZE)
		remainingInd := 0
		for {
			n, err := file.Read(buffer[remainingInd:])
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatalln(err)
			}
			lastNewLineInd := remainingInd + n - 1
			for buffer[lastNewLineInd] != '\n' {
				lastNewLineInd--
			}

			payload := make([]byte, lastNewLineInd)
			copy(payload, buffer[:lastNewLineInd])
			chunkChannel <- payload
			remainingInd = copy(buffer, buffer[lastNewLineInd+1:])
		}
	}()
	return chunkChannel
}

func hash(name []byte) uint64 {
	var h uint64 = 5381
	for _, b := range name {
		h = (h << 5) + h + uint64(b)
	}
	return h
}
func parseLine(chunk []byte) ([]byte, int, int) {
	ind := 0
	for chunk[ind] != ';' {
		ind++
	}
	name := chunk[:ind]
	ind++
	negative := false
	if chunk[ind] == '-' {
		negative = true
		ind++
	}

	temp := int(chunk[ind] - '0')
	ind++
	if chunk[ind] == '.' {
		temp = temp*10 + int(chunk[ind+1]-'0')
		ind += 3
	} else {
		t := (chunk[ind]-'0')*10 + chunk[ind+2] - '0'
		temp = 100*temp + int(t)
		ind += 4
	}

	if negative {
		temp = -temp
	}
	return name, temp, ind
}

func updateStationsMap(stations map[uint64]*Station, name []byte, temp int) {
	id := hash(name)
	station, ok := stations[id]
	if ok {
		station.Max = max(station.Max, temp)
		station.Min = min(station.Min, temp)
		station.Sum += temp
		station.Count++
		return
	}
	stations[id] = &Station{
		Name:  name,
		Max:   temp,
		Min:   temp,
		Sum:   temp,
		Count: 1,
	}
}

func spwanWorker(chunkChannel <-chan []byte, mapChannel chan map[uint64]*Station, wg *sync.WaitGroup) {
	defer wg.Done()
	stations := make(map[uint64]*Station)
	for chunk := range chunkChannel {
		chunkSize := len(chunk)
		ind := 0
		for ind < chunkSize {
			name, temp, newind := parseLine(chunk[ind:])
			updateStationsMap(stations, name, temp)
			ind += newind
		}
	}
	mapChannel <- stations
}
func main() {
	profFile, err := os.Create("cpu.prof")
	if err != nil {
		panic(err)
	}
	defer profFile.Close()
	if err := pprof.StartCPUProfile(profFile); err != nil {
		panic(err)
	}
	defer pprof.StopCPUProfile()
	st := time.Now()
	chunkChannel := ReadFile()

	var wg sync.WaitGroup
	numberOfWorkers := runtime.NumCPU() - 1
	mapChannel := make(chan map[uint64]*Station, numberOfWorkers)
	for ind := 0; ind < numberOfWorkers; ind++ {
		go spwanWorker(chunkChannel, mapChannel, &wg)
		wg.Add(1)
	}
	wg.Wait()

	close(mapChannel)
	result := make(map[uint64]*Station)
	for stations := range mapChannel {
		for id, station := range stations {
			record, ok := result[id]
			if ok {
				record.Max = max(record.Max, station.Max)
				record.Min = min(record.Min, station.Min)
				record.Sum += station.Sum
				record.Count += station.Count
			} else {
				result[id] = station
			}
		}
	}

	for _, record := range result {
		fmt.Printf("%s -> Min = %.1f, Mean = %.1f, Max = %.1f\n", record.Name,
			float64(record.Min)/10,
			float64(record.Sum)/float64(record.Count)/10,
			float64(record.Max)/10,
		)
	}
	log.Println(time.Since(st))
}
