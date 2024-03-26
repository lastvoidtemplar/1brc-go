package main

import (
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"runtime"
	"runtime/pprof"
	"sort"
	"sync"
	"time"
)

const FILE_PATH = "/1brc/measurements.txt"
const MAX_BUFFER_SIZE = 2 * 1024 * 1024
const CHANNEL_CAPACITY = 100
const NUMBER_OF_BUCKETS = 100000

type Station struct {
	Name  []byte
	Max   int
	Min   int
	Sum   int
	Count int
}

type Pair struct {
	key     uint64
	station *Station
}

type StationMap struct {
	buckets [][]Pair
	keys    []uint64
}

func (m *StationMap) Get(key uint64) (*Station, bool) {
	bucketInd := key % NUMBER_OF_BUCKETS
	bucket := m.buckets[bucketInd]
	if bucket == nil {
		return nil, false
	}

	for _, pair := range bucket {
		if pair.key == key {
			return pair.station, true
		}
	}
	return nil, false
}

func (m *StationMap) Set(key uint64, station *Station) {
	bucketInd := key % NUMBER_OF_BUCKETS
	m.keys = append(m.keys, key)
	m.buckets[bucketInd] = append(m.buckets[bucketInd], Pair{key: key, station: station})
}

func NewStationMap() *StationMap {
	return &StationMap{
		buckets: make([][]Pair, NUMBER_OF_BUCKETS),
		keys:    make([]uint64, 0, NUMBER_OF_BUCKETS),
	}
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

	temp := int(chunk[ind])
	ind++
	if chunk[ind] == '.' {
		temp = temp*10 + int(chunk[ind+1]) - int('0')*11
		ind += 3
	} else {
		t := int(chunk[ind])*10 + int(chunk[ind+2]) - int('0')*111
		temp = 100*temp + t
		ind += 4
	}

	if negative {
		temp = -temp
	}
	return name, temp, ind
}

func updateStationsMap(stations *StationMap, name []byte, temp int) {
	id := hash(name)
	station, ok := stations.Get(id)
	if ok {
		station.Max = max(station.Max, temp)
		station.Min = min(station.Min, temp)
		station.Sum += temp
		station.Count++
		return
	}
	stations.Set(id, &Station{
		Name:  name,
		Max:   temp,
		Min:   temp,
		Sum:   temp,
		Count: 1,
	})
}

func spwanWorker(chunkChannel <-chan []byte, mapChannel chan *StationMap, wg *sync.WaitGroup) {
	defer wg.Done()
	stations := NewStationMap()
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
	mapChannel := make(chan *StationMap, numberOfWorkers)
	for ind := 0; ind < numberOfWorkers; ind++ {
		go spwanWorker(chunkChannel, mapChannel, &wg)
		wg.Add(1)
	}
	wg.Wait()

	close(mapChannel)
	result := NewStationMap()
	for stations := range mapChannel {
		for _, id := range stations.keys {
			station, _ := stations.Get(id)
			record, ok := result.Get(id)
			if ok {
				record.Max = max(record.Max, station.Max)
				record.Min = min(record.Min, station.Min)
				record.Sum += station.Sum
				record.Count += station.Count
			} else {
				result.Set(id, station)
			}
		}
	}

	sort.Slice(result.keys, func(i, j int) bool {
		first, _ := result.Get(result.keys[i])
		second, _ := result.Get(result.keys[j])
		return string(first.Name) < string(second.Name)
	})

	for _, id := range result.keys {
		record, _ := result.Get(id)
		fmt.Printf("%s=%.1f/%.1f/%.1f\n", record.Name,
			float64(record.Min)/10,
			math.Round(float64(record.Sum)/float64(record.Count))/10,
			float64(record.Max)/10,
		)
	}

	log.Println(time.Since(st))
}
