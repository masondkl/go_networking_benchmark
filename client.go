package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"
)

func client() {
	fmt.Println("Starting client")
	address := os.Args[2]
	dataSize, err := strconv.Atoi(os.Args[3])
	if err != nil {
		panic(err)
	}

	numOps, err := strconv.Atoi(os.Args[4])
	if err != nil {
		panic(err)
	}

	numClients, err := strconv.Atoi(os.Args[5])
	if err != nil {
		panic(err)
	}

	numClientOps := numOps / numClients

	fmt.Println("Client operations:", numClientOps)

	connections := make([]net.Conn, numClients)
	for i := 0; i < numClients; i++ {
		connection, err := net.Dial("tcp", address)
		if err != nil {
			panic(err)
		}
		err = connection.(*net.TCPConn).SetNoDelay(true)
		if err != nil {
			panic(err)
		}
		connections[i] = connection
	}

	fmt.Printf("Connected to %d clients\n", numClients)
	clientTimes := make([][]int, numClientOps)
	group := sync.WaitGroup{}
	group.Add(numClients)
	start := time.Now().UnixMilli()
	for i := range numClients {
		clientTimes[i] = make([]int, numClientOps)
		go func(i int, connection net.Conn) {
			bytes := make([]byte, dataSize+4)
			for c := range numClientOps {
				binary.LittleEndian.PutUint32(bytes[:4], uint32(dataSize))

				begin := time.Now().UnixMicro()

				err := Write(connection, bytes)
				if err != nil {
					panic(err)
				}

				err = Read(connection, bytes[:4])
				if err != nil {
					panic(err)
				}

				amount := binary.LittleEndian.Uint32(bytes[:4])

				err = Read(connection, bytes[:amount])
				if err != nil {
					panic(err)
				}

				end := time.Now().UnixMicro()
				clientTimes[i][c] = int(end - begin)
				if c%10000 == 0 {
					fmt.Printf("[%d] %d\n", i, c)
				}
			}
			group.Done()
			//err := connection.Close()
			//if err != nil {
			//	panic(err)
			//}
		}(i, connections[i])
	}
	group.Wait()

	end := time.Now().UnixMilli()

	times := make([]int, 0)
	for i := range numClients {
		times = append(times, clientTimes[i]...)
	}
	sort.Ints(times)
	length := len(times)

	fmt.Printf("0th percentile %dus\n", times[0])
	fmt.Printf("10th percentile %dus\n", times[int(float32(length)*0.1)])
	fmt.Printf("20th percentile %dus\n", times[int(float32(length)*0.2)])
	fmt.Printf("30th percentile %dus\n", times[int(float32(length)*0.3)])
	fmt.Printf("40th percentile %dus\n", times[int(float32(length)*0.4)])
	fmt.Printf("50th percentile %dus\n", times[int(float32(length)*0.5)])
	fmt.Printf("60th percentile %dus\n", times[int(float32(length)*0.6)])
	fmt.Printf("70th percentile %dus\n", times[int(float32(length)*0.7)])
	fmt.Printf("80th percentile %dus\n", times[int(float32(length)*0.8)])
	fmt.Printf("90th percentile %dus\n", times[int(float32(length)*0.9)])
	fmt.Printf("100th percentile %dus\n", times[length-1])
	fmt.Printf("[numClients=%d,numOps=%d,totalTime=%dms, ops=%d/s]", numClients, numOps, end-start, int(float32(numOps)/(float32(end-start)/1000.0)))
	select {}
}
