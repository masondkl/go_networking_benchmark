package client

import (
	"encoding/binary"
	"fmt"
	"net"
	"networking_benchmark/shared"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func StartClient() {
	fmt.Println("Starting client")
	addresses := strings.Split(os.Args[2], ",")
	totalAddresses := len(addresses)
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

	numClientOps := numOps / (numClients * totalAddresses)

	fmt.Println("Client operations:", numClientOps)

	connections := make([]net.Conn, numClients*totalAddresses)
	connected := 0
	for i := range totalAddresses {
		for range numClients {
			connection, err := net.Dial("tcp", addresses[i])
			if err != nil {
				panic(err)
			}
			err = connection.(*net.TCPConn).SetNoDelay(true)
			if err != nil {
				panic(err)
			}
			connections[connected] = connection
			connected++
		}
	}

	if connected != numClients*totalAddresses {
		panic(fmt.Sprintf("%d clients connected", connected))
	}

	fmt.Printf("Connected to %d clients\n", numClients*totalAddresses)
	completedOps := uint32(0)
	clientTimes := make([][]int, numClientOps)
	group := sync.WaitGroup{}
	group.Add(numClients * totalAddresses)
	start := time.Now().UnixMilli()

	go func() {
		total := numClients * totalAddresses * numClientOps
		barWidth := 50 // Width of the progress bar in characters

		for {
			completed := atomic.LoadUint32(&completedOps)
			//fmt.Printf("Completed - Total: %d,%d\n", completed, total)
			if completed >= uint32(total) {
				//fmt.Printf("Finished - Total: %d,%d\n", completed, total)
				break
			}

			// Calculate percentage
			percent := float64(completed) / float64(total) * 100
			filled := int(float64(barWidth) * float64(completed) / float64(total))

			// Build progress bar string
			bar := "[" + strings.Repeat("=", filled) +
				strings.Repeat(" ", barWidth-filled) + "]"

			// Print with carriage return to overwrite previous line
			fmt.Printf("\rProgress: %s %.2f%% (%d/%d)",
				bar, percent, completed, total)

			time.Sleep(200 * time.Millisecond) // Update 5 times per second
		}
		fmt.Println() // New line when done
	}()

	for i := range connections {
		clientTimes[i] = make([]int, numClientOps)
		go func(i int, connection net.Conn) {
			bytes := make([]byte, dataSize+4)
			for c := range numClientOps {
				binary.LittleEndian.PutUint32(bytes[:4], uint32(dataSize))

				begin := time.Now().UnixMicro()

				err := shared.Write(connection, bytes)
				if err != nil {
					panic(err)
				}

				err = shared.Read(connection, bytes[:4])
				if err != nil {
					panic(err)
				}

				amount := binary.LittleEndian.Uint32(bytes[:4])

				err = shared.Read(connection, bytes[:amount])
				if err != nil {
					panic(err)
				}

				end := time.Now().UnixMicro()
				clientTimes[i][c] = int(end - begin)
				//if c%10000 == 0 {
				//	fmt.Printf("[%d] %d\n", i, c)
				//}
				atomic.AddUint32(&completedOps, 1)
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

	fmt.Printf("\n0th percentile %dus\n", times[0])
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
	fmt.Printf("[dataSize=%d,numClients=%d,numOps=%d,totalTime=%dms, ops=%d/s]", dataSize, numClients*totalAddresses, numOps, end-start, int(float32(numOps)/(float32(end-start)/1000.0)))
	select {}
}
