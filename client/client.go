package client

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net"
	"networking_benchmark/shared"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var completedOps uint32

type Client struct {
	Addresses      []string
	TotalAddresses int
	DataSize       int
	NumOps         int
	ReadRatio      float64
	NumClients     int
	NumClientOps   int
	IsReadMemory   bool
	IsWriteMemory  bool

	Connections  []net.Conn
	Keys         [][]byte
	WarmupValues [][]byte
	UpdateValues [][]byte
}

//var (
//	nodeIndex           = flag.Int("node", 0, "node index")
//	poolDataSize        = flag.Int("pool-data-size", 0, "pool data size")
//	poolWarmupSize      = flag.Int("pool-warmup-size", 0, "pool warmup size")
//	numPeerConnections  = flag.Int("peer-connections", 0, "number of peer connections")
//	peerListenAddress   = flag.String("peer-listen", "", "peer listen address")
//	clientListenAddress = flag.String("client-listen", "", "client listen address")
//	peerAddressesString = flag.String("peer-addresses", "", "comma-separated peer addresses")
//	walFileCount        = flag.Int("wal-file-count", 0, "wal file count")
//	manual              = flag.String("manual", "none", "fsync, dsync, or none")
//	flags               = flag.String("flags", "none", "fsync, dsync, sync, none")
//	memory              = flag.Bool("memory", false, "use memory")
//)

var ()

func StartClient(args []string) {
	fs := flag.NewFlagSet("client", flag.ExitOnError)
	var (
		addressesCSV  string
		dataSize      int
		numOps        int
		readRatio     float64
		numClients    int
		isReadMemory  bool
		isWriteMemory bool
		isFindLeader  bool
	)

	fs.StringVar(&addressesCSV, "addresses", "", "comma‑separated TCP addresses")
	fs.IntVar(&dataSize, "data-size", 0, "payload size in bytes")
	fs.IntVar(&numOps, "ops", 0, "total operations per client")
	fs.Float64Var(&readRatio, "read-ratio", 0.5, "fraction of reads 0–1")
	fs.IntVar(&numClients, "clients", 1, "number of concurrent clients")
	fs.BoolVar(&isReadMemory, "read-mem", false, "read from memory")
	fs.BoolVar(&isWriteMemory, "write-mem", false, "write to memory")
	fs.BoolVar(&isFindLeader, "find-leader", false, "check the provided addresses and find the leader node")

	err := fs.Parse(args)
	if err != nil {
		panic(err)
	}

	if addressesCSV == "" {
		log.Fatalf("-addresses is required")
	}
	if dataSize <= 0 {
		log.Fatalf("-data-size must be > 0")
	}
	if numOps <= 0 {
		log.Fatalf("-ops must be > 0")
	}
	if readRatio < 0 || readRatio > 1 {
		log.Fatalf("-read-ratio must be between 0 and 1")
	}
	if numClients <= 0 {
		log.Fatalf("-clients must be > 0")
	}

	addresses := strings.Split(addressesCSV, ",")
	totalAddresses := len(addresses)

	if isFindLeader {
		connections := make([]net.Conn, totalAddresses)
		leaderGroup := sync.WaitGroup{}
		leaderIndex := -1
		leaderGroup.Add(1)
		for i := range totalAddresses {
			var connection net.Conn
			for {
				connection, err = net.Dial("tcp", addresses[i])
				if err != nil {
					time.Sleep(250 * time.Millisecond)
					continue
				}
				break
			}
			connections[i] = connection
			go func() {
				buffer := make([]byte, 5)

				for {
					binary.LittleEndian.PutUint32(buffer[:4], 1)
					buffer[4] = shared.OP_LEADER

					err = shared.Write(connection, buffer[:])
					if err != nil {
						break
					}
					err = shared.Read(connection, buffer[:1])
					if err != nil {
						break
					}
					if buffer[0] == byte(1) {
						leaderGroup.Done()
						leaderIndex = i
						break
					}
					time.Sleep(250 * time.Millisecond)
				}
			}()
		}
		leaderGroup.Wait()
		for i := range connections {
			connections[i].Close()
		}

		leaderAddress := addresses[leaderIndex]
		addresses = make([]string, 1)
		addresses[0] = leaderAddress
		totalAddresses = 1
		fmt.Printf("%d\n", leaderIndex)
	}

	numClientOps := numOps / (numClients * totalAddresses)
	client := &Client{
		Addresses:      addresses,
		TotalAddresses: totalAddresses,
		DataSize:       dataSize,
		NumOps:         numClientOps * numClients * totalAddresses,
		ReadRatio:      readRatio,
		NumClients:     numClients,
		NumClientOps:   numClientOps,
		Connections:    make([]net.Conn, numClients*totalAddresses),
		Keys:           make([][]byte, numOps),
		WarmupValues:   make([][]byte, numOps),
		UpdateValues:   make([][]byte, numOps),
		IsReadMemory:   isReadMemory,
		IsWriteMemory:  isWriteMemory,
	}

	for i := 0; i < numOps; i++ {
		client.Keys[i] = []byte(fmt.Sprintf("key%d", i))
		client.WarmupValues[i] = []byte(strings.Repeat("x", dataSize))
		client.UpdateValues[i] = []byte(strings.Repeat("z", dataSize))
	}

	client.connect()
	client.warmup()
	client.benchmark()
}

func (client *Client) connect() {
	connected := 0
	for i := range client.TotalAddresses {
		for range client.NumClients {
			connection, err := net.Dial("tcp", client.Addresses[i])
			if err != nil {
				panic(err)
			}
			err = connection.(*net.TCPConn).SetNoDelay(true)
			if err != nil {
				panic(err)
			}
			client.Connections[connected] = connection
			connected++
		}
	}

	if connected != client.NumClients*client.TotalAddresses {
		panic(fmt.Sprintf("%d clients connected", connected))
	}
}

func (client *Client) benchmark() {
	var benchmarkBar sync.WaitGroup
	benchmarkBar.Add(1)
	completedOps = 0
	go func() {
		defer benchmarkBar.Done()
		progressBar("Benchmark", client.NumOps)
	}()

	clientReadTimes := make([]int, client.NumOps)
	clientWriteTimes := make([]int, client.NumOps)
	clientTimes := make([]int, client.NumOps)
	group := sync.WaitGroup{}
	group.Add(client.NumClients * client.TotalAddresses)

	start := time.Now().UnixMilli()

	var count uint32
	var readCount uint32
	var writeCount uint32

	for i := range client.Connections {
		go func(i int, connection net.Conn) {
			buffer := make([]byte, client.DataSize+128)
			for c := range client.NumClientOps {

				isRead := rand.Float64() < client.ReadRatio

				key := client.Keys[i*client.NumClientOps+c]
				warmupValue := client.WarmupValues[i*client.NumClientOps+c]
				updateValue := client.UpdateValues[i*client.NumClientOps+c]
				value := warmupValue

				var totalLength int
				if !isRead {
					value = updateValue
					totalLength = 13 + len(key) + len(value)

					if client.IsWriteMemory {
						buffer[4] = shared.OP_WRITE_MEMORY
					} else {
						buffer[4] = shared.OP_WRITE
					}
					binary.LittleEndian.PutUint32(buffer[9+len(key):], uint32(len(value)))
					copy(buffer[13+len(key):], value)
				} else {
					totalLength = 9 + len(key)
					if client.IsReadMemory {
						buffer[4] = shared.OP_READ_MEMORY
					} else {
						buffer[4] = shared.OP_READ
					}
				}

				binary.LittleEndian.PutUint32(buffer[:4], uint32(totalLength-4))
				binary.LittleEndian.PutUint32(buffer[5:9], uint32(len(key)))
				copy(buffer[9:], key)

				begin := time.Now().UnixMicro()
				err := shared.Write(connection, buffer[:totalLength])
				if err != nil {
					panic(err)
				}

				err = shared.Read(connection, buffer[:4])
				if err != nil {
					panic(err)
				}

				amount := binary.LittleEndian.Uint32(buffer[:4])

				err = shared.Read(connection, buffer[:amount])
				if err != nil {
					panic(err)
				}
				if !isRead {
					if client.IsWriteMemory && buffer[0] != shared.OP_WRITE_MEMORY {
						panic(fmt.Sprintf("Memory Write operation failed, expected %v, got %v", shared.OP_WRITE_MEMORY, buffer[0]))
					}
					if !client.IsWriteMemory && buffer[0] != shared.OP_WRITE {
						panic(fmt.Sprintf("Disk Write operation failed, expected %v, got %v", shared.OP_WRITE, buffer[0]))
					}
				} else {
					if client.IsReadMemory && buffer[0] != shared.OP_READ_MEMORY {
						panic(fmt.Sprintf("Memory Read operation failed, expected %v, got %v", shared.OP_READ_MEMORY, buffer[0]))
					}

					if !client.IsReadMemory && buffer[0] != shared.OP_READ {
						panic(fmt.Sprintf("Disk Read operation failed, expected %v, got %v", shared.OP_READ, buffer[0]))
					}
					valueSize := binary.LittleEndian.Uint32(buffer[1:5])
					//fmt.Printf("What value size: %d\n", valueSize)
					if valueSize == 0 {
						panic(fmt.Sprintf("GOT A NULL READ FOR KEY: %s", string(key)))
					} else {
						//warmup-xx, returned- x
						//if !bytes.Equal(buffer[5:5+valueSize], value) {
						//	panic(fmt.Sprintf("GOT A WRONG VALUE FOR KEY: %s, warmup-%s, returned-%s", string(key), string(value), string(buffer[4:4+valueSize])))
						//}
					}
				}

				end := time.Now().UnixMicro()

				nextCount := atomic.AddUint32(&count, 1)
				clientTimes[nextCount-1] = int(end - begin)

				if isRead {
					nextReadCount := atomic.AddUint32(&readCount, 1)
					clientReadTimes[nextReadCount-1] = int(end - begin)
				} else if !isRead {
					nextWriteCount := atomic.AddUint32(&writeCount, 1)
					clientWriteTimes[nextWriteCount-1] = int(end - begin)
				}
				atomic.AddUint32(&completedOps, 1)
			}
			group.Done()
		}(i, client.Connections[i])
	}

	group.Wait()

	end := time.Now().UnixMilli()
	benchmarkBar.Wait()
	client.displayResults(start, end, clientTimes, clientWriteTimes, clientReadTimes, int(count), int(writeCount), int(readCount))
}

func (client *Client) warmup() {
	warmup := sync.WaitGroup{}
	warmup.Add(client.NumClients * client.TotalAddresses)

	var warmupBar sync.WaitGroup
	warmupBar.Add(1)
	go func() {
		defer warmupBar.Done()
		progressBar("Warmup", client.NumOps+(client.NumClients*client.TotalAddresses))
	}()

	for i := range client.Connections {
		go func(i int, connection net.Conn) {
			buffer := make([]byte, client.DataSize+128)
			for c := range client.NumClientOps {
				key := client.Keys[i*client.NumClientOps+c]
				warmupValue := client.WarmupValues[i*client.NumClientOps+c]
				value := warmupValue
				totalLength := 13 + len(key) + len(value)
				if client.IsWriteMemory {
					buffer[4] = shared.OP_WRITE_MEMORY
				} else {
					buffer[4] = shared.OP_WRITE
				}
				binary.LittleEndian.PutUint32(buffer[9+len(key):], uint32(len(value)))
				copy(buffer[13+len(key):], value)

				binary.LittleEndian.PutUint32(buffer[:4], uint32(totalLength-4))
				binary.LittleEndian.PutUint32(buffer[5:9], uint32(len(key)))
				copy(buffer[9:], key)

				err := shared.Write(connection, buffer[:totalLength])
				if err != nil {
					panic(err)
				}

				err = shared.Read(connection, buffer[:4])
				if err != nil {
					panic(err)
				}

				amount := binary.LittleEndian.Uint32(buffer[:4])

				err = shared.Read(connection, buffer[:amount])
				if err != nil {
					panic(err)
				}

				if client.IsWriteMemory && buffer[0] != shared.OP_WRITE_MEMORY {
					panic(fmt.Sprintf("Memory Write operation failed, expected %v, got %v", shared.OP_WRITE_MEMORY, buffer[0]))
				}
				if !client.IsWriteMemory && buffer[0] != shared.OP_WRITE {
					panic(fmt.Sprintf("Disk Write operation failed, expected %v, got %v", shared.OP_WRITE, buffer[0]))
				}

				atomic.AddUint32(&completedOps, 1)
			}

			key := client.Keys[0]
			warmupValue := client.WarmupValues[0]

			var totalLength int
			totalLength = 9 + len(key)
			if client.IsReadMemory {
				buffer[4] = shared.OP_READ_MEMORY
			} else {
				buffer[4] = shared.OP_READ
			}

			binary.LittleEndian.PutUint32(buffer[:4], uint32(totalLength-4))
			binary.LittleEndian.PutUint32(buffer[5:9], uint32(len(key)))
			copy(buffer[9:], key)

			err := shared.Write(connection, buffer[:totalLength])
			if err != nil {
				panic(err)
			}

			err = shared.Read(connection, buffer[:4])
			if err != nil {
				panic(err)
			}

			amount := binary.LittleEndian.Uint32(buffer[:4])

			err = shared.Read(connection, buffer[:amount])
			if err != nil {
				panic(err)
			}

			if client.IsReadMemory && buffer[0] != shared.OP_READ_MEMORY {
				panic(fmt.Sprintf("Memory Read operation failed, expected %v, got %v", shared.OP_READ_MEMORY, buffer[0]))
			}

			if !client.IsReadMemory && buffer[0] != shared.OP_READ {
				panic(fmt.Sprintf("Memory Read operation failed, expected %v, got %v", shared.OP_READ, buffer[0]))
			}

			valueSize := binary.LittleEndian.Uint32(buffer[1:5])
			if valueSize == 0 {
				panic(fmt.Sprintf("GOT A NULL READ FOR KEY: %s", string(key)))
			} else {
				if !bytes.Equal(buffer[5:5+valueSize], warmupValue) {
					panic(fmt.Sprintf("GOT A WRONG VALUE FOR KEY: %s, warmup-%s, returned-%s", string(key), string(warmupValue), string(buffer[4:4+valueSize])))
				}
			}

			atomic.AddUint32(&completedOps, 1)

			warmup.Done()
		}(i, client.Connections[i])
	}

	warmup.Wait()
	warmupBar.Wait()
}

func (client *Client) displayResults(
	start int64,
	end int64,
	clientTimes []int,
	clientWriteTimes []int,
	clientReadTimes []int,
	count int,
	writeCount int,
	readCount int,
) {
	sort.Ints(clientTimes[:count])
	sort.Ints(clientWriteTimes[:writeCount])
	sort.Ints(clientReadTimes[:readCount])

	avgAll := 0
	maxAll := math.MinInt32
	minAll := math.MaxInt32
	for i := range count {
		timeAll := clientTimes[i]
		avgAll += timeAll
		if timeAll > maxAll {
			maxAll = timeAll
		}
		if timeAll < minAll {
			minAll = timeAll
		}
	}

	avgAll /= count

	avgWrite := 0
	maxWrite := math.MinInt32
	minWrite := math.MaxInt32
	for i := range writeCount {
		timeWrite := clientWriteTimes[i]
		avgWrite += timeWrite
		if timeWrite > maxWrite {
			maxWrite = timeWrite
		}
		if timeWrite < minWrite {
			minWrite = timeWrite
		}
	}

	if writeCount > 0 {
		avgWrite /= writeCount
	}

	avgRead := 0
	maxRead := math.MinInt32
	minRead := math.MaxInt32
	for i := range readCount {
		timeRead := clientReadTimes[i]
		avgRead += timeRead
		if timeRead > maxRead {
			maxRead = timeRead
		}
		if timeRead < minRead {
			minRead = timeRead
		}
	}
	if readCount > 0 {
		avgRead /= readCount
	}
	fmt.Printf("\nBenchmark complete!\n")
	fmt.Printf("Connections: %d\n", client.NumClients*client.TotalAddresses)
	fmt.Printf("Data Size: %d\n", client.DataSize)
	if writeCount > 0 && readCount > 0 {
		fmt.Printf("All - Count(%d) OPS(%d) Avg(%d) Min(%d) Max(%d) 50th(%d) 90th(%d) 95th(%d) 99th(%d) 99.9th(%d) 99.99th(%d)\n",
			count, int(float32(count)/(float32(end-start)/1000.0)), avgAll, minAll, maxAll,
			clientTimes[int(float32(count)*0.5)],
			clientTimes[int(float32(count)*0.9)],
			clientTimes[int(float32(count)*0.95)],
			clientTimes[int(float32(count)*0.99)],
			clientTimes[int(float32(count)*0.999)],
			clientTimes[int(float32(count)*0.9999)],
		)
	}
	if writeCount > 0 {
		fmt.Printf("Update - Count(%d) OPS(%d) Avg(%d) Min(%d) Max(%d) 50th(%d) 90th(%d) 95th(%d) 99th(%d) 99.9th(%d) 99.99th(%d)\n",
			writeCount, int(float32(writeCount)/(float32(end-start)/1000.0)), avgWrite, minWrite, maxWrite,
			clientWriteTimes[int(float32(writeCount)*0.5)],
			clientWriteTimes[int(float32(writeCount)*0.9)],
			clientWriteTimes[int(float32(writeCount)*0.95)],
			clientWriteTimes[int(float32(writeCount)*0.99)],
			clientWriteTimes[int(float32(writeCount)*0.999)],
			clientWriteTimes[int(float32(writeCount)*0.9999)],
		)
	}
	if readCount > 0 {
		fmt.Printf("Read - Count(%d) OPS(%d) Avg(%d) Min(%d) Max(%d) 50th(%d) 90th(%d) 95th(%d) 99th(%d) 99.9th(%d) 99.99th(%d)\n",
			readCount, int(float32(readCount)/(float32(end-start)/1000.0)), avgRead, minRead, maxRead,
			clientReadTimes[int(float32(readCount)*0.5)],
			clientReadTimes[int(float32(readCount)*0.9)],
			clientReadTimes[int(float32(readCount)*0.95)],
			clientReadTimes[int(float32(readCount)*0.99)],
			clientReadTimes[int(float32(readCount)*0.999)],
			clientReadTimes[int(float32(readCount)*0.9999)],
		)
	}
}

func progressBar(title string, numOps int) {
	barWidth := 50
	for {
		completed := atomic.LoadUint32(&completedOps)

		if completed > uint32(numOps) {
			completed = uint32(numOps)
		}

		percent := float64(completed) / float64(numOps) * 100
		filled := int(float64(barWidth) * float64(completed) / float64(numOps))
		if filled > barWidth {
			filled = barWidth
		}
		if filled < 0 {
			filled = 0
		}

		bar := "[" + strings.Repeat("=", filled) + strings.Repeat(" ", barWidth-filled) + "]"
		fmt.Printf("\r%s Progress: %s %.2f%% (%d/%d)", title, bar, percent, completed, numOps)
		if completed >= uint32(numOps) {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
}
