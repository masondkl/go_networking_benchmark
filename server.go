package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

const OP_PROPOSE = 0
const OP_ACK = 1

func server() {
	var senders sync.Map
	var opIndex uint32

	nodeIndex, err := strconv.Atoi(os.Args[2])
	if err != nil {
		panic(err)
	}
	poolDataSize, err := strconv.Atoi(os.Args[3])
	if err != nil {
		panic(err)
	}
	poolWarmupSize, err := strconv.Atoi(os.Args[4])
	if err != nil {
		panic(err)
	}
	numPeerConnections, err := strconv.Atoi(os.Args[5])
	if err != nil {
		panic(err)
	}
	clientWriteChanSize, err := strconv.Atoi(os.Args[6])
	if err != nil {
		panic(err)
	}
	peerWriteChanSize, err := strconv.Atoi(os.Args[7])
	if err != nil {
		panic(err)
	}
	peerListenAddress := os.Args[8]
	clientListenAddress := os.Args[9]
	peerAddresses := strings.Split(os.Args[10], ",")

	var pool = sync.Pool{
		New: func() interface{} {
			return make([]byte, poolDataSize)
		},
	}

	for range poolWarmupSize {
		pool.Put(pool.Get())
	}

	peerListener, err := net.Listen("tcp", peerListenAddress)
	if err != nil {
		panic(err)
	}

	numPeers := len(peerAddresses)
	peerConnections := make([][]chan WriteOp, numPeers)
	peerRoundRobins := make([]uint32, numPeers)

	go func() {
		clientListener, err := net.Listen("tcp", clientListenAddress)
		if err != nil {
			panic(err)
		}

		for {
			connection, err := clientListener.Accept()
			if err != nil {
				panic(err)
			}

			err = connection.(*net.TCPConn).SetNoDelay(true)
			if err != nil {
				panic(err)
			}

			writeChan := make(chan WriteOp, clientWriteChanSize)
			go func() {
				defer func() {
					err := connection.Close()
					if err != nil {
						panic(err)
					}
					close(writeChan)
				}()

				for clientWrite := range writeChan {
					err := Write(connection, clientWrite.buffer[:clientWrite.size])
					if err != nil {
						panic(err)
					}
					pool.Put(clientWrite.buffer)
				}
			}()

			go func() {
				readBuffer := make([]byte, poolDataSize)

				for {
					if err := Read(connection, readBuffer[:4]); err != nil {
						panic("Error reading size")
					}

					amount := binary.LittleEndian.Uint32(readBuffer[:4])

					if err := Read(connection, readBuffer[:amount]); err != nil {
						log.Printf("Error reading message: %v", err)
						panic(err)
					}

					nextIndex := atomic.AddUint32(&opIndex, 1)

					//fmt.Printf("Received client request new index is: %d\n", nextIndex)

					senders.Store(nextIndex, &ClientRequest{
						index:           nextIndex,
						acks:            1,
						clientWriteChan: writeChan,
					})

					for i := range numPeers {
						if i == nodeIndex {
							continue
						}

						//fmt.Printf("[%d -> %d] Proposing\n", nodeIndex, i)
						bufferCopy := pool.Get().([]byte)
						size := amount + 9
						binary.LittleEndian.PutUint32(bufferCopy[0:4], size-4)
						bufferCopy[4] = OP_PROPOSE
						binary.LittleEndian.PutUint32(bufferCopy[5:9], nextIndex)
						copy(bufferCopy[9:size], readBuffer[:amount])
						peerConnections[i][atomic.AddUint32(&peerRoundRobins[i], 1)%uint32(numPeerConnections)] <- WriteOp{
							size:   size,
							buffer: bufferCopy,
						}
					}
				}
			}()
		}
	}()

	go func() {
		for {
			connection, err := peerListener.Accept()
			if err != nil {
				panic(err)
			}

			err = connection.(*net.TCPConn).SetNoDelay(true)
			if err != nil {
				panic(err)
			}

			bytes := make([]byte, 4)
			err = Read(connection, bytes)
			if err != nil {
				panic(err)
			}
			peerIndex := binary.LittleEndian.Uint32(bytes)
			fmt.Printf("Got connection from peer %d\n", peerIndex)

			go func() {
				readBuffer := make([]byte, poolDataSize)
				for {
					err := Read(connection, readBuffer[:4])
					if err != nil {
						panic(err)
					}

					amount := binary.LittleEndian.Uint32(readBuffer[:4])

					err = Read(connection, readBuffer[:amount])
					if err != nil {
						panic(err)
					}

					op := readBuffer[0]
					index := binary.LittleEndian.Uint32(readBuffer[1:5])
					if op == OP_PROPOSE {
						buffer := pool.Get().([]byte)
						binary.LittleEndian.PutUint32(buffer[0:4], 5)
						buffer[4] = OP_ACK
						binary.LittleEndian.PutUint32(buffer[5:9], index)

						//fmt.Printf("[%d -> %d] Received proposal for index: %d\n", peerIndex, nodeIndex, index)

						peerConns := peerConnections[peerIndex]
						nextPeer := atomic.AddUint32(&peerRoundRobins[peerIndex], 1) % uint32(numPeerConnections)

						peerConns[nextPeer] <- WriteOp{
							buffer: buffer,
							size:   9,
						}
					} else if op == OP_ACK {
						senderAny, ok := senders.Load(index)
						if !ok {
							panic("Received ack late!")
						}
						request := senderAny.(*ClientRequest)

						totalAcks := atomic.AddUint32(&request.acks, 1)
						//fmt.Printf("Total acks vs numPeers: %d vs %d\n", totalAcks, numPeers)
						if totalAcks == uint32(numPeers) {
							//fmt.Printf("Responding to client for index: %d\n", index)
							buffer := pool.Get().([]byte)
							binary.LittleEndian.PutUint32(buffer[:4], 4)
							request.clientWriteChan <- WriteOp{
								buffer: buffer,
								size:   8,
							}
							senders.Delete(index)
						} else {
							//fmt.Printf("Received ack for index: %d\n", index)
						}

					} else {
						log.Panicf("GOT BAD OP: %d\n", op)
					}
				}
			}()
		}
	}()

	for p := range numPeers {
		if p == nodeIndex {
			fmt.Printf("Skipping it because its %d\n", p)
			continue
		}
		peerRoundRobins[p] = 0
		peerConnections[p] = make([]chan WriteOp, numPeerConnections)
		for c := range numPeerConnections {
			for {
				connection, err := net.Dial("tcp", peerAddresses[p])
				if err != nil {
					continue
				}

				err = connection.(*net.TCPConn).SetNoDelay(true)
				if err != nil {
					panic(err)
				}

				bytes := make([]byte, 4)
				binary.LittleEndian.PutUint32(bytes, uint32(nodeIndex))
				err = Write(connection, bytes)
				if err != nil {
					panic(err)
				}

				writeChan := make(chan WriteOp, peerWriteChanSize)
				go func() {
					defer func() {
						err := connection.Close()
						if err != nil {
							panic(err)
						}
						close(writeChan)
					}()

					for clientWrite := range writeChan {
						err := Write(connection, clientWrite.buffer[:clientWrite.size])
						if err != nil {
							panic(err)
						}
						pool.Put(clientWrite.buffer)
					}
				}()

				peerConnections[p][c] = writeChan
				break
			}
		}
	}

	fmt.Printf("ALL CONNECTIONS MADE IT\n")
	select {}
}
