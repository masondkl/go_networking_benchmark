package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"log"
	"math"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
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

	var stored [][]byte
	stored = make([][]byte, poolWarmupSize)
	for i := range poolWarmupSize {
		stored[i] = pool.Get().([]byte)
	}
	for i := range stored {
		pool.Put(stored[i])
	}
	stored = nil

	peerListener, err := net.Listen("tcp", peerListenAddress)
	if err != nil {
		panic(err)
	}

	numPeers := len(peerAddresses)
	//peerSteps := make([][]chan raftpb.Message, numPeers)
	//peerProposes := make([][]chan WriteOp, numPeers)
	peerConnections := make([][]chan WriteOp, numPeers)
	peerConnRoundRobins := make([]uint32, numPeers)
	//peerProposeRoundRobins := make([]uint32, numPeers)
	//peerStepRoundRobins := make([]uint32, numPeers)

	storage := raft.NewMemoryStorage()
	config := &raft.Config{
		ID:              uint64(nodeIndex + 1),
		ElectionTick:    10,
		HeartbeatTick:   5,
		Storage:         storage,
		MaxSizePerMsg:   math.MaxUint32,
		MaxInflightMsgs: 1000000,
	}

	peers := make([]raft.Peer, numPeers)
	for i := 0; i < numPeers; i++ {
		peers[i].ID = uint64(i + 1)
	}

	node := raft.StartNode(config, peers)

	connectGroup := sync.WaitGroup{}
	connectGroup.Add((numPeers - 1) * numPeerConnections)

	ticker := time.NewTicker(150 * time.Millisecond)
	defer ticker.Stop()
	go func() {
		connectGroup.Wait()
		for {
			select {
			case <-ticker.C:
				node.Tick()
			case rd := <-node.Ready():
				//fmt.Printf("Commited entries: %d\n", len(rd.CommittedEntries))
				if !raft.IsEmptyHardState(rd.HardState) {
					//fmt.Printf("Setting hardstate\n")
					err := storage.SetHardState(rd.HardState)
					if err != nil {
						panic(err)
					}
				}

				if len(rd.Entries) > 0 {
					if err := storage.Append(rd.Entries); err != nil {
						log.Printf("Append entries error: %v", err)
					}
				}

				// TODO("Fix peer sending msgs")
				for _, msg := range rd.Messages {
					buffer := pool.Get().([]byte)

					if len(msg.Snapshot.Data) > 0 {
						fmt.Printf("%d\n", len(msg.Snapshot.Data))
					}

					if !raft.IsEmptySnap(rd.Snapshot) {
						fmt.Printf("Append snapshot to raft\n")
					}

					requiredSize := 4 + msg.Size()
					if len(buffer) < requiredSize {
						buffer = append(buffer, make([]byte, requiredSize)...)
					}

					size, err := msg.MarshalTo(buffer[4:])

					if err != nil {
						log.Printf("Marshal error: %v", err)
						continue
					}

					//fmt.Printf("Received proposal %v entries len=%d size=%d\n", msg, len(msg.Entries), size)
					//fmt.Printf("Size :%d \n", size)
					//if size > 51 {
					//	fmt.Printf("Received proposal %v entries len=%d size=%d\n", msg, len(msg.Entries), size)
					//}

					//fmt.Printf("Send peer message: %d -> %d\n", msg.From, msg.To)
					binary.LittleEndian.PutUint32(buffer[:4], uint32(size))

					peerConnections[msg.To-1][atomic.AddUint32(&peerConnRoundRobins[msg.To-1], 1)%uint32(numPeerConnections)] <- WriteOp{
						buffer,
						uint32(size + 4),
					}
				}

				for _, entry := range rd.Entries {
					switch entry.Type {
					case raftpb.EntryConfChange:
						var cc raftpb.ConfChange
						if err := cc.Unmarshal(entry.Data); err != nil {
							log.Printf("Unmarshal conf change error: %v", err)
							continue
						}
						node.ApplyConfChange(cc)
					case raftpb.EntryNormal:
						if len(entry.Data) >= 4 && node.Status().Lead == config.ID {
							index := binary.LittleEndian.Uint32(entry.Data[:4])

							buffer := pool.Get().([]byte)
							//Write 0 bytes
							binary.BigEndian.PutUint32(buffer[:4], 0)

							senderAny, ok := senders.LoadAndDelete(index)
							if ok {
								request := senderAny.(ClientRequest)
								pool.Put(request.buffer)
								select {
								case request.writeChan <- WriteOp{buffer, 4}:
								default:
									log.Printf("Client write channel is full, dropping message?")
								}
								//fmt.Printf("Committing %d\n", messageId)
							} else {
								log.Panicf("PROBLEM LOADING SENDER FOR %d", index)
							}
						}
					}
				}
				node.Advance()
			}
		}
	}()

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
				readBuffer := make([]byte, 1000000)

				for {
					if err := Read(connection, readBuffer[:4]); err != nil {
						panic("Error reading size")
					}

					amount := binary.LittleEndian.Uint32(readBuffer[:4])

					if err := Read(connection, readBuffer[:amount]); err != nil {
						log.Printf("Error reading message: %v", err)
						panic(err)
					}

					messageId := atomic.AddUint32(&opIndex, 1)
					bufferCopy := pool.Get().([]byte)
					binary.LittleEndian.PutUint32(bufferCopy[:4], messageId)
					copy(bufferCopy[4:amount+4], readBuffer[:amount])
					senders.Store(messageId, ClientRequest{writeChan, bufferCopy})

					go func(buffer []byte) {
						err := node.Propose(context.TODO(), buffer[:amount+4])
						if err != nil {
							panic(err)
						}
					}(bufferCopy)
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
				connectGroup.Wait()
				readBuffer := make([]byte, 1000000)
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

					// TODO("replace with raft step and marshal")

					var msg raftpb.Message
					if err := msg.Unmarshal(readBuffer[:amount]); err != nil {
						log.Printf("Unmarshal error: %v", err)
						continue
					}
					//if msg.From > 0 {
					//fmt.Printf("Received proposal %v entries len=%d\n", msg, len(msg.Entries))

					go func(msg raftpb.Message) {
						err := node.Step(context.TODO(), msg)
						if err != nil {
							panic(err)
						}
					}(msg)
					//}
				}
			}()
		}
	}()

	for p := range numPeers {
		if p == nodeIndex {
			fmt.Printf("Skipping it because its %d\n", p)
			continue
		}
		peerConnRoundRobins[p] = 0
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
				peerConnections[p][c] = writeChan
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

				connectGroup.Done()
				break
			}
		}
	}

	fmt.Printf("ALL CONNECTIONS MADE IT\n")
	select {}
}
