package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"log"
	"math"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const OP_PROPOSE = 0
const OP_ACK = 1

var (
	nodeIndex           = flag.Int("node", 0, "node index")
	poolDataSize        = flag.Int("pool-data-size", 0, "pool data size")
	poolWarmupSize      = flag.Int("pool-warmup-size", 0, "pool warmup size")
	numPeerConnections  = flag.Int("peer-connections", 0, "number of peer connections")
	clientWriteChanSize = flag.Int("client-write-chan", 0, "client write channel size")
	peerWriteChanSize   = flag.Int("peer-write-chan", 0, "peer write channel size")
	peerListenAddress   = flag.String("peer-listen", "", "peer listen address")
	clientListenAddress = flag.String("client-listen", "", "client listen address")
	peerAddressesString = flag.String("peer-addresses", "", "comma-separated peer addresses")
)

func profile() {
	cpuProfile, err := os.Create("cpu.prof")
	if err != nil {
		log.Fatalf("could not create CPU profile: %v", err)
	}
	pprof.StartCPUProfile(cpuProfile)
	runtime.SetBlockProfileRate(1)
	runtime.SetMutexProfileFraction(1)

	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		sig := <-sigChan

		log.Printf("Received signal %s, stopping profiler...", sig)

		// Stop CPU profiling
		pprof.StopCPUProfile()
		cpuProfile.Close()
		log.Println("CPU profiling stopped")

		// Memory (heap) profile
		memProfileFile, _ := os.Create("mem.prof")
		runtime.GC() // update mem stats
		pprof.WriteHeapProfile(memProfileFile)
		memProfileFile.Close()
		log.Println("Memory profile written")

		// Other profiles
		profiles := []string{"goroutine", "threadcreate", "block", "mutex"}
		for _, prof := range profiles {
			f, err := os.Create(prof + ".prof")
			if err != nil {
				log.Printf("Could not create %s: %v", prof, err)
				continue
			}
			if err := pprof.Lookup(prof).WriteTo(f, 0); err != nil {
				log.Printf("Error writing %s: %v", prof, err)
			}
			f.Close()
			log.Printf("%s profile written", prof)
		}

		os.Exit(0)
	}()
}

func server() {
	flag.Parse()
	//profile()

	var senders sync.Map
	var opIndex uint32
	peerAddresses := strings.Split(*peerAddressesString, ",")

	host, _, err := net.SplitHostPort(*peerListenAddress)
	if err != nil {
		panic(err)
	}

	pprofAddress := net.JoinHostPort(host, "6060")

	go func() {
		log.Printf("Starting pprof server on %s\n", pprofAddress)
		log.Println(http.ListenAndServe(pprofAddress, nil))
	}()

	var pool = sync.Pool{
		New: func() interface{} {
			return make([]byte, *poolDataSize)
		},
	}

	var stored [][]byte
	stored = make([][]byte, *poolWarmupSize)
	for i := range *poolWarmupSize {
		stored[i] = pool.Get().([]byte)
	}
	for i := range stored {
		pool.Put(stored[i])
	}
	stored = nil

	peerListener, err := net.Listen("tcp", *peerListenAddress)
	if err != nil {
		panic(err)
	}

	numPeers := len(peerAddresses)
	peerConnections := make([][]PeerConnection, numPeers)
	peerConnRoundRobins := make([]uint32, numPeers)

	storage := raft.NewMemoryStorage()
	config := &raft.Config{
		ID:              uint64(*nodeIndex + 1),
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

	go func() {
		clientListener, err := net.Listen("tcp", *clientListenAddress)
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

			writeLock := &sync.Mutex{}

			go func() {
				readBuffer := make([]byte, 1000000)

				for {
					if err := Read(connection, readBuffer[:4]); err != nil {
						return
					}

					amount := binary.LittleEndian.Uint32(readBuffer[:4])

					if err := Read(connection, readBuffer[:amount]); err != nil {
						log.Printf("Error reading message: %v", err)
						return
					}

					messageId := atomic.AddUint32(&opIndex, 1)
					bufferCopy := pool.Get().([]byte)
					binary.LittleEndian.PutUint32(bufferCopy[:4], messageId)
					copy(bufferCopy[4:amount+4], readBuffer[:amount])
					senders.Store(messageId, ClientRequest{&connection, writeLock, bufferCopy})

					go func() {
						err := node.Propose(context.TODO(), bufferCopy[:amount+4])
						if err != nil {
							panic(err)
						}
					}()
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
				readBuffer := make([]byte, 1000000)
				for {
					err := Read(connection, readBuffer[:4])
					if err != nil {
						return
					}

					amount := binary.LittleEndian.Uint32(readBuffer[:4])

					err = Read(connection, readBuffer[:amount])
					if err != nil {
						return
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
		if p == *nodeIndex {
			fmt.Printf("Skipping it because its %d\n", p)
			continue
		}
		peerConnRoundRobins[p] = 0
		peerConnections[p] = make([]PeerConnection, *numPeerConnections)
		for c := range *numPeerConnections {
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
				binary.LittleEndian.PutUint32(bytes, uint32(*nodeIndex))
				err = Write(connection, bytes)
				if err != nil {
					panic(err)
				}

				peerConnections[p][c] = PeerConnection{
					connection: &connection,
					writeLock:  &sync.Mutex{},
				}

				break
			}
		}
	}

	fmt.Printf("Peers connected! starting raft\n")
	ticker := time.NewTicker(150 * time.Millisecond)
	defer ticker.Stop()
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
				conn := peerConnections[msg.To-1][atomic.AddUint32(&peerConnRoundRobins[msg.To-1], 1)%uint32(*numPeerConnections)]
				go func() {
					conn.writeLock.Lock()
					err := Write(*conn.connection, buffer[:size+4])
					if err != nil {
						log.Printf("Write error: %v", err)
					}
					conn.writeLock.Unlock()
					pool.Put(buffer)
				}()
				//peerConnections[msg.To-1][atomic.AddUint32(&peerConnRoundRobins[msg.To-1], 1)%uint32(*numPeerConnections)] <- WriteOp{
				//	buffer,
				//	uint32(size + 4),
				//}
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

						senderAny, ok := senders.LoadAndDelete(index)
						if ok {
							request := senderAny.(ClientRequest)
							binary.LittleEndian.PutUint32(request.buffer[:4], 4)
							binary.LittleEndian.PutUint32(request.buffer[4:8], index)
							go func() {
								request.writeLock.Lock()
								err := Write(*request.connection, request.buffer[:8])
								if err != nil {
									log.Printf("Write error: %v", err)
								}
								request.writeLock.Unlock()
								pool.Put(request.buffer)
							}()
						} else {
							log.Panicf("PROBLEM LOADING SENDER FOR %d", index)
						}
					}
				}
			}
			node.Advance()
		}
	}
}
