package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"golang.org/x/sync/errgroup"
	"log"
	"math"
	"net"
	_ "net/http/pprof"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

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

func TestServer(t *testing.T) {
	flag.Parse()
	g, ctx := errgroup.WithContext(context.Background())

	if err := g.Wait(); err != nil {
		t.Error(err)
	}

	var senders sync.Map
	var opIndex uint32
	peerAddresses := strings.Split(*peerAddressesString, ",")

	fmt.Printf("host%s\n", *peerListenAddress)

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

	numPeers := len(peerAddresses)
	peerConnections := make([][]chan WriteOp, numPeers)
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

	fmt.Println("Started node!")
	g.Go(func() error {
		return handleClients(ctx, g, &pool, node, &senders, opIndex, *clientWriteChanSize)
	})
	fmt.Println("Started client listener!")
	g.Go(func() error {
		return handlePeers(ctx, g, node)
	})
	fmt.Println("Started peer listener!")

	connectPeers(ctx, t, g, peerAddresses, peerConnections, peerConnRoundRobins, &pool)
	fmt.Println("All peers connected!")

	g.Go(func() error {
		return runRaftNode(ctx, node, config, peerConnections, peerConnRoundRobins, storage, &pool, &senders)
	})

	if err := g.Wait(); err != nil {
		t.Fatalf("Server error: %v", err)
	}
}

func handleClients(
	ctx context.Context,
	g *errgroup.Group,
	pool *sync.Pool,
	node raft.Node,
	senders *sync.Map,
	opIndex uint32,
	clientWriteChanSize int,
) error {
	clientListener, err := net.Listen("tcp", *clientListenAddress)
	if err != nil {
		return fmt.Errorf("error listening on client: %w", err)
	}

	g.Go(func() error {
		<-ctx.Done()
		clientListener.Close()
		return nil
	})

	g.Go(func() error {
		defer clientListener.Close()

		for {
			select {
			case <-ctx.Done():
				return nil
			default:
				conn, err := clientListener.Accept()
				if err != nil {
					if ctx.Err() != nil {
						return nil
					}
					return fmt.Errorf("error accepting client: %w", err)
				}

				if tcpConn, ok := conn.(*net.TCPConn); ok {
					tcpConn.SetNoDelay(true)
					tcpConn.SetKeepAlive(true)
					tcpConn.SetKeepAlivePeriod(30 * time.Second)
				}

				writeChan := make(chan WriteOp, clientWriteChanSize)

				g.Go(func() error {
					defer close(writeChan)
					for {
						select {
						case <-ctx.Done():
							return nil
						case writeOp, ok := <-writeChan:
							if !ok {
								return nil
							}
							defer pool.Put(writeOp)
							if err := Write(conn, writeOp.buffer[:writeOp.size]); err != nil {
								return fmt.Errorf("error writing to client: %w", err)
							}
						}
					}
				})

				g.Go(func() error {
					readBuffer := make([]byte, 1000000)
					for {
						select {
						case <-ctx.Done():
							return g.Wait()
						default:
							if err := Read(conn, readBuffer[:4]); err != nil {
								return fmt.Errorf("error reading client: %w", err)
							}

							amount := binary.LittleEndian.Uint32(readBuffer[:4])

							if err := Read(conn, readBuffer[:amount]); err != nil {
								return fmt.Errorf("error reading client: %w", err)
							}

							messageId := atomic.AddUint32(&opIndex, 1)
							bufferCopy := pool.Get().([]byte)
							binary.LittleEndian.PutUint32(bufferCopy[:4], messageId)
							copy(bufferCopy[4:amount+4], readBuffer[:amount])
							senders.Store(messageId, ClientRequest{writeChan, bufferCopy})

							g.Go(func() error {
								if err := node.Propose(context.TODO(), bufferCopy[:amount+4]); err != nil {
									senders.Delete(messageId)
									pool.Put(bufferCopy)
									return fmt.Errorf("error proposing client: %w", err)
								}
								return nil
							})
						}
					}
				})
			}
		}
	})

	return nil
}

func handlePeers(
	ctx context.Context,
	g *errgroup.Group,
	node raft.Node,
) error {
	peerListener, err := net.Listen("tcp", *peerListenAddress)
	if err != nil {
		return fmt.Errorf("peer listener failed: %w", err)
	}

	fmt.Println("Starting peer listener!")

	g.Go(func() error {
		<-ctx.Done()
		fmt.Println("Closjng!")
		peerListener.Close()
		return nil
	})

	g.Go(func() error {
		fmt.Println("In here!")
		defer peerListener.Close()

		for {
			select {
			case <-ctx.Done():
				return nil
			default:
				fmt.Println("Waiting for peer to connect...")
				conn, err := peerListener.Accept()
				if err != nil {
					if ctx.Err() != nil {
						return nil
					}
					return fmt.Errorf("peer accept failed: %w", err)
				}

				fmt.Println("Got a connection ay?")

				if tcpConn, ok := conn.(*net.TCPConn); ok {
					tcpConn.SetNoDelay(true)
					tcpConn.SetKeepAlive(true)
					tcpConn.SetKeepAlivePeriod(30 * time.Second)
				}

				bytes := make([]byte, 4)
				err = Read(conn, bytes)
				if err != nil {
					return err
				}
				peerIndex := binary.LittleEndian.Uint32(bytes)
				fmt.Printf("Got connection from peer %d\n", peerIndex)

				g.Go(func() error {
					//defer conn.Close()
					readBuffer := make([]byte, 1024*1024)
					for {
						select {
						case <-ctx.Done():
							return nil
						default:
							err := Read(conn, readBuffer[:4])
							if err != nil {
								return err
							}

							amount := binary.LittleEndian.Uint32(readBuffer[:4])

							err = Read(conn, readBuffer[:amount])
							if err != nil {
								return err
							}

							var msg raftpb.Message
							if err := msg.Unmarshal(readBuffer[:amount]); err != nil {
								return err
							}

							g.Go(func() error {
								err := node.Step(ctx, msg)
								if err != nil {
									return err
								}
								return nil
							})
						}
					}
				})
			}
		}
	})

	return nil
}

func connectPeers(
	ctx context.Context,
	t *testing.T,
	g *errgroup.Group,
	peerAddresses []string,
	peerConnections [][]chan WriteOp,
	peerConnRoundRobins []uint32,
	pool *sync.Pool,
) {
	for p := range len(peerAddresses) {
		if p == *nodeIndex {
			fmt.Printf("Skipping it because its %d\n", p)
			continue
		}
		peerConnRoundRobins[p] = 0
		peerConnections[p] = make([]chan WriteOp, *numPeerConnections)
		fmt.Printf("Connecting peer %s\n", peerAddresses[p])
		for c := range *numPeerConnections {
			for {
				connection, err := net.Dial("tcp", peerAddresses[p])
				if err != nil {
					time.Sleep(50 * time.Millisecond)
					continue
				}

				//if tcpConn, ok := connection.(*net.TCPConn); ok {
				//	tcpConn.SetNoDelay(true)
				//	tcpConn.SetKeepAlive(true)
				//	tcpConn.SetKeepAlivePeriod(30 * time.Second)
				//}
				bytes := make([]byte, 4)
				binary.LittleEndian.PutUint32(bytes, uint32(*nodeIndex))
				err = Write(connection, bytes)
				if err != nil {
					t.Fatal(err)
				}

				writeChan := make(chan WriteOp, *peerWriteChanSize)
				peerConnections[p][c] = writeChan
				g.Go(func() error {
					defer connection.Close()
					defer close(writeChan)

					for {
						select {
						case <-ctx.Done():
							return nil
						case writeOp, ok := <-writeChan:
							if !ok {
								return nil
							}
							if err := Write(connection, writeOp.buffer[:writeOp.size]); err != nil {
								return fmt.Errorf("peer write failed: %w", err)
							}
							pool.Put(writeOp.buffer)
						}
					}
				})
				break
			}
		}
	}
}

func runRaftNode(
	ctx context.Context,
	node raft.Node,
	config *raft.Config,
	peerConnections [][]chan WriteOp,
	peerConnRoundRobins []uint32,
	storage *raft.MemoryStorage,
	pool *sync.Pool,
	senders *sync.Map,
) error {
	ticker := time.NewTicker(150 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			node.Tick()
		case rd := <-node.Ready():
			//fmt.Printf("Commited entries: %d\n", len(rd.CommittedEntries))
			if !raft.IsEmptyHardState(rd.HardState) {
				//fmt.Printf("Setting hardstate\n")
				err := storage.SetHardState(rd.HardState)
				if err != nil {
					return err
				}
			}

			if len(rd.Entries) > 0 {
				if err := storage.Append(rd.Entries); err != nil {
					return err
				}
			}

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
					return err
				}

				binary.LittleEndian.PutUint32(buffer[:4], uint32(size))

				peerConnections[msg.To-1][atomic.AddUint32(&peerConnRoundRobins[msg.To-1], 1)%uint32(*numPeerConnections)] <- WriteOp{
					buffer,
					uint32(size + 4),
				}
			}

			for _, entry := range rd.Entries {
				switch entry.Type {
				case raftpb.EntryConfChange:
					var cc raftpb.ConfChange
					if err := cc.Unmarshal(entry.Data); err != nil {
						return err
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
}
