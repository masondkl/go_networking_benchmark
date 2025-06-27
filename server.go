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
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

var (
	nodeIndex           = flag.Int("node", 0, "node index")
	poolDataSize        = flag.Int("pool-data-size", 0, "pool data size")
	poolWarmupSize      = flag.Int("pool-warmup-size", 0, "pool warmup size")
	numPeerConnections  = flag.Int("peer-connections", 0, "number of peer connections")
	peerListenAddress   = flag.String("peer-listen", "", "peer listen address")
	clientListenAddress = flag.String("client-listen", "", "client listen address")
	peerAddressesString = flag.String("peer-addresses", "", "comma-separated peer addresses")
	walFileCount        = flag.Int("wal-file-count", 0, "wal file count")
	manual              = flag.String("manual", "none", "fsync, dsync, or none")
	flags               = flag.String("flags", "none", "fsync, dsync, sync, none")
)

type WalSlot struct {
	file  *os.File
	mutex *sync.Mutex
}

type Server struct {
	senders             sync.Map
	opIndex             uint32
	peerAddresses       []string
	pool                sync.Pool
	node                raft.Node
	storage             *raft.MemoryStorage
	config              *raft.Config
	peerConnections     [][]PeerConnection
	peerConnRoundRobins []uint32
	shutdownChan        chan struct{}
	hardstateChan       chan []byte

	walSlots []WalSlot
	//walChans            []chan []byte
	//walGroup            *sync.WaitGroup
	hardstateGroup sync.WaitGroup
	isLeader       bool
}

func (s *Server) initPool() {
	if *poolDataSize%4096 != 0 {
		fmt.Println("Pool data size is not a multiple of 4096 block size")
	}
	s.pool = sync.Pool{
		New: func() interface{} {
			return make([]byte, *poolDataSize)
		},
	}
}

func (s *Server) warmupPool() {
	stored := make([][]byte, *poolWarmupSize)
	for i := range stored {
		stored[i] = s.pool.Get().([]byte)
	}
	for i := range stored {
		s.pool.Put(stored[i])
	}
}

func (s *Server) setupRaft() {
	s.storage = raft.NewMemoryStorage()
	s.config = &raft.Config{
		ID:              uint64(*nodeIndex + 1),
		ElectionTick:    10,
		HeartbeatTick:   5,
		Storage:         s.storage,
		MaxSizePerMsg:   math.MaxUint32,
		MaxInflightMsgs: 1000000,
	}

	fmt.Printf("Peer addresses: %v\n", s.peerAddresses)

	peers := make([]raft.Peer, len(s.peerAddresses))
	for i := range peers {
		fmt.Printf("Peer index: %d\n", i)
		peers[i].ID = uint64(i + 1)
	}

	s.node = raft.StartNode(s.config, peers)

	go func() {
		time.Sleep(time.Second * 15)
		if s.node.Status().Lead == s.config.ID {
			fmt.Printf("Node is leader\n")
			s.isLeader = true
		} else {
			fmt.Printf("Node is not leader\n")
			s.isLeader = false
		}
	}()
}

func (s *Server) connectToPeer(peerIdx, connIdx int) {
	for {
		conn, err := net.Dial("tcp", s.peerAddresses[peerIdx])
		if err != nil {
			//fmt.Printf("Error connecting to peer %d: %v\n", peerIdx, err)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		fmt.Printf("Connected to peer %d\n", connIdx)

		if err := conn.(*net.TCPConn).SetNoDelay(true); err != nil {
			panic(err)
		}

		bytes := make([]byte, 4)
		binary.LittleEndian.PutUint32(bytes, uint32(*nodeIndex))
		if err := Write(conn, bytes); err != nil {
			panic(err)
		}

		s.peerConnections[peerIdx][connIdx] = PeerConnection{
			connection: &conn,
			writeLock:  &sync.Mutex{},
		}
		break
	}
}

func (s *Server) setupPeerConnections() {
	numPeers := len(s.peerAddresses)
	s.peerConnections = make([][]PeerConnection, numPeers)
	s.peerConnRoundRobins = make([]uint32, numPeers)

	for p := range numPeers {
		if p == *nodeIndex {
			continue
		}
		s.peerConnRoundRobins[p] = 0
		s.peerConnections[p] = make([]PeerConnection, *numPeerConnections)
		for c := range *numPeerConnections {
			fmt.Printf("Trying to connect to peer %d\n", p)
			s.connectToPeer(p, c)
			fmt.Printf("Peer %d connected\n", p)
		}
	}
}

func (s *Server) handleClientConnection(conn net.Conn) {
	writeLock := &sync.Mutex{}
	readBuffer := make([]byte, 1000000)

	for {
		if err := Read(conn, readBuffer[:4]); err != nil {
			return
		}

		amount := binary.LittleEndian.Uint32(readBuffer[:4])
		if err := Read(conn, readBuffer[:amount]); err != nil {
			log.Printf("Error reading message: %v", err)
			return
		}

		s.handleClientMessage(conn, writeLock, readBuffer[:amount])
	}
}

func (s *Server) handleClientMessage(conn net.Conn, writeLock *sync.Mutex, data []byte) {
	messageId := atomic.AddUint32(&s.opIndex, 1)
	bufferCopy := s.pool.Get().([]byte)
	binary.LittleEndian.PutUint32(bufferCopy[:4], messageId)
	copy(bufferCopy[4:len(data)+4], data)
	s.senders.Store(messageId, ClientRequest{&conn, writeLock, bufferCopy})
	go func() {
		if err := s.node.Propose(context.TODO(), bufferCopy[:len(data)+4]); err != nil {
			panic(err)
		}
	}()
}

func (s *Server) startClientListener() {
	listener, err := net.Listen("tcp", *clientListenAddress)
	if err != nil {
		panic(err)
	}

	go func() {
		<-s.shutdownChan
		listener.Close()
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-s.shutdownChan:
				return
			default:
				panic(err)
			}
		}

		if err := conn.(*net.TCPConn).SetNoDelay(true); err != nil {
			panic(err)
		}

		go s.handleClientConnection(conn)
	}
}

func (s *Server) handlePeerConnection(conn net.Conn) {
	defer conn.Close()

	bytes := make([]byte, 4)
	if err := Read(conn, bytes); err != nil {
		return
	}
	peerIndex := binary.LittleEndian.Uint32(bytes)
	log.Printf("Got connection from peer %d", peerIndex)

	readBuffer := make([]byte, 10000000)
	for {
		if err := Read(conn, readBuffer[:4]); err != nil {
			return
		}

		amount := binary.LittleEndian.Uint32(readBuffer[:4])
		if cap(readBuffer) < int(amount) {
			readBuffer = append(readBuffer, make([]byte, int(amount)-cap(readBuffer))...)
			readBuffer = readBuffer[:cap(readBuffer)]
		}

		if err := Read(conn, readBuffer[:amount]); err != nil {
			return
		}

		s.handlePeerMessage(readBuffer[:amount])
	}
}

func (s *Server) handlePeerMessage(data []byte) {
	var msg raftpb.Message
	if err := msg.Unmarshal(data); err != nil {
		log.Printf("Unmarshal error: %v", err)
		return
	}

	go func() {
		if err := s.node.Step(context.TODO(), msg); err != nil {
			log.Printf("Step error: %v", err)
		}
	}()
}

func (s *Server) startPeerListener() {
	listener, err := net.Listen("tcp", *peerListenAddress)
	if err != nil {
		panic(err)
	}

	go func() {
		<-s.shutdownChan
		listener.Close()
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-s.shutdownChan:
				return
			default:
				panic(err)
			}
		}

		if err := conn.(*net.TCPConn).SetNoDelay(true); err != nil {
			panic(err)
		}

		go s.handlePeerConnection(conn)
	}
}

func (s *Server) processHardState(hs raftpb.HardState) {
	if !raft.IsEmptyHardState(hs) {
		buffer := s.pool.Get().([]byte)
		_, err := hs.MarshalTo(buffer)
		if err != nil {
			panic(err)
		}
		//TODO: maybe move this to processEntries and write it alongside wal file 0
		//s.hardstateGroup.Add(1)
		//s.hardstateChan <- buffer
		//s.hardstateGroup.Wait()
	}
}

func (s *Server) processEntries(entries []raftpb.Entry) {
	if len(entries) > 0 {
		if err := s.storage.Append(entries); err != nil {
			log.Printf("Append entries error: %v", err)
		}
	}
	group := sync.WaitGroup{}
	group.Add(len(entries))
	for _, e := range entries {
		go func(entry raftpb.Entry) {
			buffer := s.pool.Get().([]byte)
			size, err := entry.MarshalTo(buffer)
			if err != nil {
				panic(err)
			}
			walIndex := entry.Index % uint64(*walFileCount)
			slot := s.walSlots[walIndex]
			slot.mutex.Lock()
			count := int64(0)
			for {
				wrote, err := slot.file.WriteAt(buffer[count:size], count)
				if err != nil {
					panic(err)
				}
				count += int64(wrote)
				if count == int64(size) {
					break
				}
			}
			if *manual == "fsync" {
				err = syscall.Fsync(int(slot.file.Fd()))
				if err != nil {
					fmt.Println("Error fsyncing file: ", err)
					return
				}
			} else if *manual == "dsync" {
				err = syscall.Fdatasync(int(slot.file.Fd()))
				if err != nil {
					fmt.Println("Error fsyncing file: ", err)
					return
				}
			}
			slot.mutex.Unlock()
			group.Done()
		}(e)
	}
	group.Wait()
}

func (s *Server) processCommittedEntries(entries []raftpb.Entry) {
	for _, entry := range entries {
		switch entry.Type {
		case raftpb.EntryConfChange:
			s.processConfChange(entry)
		case raftpb.EntryNormal:
			s.processNormalEntry(entry)
		}
	}
}

func (s *Server) processConfChange(entry raftpb.Entry) {
	var cc raftpb.ConfChange
	if err := cc.Unmarshal(entry.Data); err != nil {
		log.Printf("Unmarshal conf change error: %v", err)
		return
	}
	s.node.ApplyConfChange(cc)
}

func (s *Server) processNormalEntry(entry raftpb.Entry) {
	if len(entry.Data) >= 4 && s.isLeader {
		index := binary.LittleEndian.Uint32(entry.Data[:4])
		s.respondToClient(index)
	}
}

func (s *Server) respondToClient(index uint32) {
	senderAny, ok := s.senders.LoadAndDelete(index)
	if !ok {
		log.Printf("No sender found for index %d", index)
		return
	}

	request := senderAny.(ClientRequest)
	binary.LittleEndian.PutUint32(request.buffer[:4], 4)
	binary.LittleEndian.PutUint32(request.buffer[4:8], index)

	go func() {
		request.writeLock.Lock()
		defer request.writeLock.Unlock()
		if err := Write(*request.connection, request.buffer[:8]); err != nil {
			log.Printf("Write error: %v", err)
		}
		s.pool.Put(request.buffer)
	}()
}

func (s *Server) processMessages(msgs []raftpb.Message) {
	for _, msg := range msgs {
		s.sendMessageToPeer(msg)
	}
}

func marshalMessage(msg raftpb.Message, buffer []byte) (int, error) {
	return msg.MarshalTo(buffer)
}

func (s *Server) sendMessageToPeer(msg raftpb.Message) {
	buffer := s.pool.Get().([]byte)

	if len(msg.Snapshot.Data) > 0 {
		log.Printf("Processing snapshot of size %d", len(msg.Snapshot.Data))
	}

	requiredSize := 4 + msg.Size()
	if cap(buffer) < requiredSize {
		buffer = append(buffer, make([]byte, requiredSize-len(buffer))...)
	}

	size, err := marshalMessage(msg, buffer[4:])
	if err != nil {
		log.Printf("Marshal error: %v", err)
		return
	}

	binary.LittleEndian.PutUint32(buffer[:4], uint32(size))
	peerIdx := msg.To - 1
	connIdx := atomic.AddUint32(&s.peerConnRoundRobins[peerIdx], 1) % uint32(*numPeerConnections)
	conn := s.peerConnections[peerIdx][connIdx]

	go func() {
		conn.writeLock.Lock()
		defer conn.writeLock.Unlock()
		if err := Write(*conn.connection, buffer[:size+4]); err != nil {
			log.Printf("Write error to peer %d: %v", msg.To, err)
		}
		s.pool.Put(buffer)
	}()
}

func (s *Server) processSnapshot(snap raftpb.Snapshot) {
	if !raft.IsEmptySnap(snap) {
		log.Println("Processing snapshot")
	}
}

func (s *Server) processReady(rd raft.Ready) {
	s.processHardState(rd.HardState)
	s.processSnapshot(rd.Snapshot)
	s.processMessages(rd.Messages)
	s.processEntries(rd.Entries)
	s.processCommittedEntries(rd.CommittedEntries)
	s.node.Advance()
}

func (s *Server) run() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-s.shutdownChan:
			return
		case <-ticker.C:
			s.node.Tick()
		case rd := <-s.node.Ready():
			s.processReady(rd)
		}
	}
}

func (s *Server) Shutdown() {
	close(s.shutdownChan)
}

func NewServer() *Server {
	s := &Server{
		peerAddresses: strings.Split(*peerAddressesString, ","),
		shutdownChan:  make(chan struct{}),

		walSlots: make([]WalSlot, *walFileCount),

		hardstateChan:  make(chan []byte, 100000),
		hardstateGroup: sync.WaitGroup{},
	}

	s.initPool()
	s.warmupPool()

	for i := range *walFileCount {
		fileFlags := 0
		if *flags == "fsync" {
			fileFlags = syscall.O_FSYNC
		} else if *flags == "dsync" {
			fileFlags = syscall.O_DSYNC
		} else if *flags == "sync" {
			fileFlags = syscall.O_SYNC
		}
		f, err := os.OpenFile(strconv.Itoa(i), os.O_CREATE|os.O_RDWR|fileFlags, 0644)
		if err != nil {
			panic(err)
		}
		s.walSlots[i] = WalSlot{f, &sync.Mutex{}}
	}

	s.setupRaft()
	go s.startPeerListener()
	go s.startClientListener()
	s.setupPeerConnections()

	return s
}

func startProfiling() {
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

		pprof.StopCPUProfile()
		cpuProfile.Close()
		log.Println("CPU profiling stopped")

		memProfileFile, _ := os.Create("mem.prof")
		runtime.GC()
		pprof.WriteHeapProfile(memProfileFile)
		memProfileFile.Close()
		log.Println("Memory profile written")

		profiles := []string{"goroutine", "threadcreate", "block", "mutex"}
		for _, prof := range profiles {
			f, err := os.Create(prof + ".prof")
			if err != nil {
				log.Printf("Could not create %s profile: %v", prof, err)
				continue
			}
			if err := pprof.Lookup(prof).WriteTo(f, 0); err != nil {
				log.Printf("Error writing %s profile: %v", prof, err)
			}
			f.Close()
			log.Printf("%s profile written", prof)
		}
		os.Exit(1)
	}()
}

func server() {
	flag.Parse()
	// startProfiling()
	NewServer().run()
}
