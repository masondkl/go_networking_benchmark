package server

import (
	"encoding/binary"
	"flag"
	"fmt"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"log"
	"math"
	_ "net/http/pprof"
	"networking_benchmark/shared"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	//"sync/atomic"
	"syscall"
	"time"
)

type ServerFlags struct {
	NodeIndex           int
	PoolDataSize        int
	PoolWarmupSize      int
	NumPeerConnections  int
	PeerListenAddress   string
	ClientListenAddress string
	PeerAddressesString string
	WalFileCount        int
	Manual              string
	Flags               string
	Memory              bool
}

var OP_FORWARD = byte(0)
var OP_MESSAGE = byte(1)

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
	peerConnections     [][]shared.PeerConnection
	peerConnRoundRobins []uint32
	shutdownChan        chan struct{}
	walSlots            []WalSlot
	walBulkFile         *os.File
	hardstateMutex      *sync.Mutex
	hardstateFile       *os.File
	dbChannel           chan []byte
	leader              uint32
	poolSize            uint32
	flags               *ServerFlags
}

func (s *Server) initPool() {
	s.pool = sync.Pool{
		New: func() interface{} {
			return make([]byte, s.flags.PoolDataSize)
		},
	}

	stored := make([][]byte, s.flags.PoolWarmupSize)
	for i := range stored {
		stored[i] = s.pool.Get().([]byte)
	}
	for i := range stored {
		s.pool.Put(stored[i])
	}

	//atomic.AddUint32(&s.poolSize, uint32(*PoolWarmupSize))

	//go func() {
	//	for {
	//		time.Sleep(500 * time.Millisecond)
	//		fmt.Printf("Pool size: %d\n", atomic.LoadUint32(&s.poolSize))
	//	}
	//}()
}

func (s *Server) setupRaft() {
	s.storage = raft.NewMemoryStorage()
	s.config = &raft.Config{
		ID:              uint64(s.flags.NodeIndex + 1),
		ElectionTick:    10,
		HeartbeatTick:   5,
		Storage:         s.storage,
		MaxSizePerMsg:   math.MaxUint32,
		MaxInflightMsgs: 1000000,
	}

	s.leader = uint32(s.config.ID)

	fmt.Printf("Peer addresses: %v\n", s.peerAddresses)

	peers := make([]raft.Peer, len(s.peerAddresses))
	for i := range peers {
		fmt.Printf("Peer index: %d\n", i)
		peers[i].ID = uint64(i + 1)
	}

	s.node = raft.StartNode(s.config, peers)
}

func (s *Server) processHardState(hs raftpb.HardState) {
	if !raft.IsEmptyHardState(hs) {
		if !(s.flags.Memory) {
			buffer := s.pool.Get().([]byte)
			size, err := hs.MarshalTo(buffer)
			if err != nil {
				panic(err)
			}
			s.hardstateMutex.Lock()
			count := int64(0)
			for {
				wrote, err := s.hardstateFile.WriteAt(buffer[count:size], count)
				if err != nil {
					panic(err)
				}
				count += int64(wrote)
				if count == int64(size) {
					break
				}
			}
			if s.flags.Manual == "fsync" {
				fd := int(s.hardstateFile.Fd())
				err = syscall.Fsync(fd)
				if err != nil {
					fmt.Println("Error fsyncing file: ", err)
					return
				}
			} else if s.flags.Manual == "dsync" {
				fd := int(s.hardstateFile.Fd())
				err = syscall.Fdatasync(fd)
				if err != nil {
					fmt.Println("Error fsyncing file: ", err)
					return
				}
			}
			s.hardstateMutex.Unlock()
			s.pool.Put(buffer)
		}

		err := s.storage.SetHardState(hs)
		if err != nil {
			panic(err)
		}
	}
}

var grouped map[uint64][]raftpb.Entry

func (s *Server) processEntries(entries []raftpb.Entry) {
	if len(entries) > 0 {
		if err := s.storage.Append(entries); err != nil {
			log.Printf("Append entries error: %v", err)
		}
	}
	if !(s.flags.Memory) {
		group := sync.WaitGroup{}
		for _, e := range entries {
			walIndex := e.Index % uint64(s.flags.WalFileCount)
			grouped[walIndex] = append(grouped[walIndex], e)
		}

		group.Add(len(grouped))

		for walIndex := range grouped {
			walEntries := grouped[walIndex]
			slot := s.walSlots[walIndex]
			go func() {
				buffer := s.pool.Get().([]byte)
				size := 0
				for entryIndex := range walEntries {
					entry := walEntries[entryIndex]
					entrySize, err := entry.MarshalTo(buffer[size:])
					if err != nil {
						panic(err)
					}
					size += entrySize
				}

				count := 0
				for {
					wrote, err := slot.file.Write(buffer[count:size])
					if err != nil {
						panic(err)
					}
					count += wrote
					if count == size {
						break
					}
				}
				s.pool.Put(buffer)
				if s.flags.Manual == "fsync" {
					err := syscall.Fsync(int(slot.file.Fd()))
					if err != nil {
						fmt.Println("Error fsyncing file: ", err)
						return
					}
				} else if s.flags.Manual == "dsync" {
					err := syscall.Fdatasync(int(slot.file.Fd()))
					if err != nil {
						fmt.Println("Error fsyncing file: ", err)
						return
					}
				}
				group.Done()
			}()
		}
		group.Wait()

		for k := range grouped {
			delete(grouped, k)
		}
	}
}

func (s *Server) processCommittedEntries(entries []raftpb.Entry) {
	for _, entry := range entries {
		//
		switch entry.Type {
		case raftpb.EntryConfChange:
			s.processConfChange(entry)
		case raftpb.EntryNormal:
			s.processNormalCommitEntry(entry)
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

func (s *Server) processNormalCommitEntry(entry raftpb.Entry) {
	//fmt.Printf("Processing commited entry: %v\n", entry)
	if len(entry.Data) >= 8 {
		messageIndex := binary.LittleEndian.Uint32(entry.Data[1:5])
		ownerIndex := binary.LittleEndian.Uint32(entry.Data[5:9])
		//fmt.Printf("MsgIndex: %d, OwnerIndex: %d\n", messageIndex, ownerIndex)
		//dataCopy := s.pool.Get().([]byte)
		//dataCopy.
		op := entry.Data[9]
		s.dbChannel <- entry.Data
		if ownerIndex == uint32(s.config.ID) && (op == shared.OP_WRITE || op == shared.OP_WRITE_MEMORY) {
			go s.respondToClient(op, messageIndex, nil)
		}
	}
}

func (s *Server) processSnapshot(snap raftpb.Snapshot) {
	if !raft.IsEmptySnap(snap) {
		log.Println("Processing snapshot")
	}
}

func (s *Server) processReady(rd raft.Ready) {
	s.processHardState(rd.HardState)
	s.processSnapshot(rd.Snapshot)
	s.processEntries(rd.Entries)
	s.processMessages(rd.Messages)
	s.processCommittedEntries(rd.CommittedEntries)
	s.node.Advance()
}

func (s *Server) run() {
	ticker := time.NewTicker(150 * time.Millisecond)
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

func wipeWorkingDirectory() error {
	exePath, err := os.Executable()
	if err != nil {
		return fmt.Errorf("failed to get executable path: %w", err)
	}
	exeBase := filepath.Base(exePath)

	files, err := os.ReadDir(".")
	if err != nil {
		return err
	}

	for _, file := range files {
		if file.Name() == exeBase {
			continue // Skip the running executable
		}

		err := os.RemoveAll(file.Name())
		if err != nil {
			return fmt.Errorf("failed to remove %s: %w", file.Name(), err)
		}
	}

	return nil
}

func NewServer(serverFlags *ServerFlags) *Server {
	s := &Server{
		leader:         10000, //TODO: maybe change this i just need it to not be 0 instantly
		peerAddresses:  strings.Split(serverFlags.PeerAddressesString, ","),
		shutdownChan:   make(chan struct{}),
		walSlots:       make([]WalSlot, serverFlags.WalFileCount),
		hardstateMutex: &sync.Mutex{},
		dbChannel:      make(chan []byte, 10000000),
		flags:          serverFlags,
	}

	s.initPool()

	fileFlags := 0
	if serverFlags.Flags == "fsync" {
		fileFlags = syscall.O_FSYNC
	} else if serverFlags.Flags == "dsync" {
		fileFlags = syscall.O_DSYNC
	} else if serverFlags.Flags == "sync" {
		fileFlags = syscall.O_SYNC
	}

	for i := range serverFlags.WalFileCount {
		f, err := os.OpenFile(strconv.Itoa(i), os.O_CREATE|os.O_RDWR|os.O_APPEND|fileFlags, 0644)
		if err != nil {
			panic(err)
		}
		s.walSlots[i] = WalSlot{f, &sync.Mutex{}}
	}

	hardstateFile, err := os.OpenFile("hardstate", os.O_CREATE|os.O_RDWR|fileFlags, 0644)
	if err != nil {
		panic(err)
	}
	s.hardstateFile = hardstateFile

	bulkFile, err := os.OpenFile("bulk", os.O_CREATE|os.O_RDWR|os.O_APPEND|fileFlags, 0644)
	if err != nil {
		panic(err)
	}
	s.walBulkFile = bulkFile

	go s.DbHandler()

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

func StartServer(args []string) {
	fs := flag.NewFlagSet("server", flag.ExitOnError)
	var (
		NodeIndex           = fs.Int("node", 0, "node index")
		PoolDataSize        = fs.Int("pool-data-size", 0, "pool data size")
		PoolWarmupSize      = fs.Int("pool-warmup-size", 0, "pool warmup size")
		NumPeerConnections  = fs.Int("peer-connections", 0, "number of peer connections")
		PeerListenAddress   = fs.String("peer-listen", "", "peer listen address")
		ClientListenAddress = fs.String("client-listen", "", "client listen address")
		PeerAddressesString = fs.String("peer-addresses", "", "comma-separated peer addresses")
		WalFileCount        = fs.Int("wal-file-count", 1, "wal file count")
		Manual              = fs.String("manual", "none", "fsync, dsync, or none")
		Flags               = fs.String("flags", "none", "fsync, dsync, sync, none")
		Memory              = fs.Bool("memory", false, "use Memory")
	)
	err := fs.Parse(args)
	if err != nil {
		panic(err)
	}
	flags := &ServerFlags{
		*NodeIndex,
		*PoolDataSize,
		*PoolWarmupSize,
		*NumPeerConnections,
		*PeerListenAddress,
		*ClientListenAddress,
		*PeerAddressesString,
		*WalFileCount,
		*Manual,
		*Flags,
		*Memory,
	}

	if *PoolDataSize <= 0 {
		log.Fatalf("-pool-data-size must be > 0")
	}
	if *PoolWarmupSize < 0 {
		log.Fatalf("-pool-warmup-size cannot be negative")
	}
	if *NumPeerConnections <= 0 {
		log.Fatalf("-peer-connections must be > 0")
	}
	if *PeerListenAddress == "" {
		log.Fatalf("-peer-listen is required")
	}
	if *ClientListenAddress == "" {
		log.Fatalf("-client-listen is required")
	}
	if *PeerAddressesString == "" {
		log.Fatalf("-peer-addresses is required (comma‑separated list)")
	}
	peers := strings.Split(*PeerAddressesString, ",")
	if *NodeIndex < 0 || *NodeIndex >= len(peers) {
		log.Fatalf("-node (%d) out of range; only %d peer addresses supplied",
			*NodeIndex, len(peers))
	}
	if *WalFileCount <= 0 {
		log.Fatalf("-wal-file-count must be > 0")
	}
	switch *Manual {
	case "none", "fsync", "dsync":
	default:
		log.Fatalf(`-manual must be "none", "fsync", or "dsync" (got %q)`, *Manual)
	}
	switch *Flags {
	case "none", "fsync", "dsync", "sync":
	default:
		log.Fatalf(`-flags must be "none", "fsync", "dsync", or "sync" (got %q)`, *Flags)
	}
	if *Manual != "none" && *Flags != "none" {
		log.Fatalf("choose either -manual or -flags (not both)")
	}

	grouped = make(map[uint64][]raftpb.Entry)
	err = wipeWorkingDirectory()
	if err != nil {
		panic(err)
	}
	SetupKeyBucket()
	//startProfiling()
	NewServer(flags).run()
}
