package server

import (
	"encoding/binary"
	"flag"
	"fmt"
	"github.com/google/uuid"
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
	"sync/atomic"

	//"sync/atomic"
	"syscall"
	"time"
)

type ServerFlags struct {
	NodeIndex           int
	NumPeerConnections  int
	PeerListenAddress   string
	ClientListenAddress string
	PeerAddressesString string
	WalFileCount        int
	Manual              string
	Flags               string
	Memory              bool
	FastPathWrites      bool
	NumDbs              int
	MaxDbIndex          int
}

// var OP_FORWARD = byte(0)
// var OP_MESSAGE = byte(1)

type WalSlot struct {
	file  *os.File
	mutex *sync.Mutex
}

type Pools struct {
	pool50           *shared.Pool[[]byte]
	pool1500         *shared.Pool[[]byte]
	pool15000        *shared.Pool[[]byte]
	pool50000        *shared.Pool[[]byte]
	pool150000       *shared.Pool[[]byte]
	poolMaxBatchSize *shared.Pool[[]byte]
}

func (s *Server) GetBuffer(required int) []byte {
	if required < 50 {
		return s.pools.pool50.Get()
	} else if required < 1500 {
		return s.pools.pool1500.Get()
	} else if required < 15000 {
		return s.pools.pool15000.Get()
	} else if required < 50000 {
		return s.pools.pool150000.Get()
	} else if required < 150000 {
		return s.pools.pool150000.Get()
	} else {
		return s.pools.poolMaxBatchSize.Get()
	}
}

func (s *Server) PutBuffer(buffer []byte) {
	if len(buffer) == 50 {
		s.pools.pool50.Put(buffer)
	} else if len(buffer) == 1500 {
		s.pools.pool1500.Put(buffer)
	} else if len(buffer) == 15000 {
		s.pools.pool15000.Put(buffer)
	} else if len(buffer) == 50000 {
		s.pools.pool50000.Put(buffer)
	} else if len(buffer) == 150000 {
		s.pools.pool150000.Put(buffer)
	} else {
		s.pools.poolMaxBatchSize.Put(buffer)
	}
}

type Server struct {
	senders             sync.Map
	peerAddresses       []string
	node                raft.Node
	storage             *raft.MemoryStorage
	config              *raft.Config
	peerConnections     [][]shared.PeerConnection
	peerConnRoundRobins []uint32
	walSlots            []WalSlot
	walBulkFile         *os.File
	hardstateFile       *os.File
	applyChannels       []chan []byte
	flags               *ServerFlags
	pools               *Pools
	applyIndex          uint64
	leader              uint32
	waiters             map[uint64][][]byte
	stepChannel         chan func()
	proposeChannel      chan func()
	readIndexChannel    chan func()
	commitIndex         uint32
}

var poolSize uint32

func (s *Server) initPool() {

	s.pools = &Pools{
		pool50: shared.NewPool(500000, func() []byte {
			//fmt.Printf("Creating new pool 50\n")
			return make([]byte, 50)
		}),
		pool1500: shared.NewPool(200000, func() []byte {
			//fmt.Printf("Creating new pool 1500\n")
			return make([]byte, 1500)
		}),
		pool15000: shared.NewPool(50000, func() []byte {
			//fmt.Printf("Creating new pool 15000\n")
			return make([]byte, 15000)
		}),
		pool50000: shared.NewPool(20000, func() []byte {
			//fmt.Printf("Creating new pool 50000\n")
			return make([]byte, 50000)
		}),
		pool150000: shared.NewPool(5000, func() []byte {
			//fmt.Printf("Creating new pool 150000\n")
			return make([]byte, 150000)
		}),
		poolMaxBatchSize: shared.NewPool(2500, func() []byte {
			//fmt.Printf("Creating new pool MaxBatchSize\n")
			return make([]byte, maxBatchSize)
		}),
	}

	//s.pool = sync.Pool{
	//	New: func() interface{} {
	//		return make([]byte, s.flags.PoolDataSize)
	//	},
	//}
	//
	//stored := make([][]byte, s.flags.PoolWarmupSize)
	//for i := range stored {
	//	stored[i] = s.pool.Get().([]byte)
	//}
	//for i := range stored {
	//	s.pool.Put(stored[i])
	//}

	//atomic.AddUint32(&s.poolSize, uint32(s.flags.PoolWarmupSize))

	//go func() {
	//	for {
	//		time.Sleep(250 * time.Millisecond)
	//		fmt.Printf("\npool50: %d\n", s.pools.pool50.Head)
	//		fmt.Printf("pool1500: %d\n", s.pools.pool1500.Head)
	//		fmt.Printf("pool15000: %d\n", s.pools.pool15000.Head)
	//		fmt.Printf("pool50000: %d\n", s.pools.pool50000.Head)
	//		fmt.Printf("pool150000: %d\n", s.pools.pool150000.Head)
	//		fmt.Printf("poolMaxBatchSize: %d\n", s.pools.poolMaxBatchSize.Head)
	//	}
	//}()
}

func (s *Server) setupRaft() {
	s.storage = raft.NewMemoryStorage()
	s.config = &raft.Config{
		ID:              uint64(s.flags.NodeIndex + 1),
		ElectionTick:    10,
		HeartbeatTick:   1,
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
}

func (s *Server) processHardState(hs raftpb.HardState) {
	if !raft.IsEmptyHardState(hs) {
		if !(s.flags.Memory) {
			buffer := s.GetBuffer(hs.Size())
			size, err := hs.MarshalTo(buffer)
			if err != nil {
				panic(err)
			}
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
			s.PutBuffer(buffer)
		}

		err := s.storage.SetHardState(hs)
		if err != nil {
			panic(err)
		}
	}
}

func (s *Server) processEntries(entries []raftpb.Entry) {
	if len(entries) > 0 {
		if err := s.storage.Append(entries); err != nil {
			log.Printf("Append entries error: %v", err)
		}

		if !(s.flags.Memory) {
			var grouped = make(map[uint64][]raftpb.Entry)
			group := sync.WaitGroup{}
			entryCount := 0
			for _, e := range entries {
				if e.Type == raftpb.EntryNormal {
					walIndex := e.Index % uint64(s.flags.WalFileCount)
					grouped[walIndex] = append(grouped[walIndex], e)
					entryCount++
				}
			}
			if entryCount == 0 {
				return
			}

			group.Add(len(grouped))

			for walIndex := range grouped {
				walEntries := grouped[walIndex]
				slot := s.walSlots[walIndex]
				size := 0
				for entryIndex := range walEntries {
					size += walEntries[entryIndex].Size()
				}
				buffer := s.GetBuffer(size)
				size = 0
				for entryIndex := range walEntries {
					entry := walEntries[entryIndex]
					entrySize, err := entry.MarshalTo(buffer[size:])
					if err != nil {
						panic(err)
					}
					size += entrySize
				}
				go func() {
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
					s.PutBuffer(buffer)
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

//func (s *Server) Trigger(appliedIndex uint64) {
//	//fmt.Printf("Applied index: %d\n", appliedIndex)
//	for index, keys := range s.waiters {
//		if index <= appliedIndex {
//			for _, key := range keys {
//				s.dbChannel <- key
//			}
//			delete(s.waiters, index)
//		}
//	}
//}

func (s *Server) Trigger(index uint64) {
	pendingReadRequestLock.Lock()
	toDelete := make([]int, 0)
	for readIndex, values := range pendingReadRequests {
		if readIndex <= int(index) {
			for _, value := range values {
				keySize := binary.LittleEndian.Uint32(value.Data[22:26])
				key := value.Data[26 : keySize+26]
				index, err := strconv.Atoi(string(key))
				if err != nil {
					panic(err)
				}
				s.applyChannels[(index*s.flags.NumDbs)/s.flags.MaxDbIndex] <- value.Data
			}
			toDelete = append(toDelete, readIndex)
		}
	}
	for _, value := range toDelete {
		delete(pendingReadRequests, value)
	}
	pendingReadRequestLock.Unlock()
}

func (s *Server) processNormalCommitEntry(entry raftpb.Entry) {
	if len(entry.Data) >= 8 {
		messageId := uuid.UUID(entry.Data[1:17])
		ownerIndex := binary.LittleEndian.Uint32(entry.Data[17:21])
		op := entry.Data[21]

		atomic.SwapUint32(&s.commitIndex, uint32(entry.Index))
		if op == shared.OP_WRITE || op == shared.OP_WRITE_MEMORY {
			keySize := binary.LittleEndian.Uint32(entry.Data[22:26])
			key := entry.Data[26 : keySize+26]
			index, err := strconv.Atoi(string(key))
			if err != nil {
				panic(err)
			}
			s.applyChannels[(index*s.flags.NumDbs)/s.flags.MaxDbIndex] <- entry.Data
			if s.flags.FastPathWrites && ownerIndex == uint32(s.config.ID) {
				s.respondToClient(op, messageId, nil)
			}
		}
		s.Trigger(entry.Index)
	}
}

func (s *Server) processSnapshot(snap raftpb.Snapshot) {
	if !raft.IsEmptySnap(snap) {
		log.Println("Processing snapshot")
		//
	}
}
func (s *Server) processReady(rd raft.Ready) {
	s.processHardState(rd.HardState)
	s.processSnapshot(rd.Snapshot)
	s.processEntries(rd.Entries)
	s.processMessages(rd.Messages)
	s.processCommittedEntries(rd.CommittedEntries)
	//if !raft.IsEmptyHardState(rd.HardState) && len(rd.ReadStates) > 0 {
	//	fmt.Printf("Writing to hardstate with reads: commit=%d, vote=%d, term=%d!\n", rd.HardState.Commit, rd.HardState.Vote, rd.HardState.Term)
	//}
	//s.processReadStates(rd.ReadStates)
	s.node.Advance()
}

func (s *Server) run() {
	ticker := time.NewTicker(150 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.node.Tick()
		case rd := <-s.node.Ready():
			s.processReady(rd)
		}
	}
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
		peerAddresses:    strings.Split(serverFlags.PeerAddressesString, ","),
		walSlots:         make([]WalSlot, serverFlags.WalFileCount),
		applyChannels:    make([]chan []byte, serverFlags.NumDbs),
		flags:            serverFlags,
		waiters:          make(map[uint64][][]byte),
		proposeChannel:   make(chan func(), 1000000),
		stepChannel:      make(chan func(), 1000000),
		readIndexChannel: make(chan func(), 1000000),
	}

	go func() {
		for task := range s.proposeChannel {
			task()
		}
	}()

	go func() {
		for task := range s.stepChannel {
			task()
		}
	}()

	go func() {
		for task := range s.readIndexChannel {
			task()
		}
	}()

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

	for i := range s.applyChannels {
		channel := make(chan []byte, 1000000)
		s.applyChannels[i] = channel
		go s.DbHandler(channel, i)
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

func StartServer(args []string) {
	fs := flag.NewFlagSet("server", flag.ExitOnError)
	var (
		NodeIndex           = fs.Int("node", 0, "node index")
		NumPeerConnections  = fs.Int("peer-connections", 0, "number of peer connections")
		PeerListenAddress   = fs.String("peer-listen", "", "peer listen address")
		ClientListenAddress = fs.String("client-listen", "", "client listen address")
		PeerAddressesString = fs.String("peer-addresses", "", "comma-separated peer addresses")
		WalFileCount        = fs.Int("wal-file-count", 1, "wal file count")
		Manual              = fs.String("manual", "none", "fsync, dsync, or none")
		Flags               = fs.String("flags", "none", "fsync, dsync, sync, none")
		Memory              = fs.Bool("memory", false, "use Memory")
		FastPathWrites      = fs.Bool("fast-path-writes", false, "Skip waiting to apply ")
		NumDbs              = fs.Int("num-dbs", 0, "number of bolt databases")
		MaxDbIndex          = fs.Int("max-db-index", 0, "maximum index for database to partition from")
	)

	err := fs.Parse(args)
	if err != nil {
		panic(err)
	}
	flags := &ServerFlags{
		*NodeIndex,
		*NumPeerConnections,
		*PeerListenAddress,
		*ClientListenAddress,
		*PeerAddressesString,
		*WalFileCount,
		*Manual,
		*Flags,
		*Memory,
		*FastPathWrites,
		*NumDbs,
		*MaxDbIndex,
	}
	if *NumPeerConnections <= 0 {
		log.Fatalf("-num-dbs must be > 0")
	}
	if *NumPeerConnections <= 0 {
		log.Fatalf("-max-db-index must be > 0")
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

	err = wipeWorkingDirectory()
	if err != nil {
		panic(err)
	}
	//startProfiling()
	NewServer(flags).run()
}
