package server

import (
	"context"
	"encoding/binary"
	"fmt"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"log"
	"net"
	"networking_benchmark/shared"
	"sync"
	"sync/atomic"
	"time"
)

func (s *Server) processMessages(msgs []raftpb.Message) {
	//for _, msg := range msgs {
	//	go s.sendMessageToPeer(msg)
	//}

	var grouped = make(map[uint64][]raftpb.Message)

	for _, m := range msgs {
		grouped[m.To] = append(grouped[m.To], m)
	}

	for to, group := range grouped {
		buffer := s.pool.Get().([]byte)
		nextSize := 0
		sizes := make([]int, len(group))
		for msgIndex := range group {
			sz := group[msgIndex].Size()
			nextSize += 4
			nextSize += sz
			sizes[msgIndex] = sz
		}
		buffer = shared.GrowSlice(buffer, uint32(nextSize)+8)
		offset := 8
		for msgIndex := range group {
			sz := group[msgIndex].Size()
			if sz != sizes[msgIndex] {
				log.Fatalf("%d != %d?\n", sz, sizes[msgIndex])
			}
			if offset+4+sz > len(buffer) {
				fmt.Printf("Didn't grow large enough? %d < %d\n", len(buffer), offset+4+sz)
			}
			size, err := group[msgIndex].MarshalTo(buffer[offset+4 : offset+4+sz])
			if err != nil {
				fmt.Printf("%d != %d?\n", size, sizes[msgIndex])
			} else {
				binary.LittleEndian.PutUint32(buffer[offset:], uint32(size))
				offset += size + 4
			}
		}
		go func(to uint64, group []raftpb.Message, buffer []byte) {
			binary.LittleEndian.PutUint32(buffer[0:4], uint32(offset-4))
			binary.LittleEndian.PutUint32(buffer[4:8], uint32(len(group)))
			peerIdx := to - 1
			connIdx := atomic.AddUint32(&s.peerConnRoundRobins[peerIdx], 1) % uint32(s.flags.NumPeerConnections)
			peer := s.peerConnections[peerIdx][connIdx]
			peer.WriteLock.Lock()
			if err := shared.Write(*peer.Connection, buffer[:offset]); err != nil {
				log.Printf("Write error to peer %d: %v", to, err)
			}
			peer.WriteLock.Unlock()
			s.pool.Put(buffer)
		}(to, group, buffer)
	}
}

func (s *Server) handlePeerConnection(conn net.Conn) {
	defer conn.Close()

	bytes := make([]byte, 4)
	if err := shared.Read(conn, bytes); err != nil {
		return
	}
	peerIndex := binary.LittleEndian.Uint32(bytes)
	log.Printf("Got connection from peer %d", peerIndex)

	readBuffer := make([]byte, 10000000)
	for {
		if err := shared.Read(conn, readBuffer[:4]); err != nil {
			return
		}
		amount := binary.LittleEndian.Uint32(readBuffer[:4])
		readBuffer = shared.GrowSlice(readBuffer, amount)
		if err := shared.Read(conn, readBuffer[:amount]); err != nil {
			return
		}
		msgCount := binary.LittleEndian.Uint32(readBuffer[:4])
		offset := 4
		for i := 0; i < int(msgCount); i++ {
			msgSize := binary.LittleEndian.Uint32(readBuffer[offset : offset+4])
			offset += 4
			var msg raftpb.Message
			if err := msg.Unmarshal(readBuffer[offset : offset+int(msgSize)]); err != nil {
				panic(fmt.Sprintf("Error unmarshaling message: %v", err))
			}
			if err := s.node.Step(context.TODO(), msg); err != nil {
				log.Printf("Step error: %v", err)
			}
			offset += int(msgSize)
		}
	}
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
		binary.LittleEndian.PutUint32(bytes, uint32(s.flags.NodeIndex))
		if err := shared.Write(conn, bytes); err != nil {
			panic(err)
		}

		s.peerConnections[peerIdx][connIdx] = shared.PeerConnection{
			Connection: &conn,
			WriteLock:  &sync.Mutex{},
		}
		break
	}
}

func (s *Server) setupPeerConnections() {
	numPeers := len(s.peerAddresses)
	s.peerConnections = make([][]shared.PeerConnection, numPeers)
	s.peerConnRoundRobins = make([]uint32, numPeers)

	for p := range numPeers {
		if p == s.flags.NodeIndex {
			continue
		}
		s.peerConnRoundRobins[p] = 0
		s.peerConnections[p] = make([]shared.PeerConnection, s.flags.NumPeerConnections)
		for c := range s.flags.NumPeerConnections {
			fmt.Printf("Trying to connect to peer %d\n", p)
			s.connectToPeer(p, c)
			fmt.Printf("Peer %d connected\n", p)
		}
	}
}

func (s *Server) startPeerListener() {
	listener, err := net.Listen("tcp", s.flags.PeerListenAddress)
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
