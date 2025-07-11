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
	//	go func() {
	//		buffer := s.pool.Get().([]byte)
	//		buffer = shared.GrowSlice(buffer, uint32(msg.Size())+4)
	//		size, err := msg.MarshalTo(buffer[4:])
	//		if err != nil {
	//			return
	//		}
	//		binary.LittleEndian.PutUint32(buffer[:4], uint32(size))
	//		peerIdx := msg.To - 1
	//		connIdx := atomic.AddUint32(&s.peerConnRoundRobins[peerIdx], 1) % uint32(s.flags.NumPeerConnections)
	//		peer := s.peerConnections[peerIdx][connIdx]
	//		peer.WriteLock.Lock()
	//		if err := shared.Write(*peer.Connection, buffer[:size+4]); err != nil {
	//			log.Printf("Write error to peer %d: %v", msg.To, err)
	//		}
	//		peer.WriteLock.Unlock()
	//		s.pool.Put(buffer)
	//	}()
	//}

	var grouped = make(map[uint64][]raftpb.Message)

	for _, m := range msgs {
		grouped[m.To] = append(grouped[m.To], m)
	}

	for to, group := range grouped {
		go func(to uint64, group []raftpb.Message) {
			var offset = 8
			buffer := s.pool.Get().([]byte)
			for i := range group {
				offset += group[i].Size() + 4
			}
			buffer = shared.GrowSlice(buffer, uint32(offset))
			offset = 8
			for i := range group {
				msg := group[i]
				size, err := msg.MarshalTo(buffer[offset+4:])
				if err != nil {
					return
				}
				binary.LittleEndian.PutUint32(buffer[offset:offset+4], uint32(size))
				offset += size + 4
			}
			binary.LittleEndian.PutUint32(buffer[0:4], uint32(offset-4))
			binary.LittleEndian.PutUint32(buffer[4:8], uint32(len(group)))
			peerIdx := to - 1
			connIdx := atomic.AddUint32(&s.peerConnRoundRobins[peerIdx], 1) % uint32(s.flags.NumPeerConnections)
			peer := s.peerConnections[peerIdx][connIdx]
			peer.WriteLock.Lock()
			//fmt.Printf("Writing over: %d\n", offset)
			if err := shared.Write(*peer.Connection, buffer[:offset]); err != nil {
				log.Printf("Write error to peer %d: %v", to, err)
			}
			peer.WriteLock.Unlock()
			s.pool.Put(buffer)
		}(to, group)
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
		totalSize := binary.LittleEndian.Uint32(readBuffer[:4])
		//fmt.Printf("Total size: %d\n", totalSize)
		readBuffer = shared.GrowSlice(readBuffer, totalSize)
		if err := shared.Read(conn, readBuffer[:totalSize]); err != nil {
			return
		}
		msgCount := binary.LittleEndian.Uint32(readBuffer[:4])
		offset := uint32(4)
		for i := uint32(0); i < msgCount; i++ {
			size := binary.LittleEndian.Uint32(readBuffer[offset : offset+4])
			var msg raftpb.Message
			if err := msg.Unmarshal(readBuffer[offset+4 : offset+4+size]); err != nil {
				panic(fmt.Sprintf("Error unmarshaling message: %v", err))
			}
			offset += size + 4
			go func() {
				if err := s.node.Step(context.TODO(), msg); err != nil {
					log.Printf("Step error: %v", err)
				}
			}()
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
