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
	for _, msg := range msgs {
		if msg.Type == raftpb.MsgHeartbeatResp {
			s.leader = uint32(msg.To)
		} else if msg.Type == raftpb.MsgHeartbeat {
			s.leader = uint32(s.config.ID)
		} else {
			//fmt.Printf("Processing message: %+v\n", msg)
		}
		go s.sendMessageToPeer(msg)
	}
}

func (s *Server) sendMessageToPeer(msg raftpb.Message) {
	buffer := s.pool.Get().([]byte)
	atomic.AddUint32(&s.poolSize, ^uint32(0))

	if len(msg.Snapshot.Data) > 0 {
		log.Printf("Processing snapshot of size %d", len(msg.Snapshot.Data))
	}

	requiredSize := 5 + msg.Size()
	if cap(buffer) < requiredSize {
		buffer = append(buffer, make([]byte, requiredSize-len(buffer))...)
		buffer = buffer[:cap(buffer)]
	}

	//fmt.Printf("Sending message to peer %+v\n", msg)
	buffer[4] = OP_MESSAGE
	size, err := msg.MarshalTo(buffer[5:])
	if err != nil {
		log.Printf("Marshal error: %v", err)
		return
	}

	binary.LittleEndian.PutUint32(buffer[0:4], uint32(size+1))
	peerIdx := msg.To - 1
	connIdx := atomic.AddUint32(&s.peerConnRoundRobins[peerIdx], 1) % uint32(*numPeerConnections)
	conn := s.peerConnections[peerIdx][connIdx]

	conn.WriteLock.Lock()
	defer conn.WriteLock.Unlock()
	if err := shared.Write(*conn.Connection, buffer[:size+5]); err != nil {
		log.Printf("Write error to peer %d: %v", msg.To, err)
	}
	s.pool.Put(buffer)
	atomic.AddUint32(&s.poolSize, 1)
}

func (s *Server) handlePeerMessage(data []byte, amount int) {
	op := data[0]
	if op == OP_FORWARD {
		data[0] = OP_MESSAGE
		dataCopy := make([]byte, amount)
		copy(dataCopy, data[:amount])

		if err := s.node.Propose(context.TODO(), dataCopy); err != nil {
			panic(err)
		}
		s.pool.Put(data)
		atomic.AddUint32(&s.poolSize, 1)
	} else if op == OP_MESSAGE {
		var msg raftpb.Message
		if err := msg.Unmarshal(data[1:amount]); err != nil {
			panic(fmt.Sprintf("Error unmarshaling message: %v", err))
		}

		if err := s.node.Step(context.TODO(), msg); err != nil {
			log.Printf("Step error: %v", err)
		}
		s.pool.Put(data)
		atomic.AddUint32(&s.poolSize, 1)
	} else {
		panic(fmt.Sprintf("Unknown op: %v\n", op))
	}
}

var a = uint32(0)

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

		if cap(readBuffer) < int(amount) {
			readBuffer = append(readBuffer, make([]byte, int(amount)-cap(readBuffer))...)
			readBuffer = readBuffer[:cap(readBuffer)]
		}

		if err := shared.Read(conn, readBuffer[:amount]); err != nil {
			return
		}

		bufferCopy := s.pool.Get().([]byte)
		atomic.AddUint32(&s.poolSize, ^uint32(0))
		if cap(bufferCopy) < int(amount) {
			bufferCopy = append(bufferCopy, make([]byte, int(amount)-cap(bufferCopy))...)
			bufferCopy = bufferCopy[:cap(bufferCopy)]
		}

		copy(bufferCopy[:amount], readBuffer[:amount])
		go s.handlePeerMessage(bufferCopy, int(amount))
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
		binary.LittleEndian.PutUint32(bytes, uint32(*nodeIndex))
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
		if p == *nodeIndex {
			continue
		}
		s.peerConnRoundRobins[p] = 0
		s.peerConnections[p] = make([]shared.PeerConnection, *numPeerConnections)
		for c := range *numPeerConnections {
			fmt.Printf("Trying to connect to peer %d\n", p)
			s.connectToPeer(p, c)
			fmt.Printf("Peer %d connected\n", p)
		}
	}
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
