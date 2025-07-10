package server

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/google/uuid"
	"log"
	"net"
	"networking_benchmark/shared"
	"sync/atomic"
)

type raftRequest struct {
	ctx    context.Context
	data   []byte // For Propose: proposal data; For ReadIndex: ctx data
	isRead bool   // true for ReadIndex, false for Propose
}

func (s *Server) handleClientConnection(conn net.Conn) {
	readBuffer := make([]byte, 1000000)
	leaderBuffer := make([]byte, 1)

	client := shared.Client{
		Connection:     conn,
		Channel:        make(chan func(), 1000000),
		ProposeChannel: make(chan func(), 100000),
	}

	go func() {
		for task := range client.Channel {
			task()
		}
	}()

	go func() {
		for task := range client.ProposeChannel {
			task()
		}
	}()

	for {
		if err := shared.Read(conn, readBuffer[:4]); err != nil {
			return
		}

		amount := binary.LittleEndian.Uint32(readBuffer[:4])
		if err := shared.Read(conn, readBuffer[:amount]); err != nil {
			log.Printf("Error reading message: %v", err)
			return
		}

		if readBuffer[0] == shared.OP_LEADER {
			if s.node.Status().Lead == s.config.ID {
				leaderBuffer[0] = 1
			} else {
				leaderBuffer[0] = 0
			}
			err := shared.Write(conn, leaderBuffer)
			if err != nil {
				panic(err)
			}
			continue
		}

		s.handleClientMessage(client, readBuffer[:amount])
	}
}

func (s *Server) handleClientMessage(client shared.Client, data []byte) {
	messageId := uuid.New()
	ownerId := uint32(s.config.ID)

	size := len(data) + 21
	s.senders.Store(messageId, client)
	fmt.Printf("Leader: %d - %d\n", s.leader, s.config.ID)
	if s.leader-1 == ownerId {
		dataCopy := make([]byte, size)
		dataCopy[0] = shared.OP_MESSAGE
		copy(dataCopy[1:17], messageId[:16])
		binary.LittleEndian.PutUint32(dataCopy[17:21], ownerId)
		copy(dataCopy[21:size], data)
		fmt.Printf("Proposing since we are the leader\n")
		s.proposeChannel <- func() {
			if err := s.node.Propose(context.TODO(), dataCopy[:size]); err != nil {
				log.Printf("Propose error: %v", err)
			}
		}
	} else {
		buffer := s.pool.Get().([]byte)
		buffer = shared.GrowSlice(buffer, uint32(size))
		buffer[0] = shared.OP_FORWARD
		copy(buffer[1:17], messageId[:16])
		binary.LittleEndian.PutUint32(buffer[17:21], ownerId)
		copy(buffer[21:size], data)

		connIdx := atomic.AddUint32(&s.peerConnRoundRobins[s.leader-1], 1) % uint32(s.flags.NumPeerConnections)
		peer := s.peerConnections[s.leader-1][connIdx]

		peer.Channel <- func() {
			if err := shared.Write(*peer.Connection, buffer[:size]); err != nil {
				log.Printf("Write error to peer %d: %v", s.leader, err)
			}
			atomic.AddUint32(&s.poolSize, 1)
			s.pool.Put(buffer)
		}
	}
}

func (s *Server) respondToClient(op byte, id uuid.UUID, data []byte) {
	if op == shared.OP_READ || op == shared.OP_READ_MEMORY {
		senderAny, ok := s.senders.LoadAndDelete(id)
		//fmt.Printf("Removing index: %d\n", index)
		if !ok {
			log.Printf("No sender found for id %d", id)
			return
		}
		request := senderAny.(shared.PendingRead)

		buffer := s.pool.Get().([]byte)
		atomic.AddUint32(&s.poolSize, ^uint32(0))
		length := uint32(9 + len(data))
		buffer = shared.GrowSlice(buffer, length)
		binary.LittleEndian.PutUint32(buffer[:4], length-4)
		buffer[4] = op
		binary.LittleEndian.PutUint32(buffer[5:9], uint32(len(data)))
		copy(buffer[9:length], data)

		request.Client.Channel <- func() {
			if err := shared.Write(request.Client.Connection, buffer[:length]); err != nil {
				log.Printf("Write error: %v", err)
			}
			atomic.AddUint32(&s.poolSize, uint32(1))
			s.pool.Put(buffer)
		}
	} else if op == shared.OP_WRITE || op == shared.OP_WRITE_MEMORY {
		senderAny, ok := s.senders.LoadAndDelete(id)
		if !ok {
			log.Printf("No sender found for id %d", id)
			return
		}
		request := senderAny.(shared.Client)
		request.Channel <- func() {
			buffer := s.pool.Get().([]byte)
			atomic.AddUint32(&s.poolSize, ^uint32(0))
			binary.LittleEndian.PutUint32(buffer[:4], 1)
			buffer[4] = op
			length := uint32(5)
			if err := shared.Write(request.Connection, buffer[:length]); err != nil {
				log.Printf("Write error: %v", err)
			}
			atomic.AddUint32(&s.poolSize, uint32(1))
			s.pool.Put(buffer)
		}
	}
}

func (s *Server) startClientListener() {
	listener, err := net.Listen("tcp", s.flags.ClientListenAddress)
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
