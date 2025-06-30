package server

import (
	"context"
	"encoding/binary"
	"log"
	"net"
	"networking_benchmark/shared"
	"sync"
	"sync/atomic"
)

func (s *Server) handleClientConnection(conn net.Conn) {
	writeLock := &sync.Mutex{}
	readBuffer := make([]byte, 1000000)

	for {
		if err := shared.Read(conn, readBuffer[:4]); err != nil {
			return
		}

		amount := binary.LittleEndian.Uint32(readBuffer[:4])
		if err := shared.Read(conn, readBuffer[:amount]); err != nil {
			log.Printf("Error reading message: %v", err)
			return
		}

		s.handleClientMessage(conn, writeLock, readBuffer[:amount])
	}
}

func (s *Server) handleClientMessage(conn net.Conn, writeLock *sync.Mutex, data []byte) {
	//fmt.Printf("Got a message?\n")
	messageId := atomic.AddUint32(&s.opIndex, 1)
	ownerId := uint32(s.config.ID)
	//fmt.Printf("Owner id: %d\n", ownerId)
	//fmt.Printf("Storing with messageId %d\n", messageId)

	//atomic.AddUint32(&s.poolSize, ^uint32(0))
	if s.leader == ownerId {
		size := len(data) + 9
		dataCopy := make([]byte, size)
		dataCopy[0] = OP_MESSAGE
		binary.LittleEndian.PutUint32(dataCopy[1:5], messageId)
		binary.LittleEndian.PutUint32(dataCopy[5:9], ownerId)
		copy(dataCopy[9:size], data)
		s.senders.Store(messageId, shared.ClientRequest{Connection: conn, WriteLock: writeLock})

		//fmt.Printf("Proposing since we are the leader?\n")
		go func() {
			if err := s.node.Propose(context.TODO(), dataCopy); err != nil {
				panic(err)
			}
		}()
	} else {
		// propose 0, 0
		// propose 0, 1
		// commit 0
		// error commit 0
		bufferCopy := s.pool.Get().([]byte)
		bufferCopy[4] = OP_FORWARD
		binary.LittleEndian.PutUint32(bufferCopy[5:9], messageId)
		binary.LittleEndian.PutUint32(bufferCopy[9:13], ownerId)
		copy(bufferCopy[13:len(data)+13], data)
		s.senders.Store(messageId, shared.ClientRequest{Connection: conn, WriteLock: writeLock})

		peerIdx := s.leader - 1
		connIdx := atomic.AddUint32(&s.peerConnRoundRobins[peerIdx], 1) % uint32(*numPeerConnections)
		conn := s.peerConnections[peerIdx][connIdx]

		go func() {
			writeLock.Lock()
			defer writeLock.Unlock()

			binary.LittleEndian.PutUint32(bufferCopy[:4], uint32(len(data)+9))
			if err := shared.Write(*conn.Connection, bufferCopy[:len(data)+13]); err != nil {
				log.Printf("Write error to peer %d: %v", s.leader, err)
			}
			s.pool.Put(bufferCopy)
		}()
	}
}

func (s *Server) respondToClient(index uint32) {
	senderAny, ok := s.senders.LoadAndDelete(index)
	//fmt.Printf("Removing index: %d\n", index)
	respondBuffer := s.pool.Get().([]byte)
	if !ok {
		log.Printf("No sender found for index %d", index)
		return
	}

	request := senderAny.(shared.ClientRequest)
	binary.LittleEndian.PutUint32(respondBuffer[:4], 4)
	binary.LittleEndian.PutUint32(respondBuffer[4:8], index)

	request.WriteLock.Lock()
	defer request.WriteLock.Unlock()
	if err := shared.Write(request.Connection, respondBuffer[:8]); err != nil {
		log.Printf("Write error: %v", err)
	}
	s.pool.Put(respondBuffer)
	//atomic.AddUint32(&s.poolSize, 1)
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
