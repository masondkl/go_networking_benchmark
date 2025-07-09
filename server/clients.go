package server

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/google/uuid"
	"log"
	"net"
	"networking_benchmark/shared"
	"sync"
)

func (s *Server) handleClientConnection(conn net.Conn) {
	writeLock := &sync.Mutex{}
	readBuffer := make([]byte, 1000000)
	leaderBuffer := make([]byte, 1)

	for {
		if err := shared.Read(conn, readBuffer[:4]); err != nil {
			return
		}

		fmt.Printf("Got a client message from %s\n")

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

		s.handleClientMessage(conn, writeLock, readBuffer[:amount])
	}
}

//	func (s *Server) handleClientMessage(conn net.Conn, writeLock *sync.Mutex, data []byte) {
//		//fmt.Printf("Got a message?\n")
//
//		messageId := atomic.AddUint32(&s.opIndex, 1)
//		ownerId := uint32(s.config.ID)
//		//fmt.Printf("Owner id: %d\n", ownerId)
//		//fmt.Printf("Storing with messageId %d\n", messageId)
//
//		//atomic.AddUint32(&s.poolSize, ^uint32(0))
//		//if s.leader == ownerId {
//		size := len(data) + 9
//		dataCopy := make([]byte, size)
//		//dataCopy[0] = OP_MESSAGE
//		binary.LittleEndian.PutUint32(dataCopy[1:5], messageId)
//		binary.LittleEndian.PutUint32(dataCopy[5:9], ownerId)
//		copy(dataCopy[9:size], data)
//		//fmt.Printf("Got op: %d\n", dataCopy[9])
//		s.senders.Store(messageId, shared.ClientRequest{Connection: conn, WriteLock: writeLock})
//
//		//fmt.Printf("Proposing since we are the leader?\n")
//		go func() {
//			if err := s.node.Propose(context.TODO(), dataCopy); err != nil {
//				panic(err)
//			}
//		}()
//		//} else {
//		//	bufferCopy := s.pool.Get().([]byte)
//		//	bufferCopy[4] = OP_FORWARD
//		//	binary.LittleEndian.PutUint32(bufferCopy[5:9], messageId)
//		//	binary.LittleEndian.PutUint32(bufferCopy[9:13], ownerId)
//		//	copy(bufferCopy[13:len(data)+13], data)
//		//	s.senders.Store(messageId, shared.ClientRequest{Connection: conn, WriteLock: writeLock})
//		//
//		//	peerIdx := s.leader - 1
//		//	connIdx := atomic.AddUint32(&s.peerConnRoundRobins[peerIdx], 1) % uint32(s.flags.NumPeerConnections)
//		//	conn := s.peerConnections[peerIdx][connIdx]
//		//
//		//	go func() {
//		//		writeLock.Lock()
//		//		defer writeLock.Unlock()
//		//
//		//		binary.LittleEndian.PutUint32(bufferCopy[:4], uint32(len(data)+9))
//		//		if err := shared.Write(*conn.Connection, bufferCopy[:len(data)+13]); err != nil {
//		//			log.Printf("Write error to peer %d: %v", s.leader, err)
//		//		}
//		//		s.pool.Put(bufferCopy)
//		//	}()
//		//}
//	}
func (s *Server) handleClientMessage(conn net.Conn, writeLock *sync.Mutex, data []byte) {
	messageId := uuid.New()
	ownerId := uint32(s.config.ID)

	size := len(data) + 20
	dataCopy := make([]byte, size)
	//binary.LittleEndian.PutUint32(dataCopy[1:5], messageId)
	copy(dataCopy[:], messageId[:16])
	binary.LittleEndian.PutUint32(dataCopy[16:20], ownerId)
	copy(dataCopy[20:size], data)

	//fmt.Printf("op: %d\n", dataCopy[20])
	if dataCopy[20] == shared.OP_READ || dataCopy[20] == shared.OP_READ_MEMORY {
		//fmt.Println("readindex")
		ctx := make([]byte, 16)
		copy(ctx[:], messageId[:16])
		s.senders.Store(messageId, shared.PendingRead{Connection: conn, WriteLock: writeLock, Key: dataCopy})
		go func() {
			if err := s.node.ReadIndex(context.TODO(), ctx); err != nil {
				fmt.Printf("Error reading index: %v", err)
			}
		}()
	} else {
		//fmt.Println("propose")
		s.senders.Store(messageId, shared.ClientRequest{Connection: conn, WriteLock: writeLock})
		go func() {
			if err := s.node.Propose(context.TODO(), dataCopy); err != nil {
				panic(err)
			}
		}()
	}
}

func (s *Server) respondToClient(op byte, id uuid.UUID, data []byte) {
	if op == shared.OP_READ || op == shared.OP_READ_MEMORY {
		senderAny, ok := s.senders.LoadAndDelete(id)
		//fmt.Printf("Removing index: %d\n", index)
		buffer := s.pool.Get().([]byte)
		if !ok {
			log.Printf("No sender found for id %d", id)
			return
		}
		request := senderAny.(shared.PendingRead)

		length := uint32(9 + len(data))
		buffer = shared.GrowSlice(buffer, length)
		binary.LittleEndian.PutUint32(buffer[:4], length-4)
		buffer[4] = op
		binary.LittleEndian.PutUint32(buffer[5:9], uint32(len(data)))
		copy(buffer[9:length], data)

		request.WriteLock.Lock()
		if err := shared.Write(request.Connection, buffer[:length]); err != nil {
			log.Printf("Write error: %v", err)
		}
		request.WriteLock.Unlock()
		s.pool.Put(buffer)
	} else if op == shared.OP_WRITE || op == shared.OP_WRITE_MEMORY {
		senderAny, ok := s.senders.LoadAndDelete(id)
		//fmt.Printf("Removing index: %d\n", index)
		buffer := s.pool.Get().([]byte)
		if !ok {
			log.Printf("No sender found for id %d", id)
			return
		}
		request := senderAny.(shared.ClientRequest)
		binary.LittleEndian.PutUint32(buffer[:4], 1)
		buffer[4] = op
		length := uint32(5)
		request.WriteLock.Lock()
		if err := shared.Write(request.Connection, buffer[:length]); err != nil {
			log.Printf("Write error: %v", err)
		}
		request.WriteLock.Unlock()
		s.pool.Put(buffer)
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
