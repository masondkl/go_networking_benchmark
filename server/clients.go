package server

import (
	"context"
	"encoding/binary"
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
	readBuffer := make([]byte, maxBatchSize)
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
	//fmt.Printf("Storing messageId: %v\n", messageId)

	op := data[0]

	if op == shared.OP_READ_MEMORY || op == shared.OP_READ {
		dataCopy := make([]byte, size)
		dataCopy[0] = shared.OP_MESSAGE
		copy(dataCopy[1:17], messageId[:16])
		binary.LittleEndian.PutUint32(dataCopy[17:21], ownerId)
		copy(dataCopy[21:size], data)

		s.senders.Store(messageId, shared.PendingRead{Client: client, Data: dataCopy})
		size = 21
		buffer := s.GetBuffer(size)
		binary.LittleEndian.PutUint32(buffer[0:4], uint32(size))
		copy(buffer[5:21], messageId[:16])

		if s.leader == ownerId {
			buffer[4] = shared.OP_READ_INDEX_REQ

			readIndexLock.Lock()
			//fmt.Printf("Storing readIndex 1 with messageId: %v\n", messageId)
			readIndexes[messageId] = &ReadIndexStore{Acks: 1, Proposer: ownerId}
			readIndexLock.Unlock()
			for peerIndex := range s.peerAddresses {
				if peerIndex == int(s.config.ID-1) {
					continue
				}
				connIdx := atomic.AddUint32(&s.peerConnRoundRobins[peerIndex], 1) % uint32(s.flags.NumPeerConnections)
				peer := s.peerConnections[peerIndex][connIdx]
				bufferCopy := s.GetBuffer(21)
				copy(bufferCopy, buffer[:21])
				peer.Channel <- func() {
					//fmt.Printf("Sending out request!\n")
					if err := shared.Write(*peer.Connection, bufferCopy[:21]); err != nil {
						log.Printf("Write error to peer %d: %v", s.leader, err)
					}
					s.PutBuffer(bufferCopy)
				}
			}
			s.PutBuffer(buffer)
		} else {
			buffer[4] = shared.OP_READ_INDEX_FORWARD
			connIdx := atomic.AddUint32(&s.peerConnRoundRobins[s.leader-1], 1) % uint32(s.flags.NumPeerConnections)
			peer := s.peerConnections[s.leader-1][connIdx]

			peer.Channel <- func() {
				if err := shared.Write(*peer.Connection, buffer[:size]); err != nil {
					log.Printf("Write error to peer %d: %v", s.leader, err)
				}
				s.PutBuffer(buffer)
			}
		}
	} else {
		s.senders.Store(messageId, client)
		if s.leader == ownerId {
			dataCopy := make([]byte, size)
			dataCopy[0] = shared.OP_MESSAGE
			copy(dataCopy[1:17], messageId[:16])
			binary.LittleEndian.PutUint32(dataCopy[17:21], ownerId)
			copy(dataCopy[21:size], data)
			client.ProposeChannel <- func() {
				if err := s.node.Propose(context.TODO(), dataCopy[:size]); err != nil {
					log.Printf("Propose error: %v", err)
				}
			}
		} else {
			//fmt.Printf("Forwarding message\n")
			size += 4
			buffer := s.GetBuffer(size)
			buffer[4] = shared.OP_FORWARD
			binary.LittleEndian.PutUint32(buffer[0:4], uint32(size))
			copy(buffer[5:21], messageId[:16])
			binary.LittleEndian.PutUint32(buffer[21:25], ownerId)
			copy(buffer[25:size], data)

			connIdx := atomic.AddUint32(&s.peerConnRoundRobins[s.leader-1], 1) % uint32(s.flags.NumPeerConnections)
			peer := s.peerConnections[s.leader-1][connIdx]

			peer.Channel <- func() {
				if err := shared.Write(*peer.Connection, buffer[:size]); err != nil {
					log.Printf("Write error to peer %d: %v", s.leader, err)
				}
				s.PutBuffer(buffer)
			}
		}
	}
}

func (s *Server) respondToClient(op byte, id uuid.UUID, data []byte) {
	if op == shared.OP_READ || op == shared.OP_READ_MEMORY {
		senderAny, ok := s.senders.LoadAndDelete(id)
		//fmt.Printf("Removing index: %d\n", index)
		if !ok {
			log.Printf("No sender found for read id %v", id)
			return
		}
		//fmt.Printf("Deleted read messageid: %v\n", id)
		request := senderAny.(shared.PendingRead)
		length := 9 + len(data)
		buffer := s.GetBuffer(length)

		binary.LittleEndian.PutUint32(buffer[:4], uint32(length)-4)
		buffer[4] = op
		binary.LittleEndian.PutUint32(buffer[5:9], uint32(len(data)))
		copy(buffer[9:length], data)

		request.Client.Channel <- func() {
			if err := shared.Write(request.Client.Connection, buffer[:length]); err != nil {
				log.Printf("Write error: %v", err)
			}
			s.PutBuffer(buffer)
		}
	} else if op == shared.OP_WRITE || op == shared.OP_WRITE_MEMORY {
		senderAny, ok := s.senders.LoadAndDelete(id)
		if !ok {
			log.Printf("No sender found for write id %v", id)
			return
		}
		//fmt.Printf("Deleted write messageid: %v\n", id)
		request := senderAny.(shared.Client)
		request.Channel <- func() {
			buffer := s.GetBuffer(5)
			binary.LittleEndian.PutUint32(buffer[:4], 1)
			buffer[4] = op
			length := uint32(5)
			if err := shared.Write(request.Connection, buffer[:length]); err != nil {
				log.Printf("Write error: %v", err)
			}
			s.PutBuffer(buffer)
		}
	}
}

func (s *Server) startClientListener() {
	listener, err := net.Listen("tcp", s.flags.ClientListenAddress)
	if err != nil {
		panic(err)
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			panic(err)
		}

		if err := conn.(*net.TCPConn).SetNoDelay(true); err != nil {
			panic(err)
		}

		go s.handleClientConnection(conn)
	}
}
