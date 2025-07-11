package server

import "C"
import (
	"context"
	"encoding/binary"
	"fmt"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"log"
	"net"
	"networking_benchmark/shared"
	"sync/atomic"
	"time"
	"unsafe"
)

func cAlloc(size int) []byte {
	ptr := C.malloc(C.size_t(size))
	return (*[1 << 30]byte)(unsafe.Pointer(ptr))[:size:size]
}

func cFree(b []byte) {
	C.free(unsafe.Pointer(&b[0]))
}

type MessageBatch struct {
	TotalSize int
	Messages  []raftpb.Message
}

var maxBatchSize = (1024 * 1024) * 50

var grouped map[uint64][]*MessageBatch

func (s *Server) processMessages(msgs []raftpb.Message) {
	//test := make([]uint64, 4)

	for i := range msgs {
		msg := msgs[i]
		msgSize := msg.Size() + 4
		batches, exists := grouped[msg.To]
		if !exists {
			batches = []*MessageBatch{{
				TotalSize: 0,
				Messages:  make([]raftpb.Message, 0),
			}}
			grouped[msg.To] = batches
		}

		lastBatch := batches[len(batches)-1]
		if lastBatch.TotalSize+msgSize > maxBatchSize {
			grouped[msg.To] = append(batches, &MessageBatch{
				TotalSize: msgSize,
				Messages:  []raftpb.Message{msg},
			})
		} else {
			lastBatch.Messages = append(lastBatch.Messages, msg)
			lastBatch.TotalSize += msgSize
		}
	}
	//
	//for i := range test {
	//	if test[i] > 1 {
	//		fmt.Printf("Sending multiple messages: %d\n", test[i])
	//	}
	//}

	for recipient, batches := range grouped {
		for _, batch := range batches {
			maxSize := batch.TotalSize + 9
			buffer := s.GetBuffer(maxSize)
			binary.LittleEndian.PutUint32(buffer[0:4], uint32(maxSize))
			buffer[4] = shared.OP_MESSAGE
			binary.LittleEndian.PutUint32(buffer[5:9], uint32(len(batch.Messages)))
			//fmt.Printf("Sending %d messages to %d\n", len(batch.Messages), recipient)
			offset := 9
			for _, msg := range batch.Messages {
				msgSize := msg.Size()
				binary.LittleEndian.PutUint32(buffer[offset:offset+4], uint32(msgSize))
				size, err := msg.MarshalTo(buffer[offset+4:])
				if err != nil {
					panic(err)
				}

				if size != msg.Size() {
					fmt.Printf("Message size is different: %d - %d\n", size, msgSize)
				}

				offset += msgSize + 4
			}

			peerIdx := recipient - 1
			connIdx := atomic.AddUint32(&s.peerConnRoundRobins[peerIdx], 1) % uint32(s.flags.NumPeerConnections)
			peer := s.peerConnections[peerIdx][connIdx]
			peer.Channel <- func() {
				if err := shared.Write(*peer.Connection, buffer[:maxSize]); err != nil {
					log.Printf("Write error to peer %d: %v", peerIdx+1, err)
				}
				s.PutBuffer(buffer)
			}
		}
	}

	for k := range grouped {
		delete(grouped, k)
	}
}

func (s *Server) handlePeerConnection(conn net.Conn) {
	defer conn.Close()

	stepChannel := make(chan func(), 1000000)

	go func() {
		for step := range stepChannel {
			step()
		}
	}()

	bytes := make([]byte, 4)
	if err := shared.Read(conn, bytes); err != nil {
		return
	}
	peerIndex := binary.LittleEndian.Uint32(bytes)
	log.Printf("Got connection from peer %d", peerIndex)

	readBuffer := make([]byte, maxBatchSize+1000)
	for {
		if err := shared.Read(conn, readBuffer[:4]); err != nil {
			return
		}
		totalSize := binary.LittleEndian.Uint32(readBuffer[:4]) - 4
		if err := shared.Read(conn, readBuffer[:totalSize]); err != nil {
			return
		}

		//fmt.Printf("\nRead total size: %d\n", totalSize)

		op := readBuffer[0]
		if op == shared.OP_FORWARD {
			//fmt.Printf("Got forward from peer %d\n", peerIndex)
			//buffer := s.GetBuffer(int(totalSize))
			buffer := make([]byte, totalSize)
			copy(buffer, readBuffer[:totalSize])
			s.proposeChannel <- func() {
				if err := s.node.Propose(context.TODO(), buffer[:totalSize]); err != nil {
					log.Printf("Propose error: %v", err)
				}
			}
		} else if op == shared.OP_MESSAGE {
			//fmt.Printf("Got message from peer %d\n", peerIndex)
			batchSize := binary.LittleEndian.Uint32(readBuffer[1:5])
			//fmt.Printf("Got message from peer %d (%d messages)\n", peerIndex, batchSize)
			offset := uint32(5)
			for range batchSize {
				msgSize := binary.LittleEndian.Uint32(readBuffer[offset : offset+4])
				offset += 4
				var msg raftpb.Message
				if err := msg.Unmarshal(readBuffer[offset : offset+msgSize]); err != nil {
					panic(fmt.Sprintf("Error unmarshaling message: %v", err))
				}
				offset += msgSize
				stepChannel <- func() {
					if msg.Type == raftpb.MsgHeartbeat {
						s.leader = uint32(msg.From)
					} else if msg.Type == raftpb.MsgHeartbeatResp {
						s.leader = uint32(s.config.ID)
					}

					if err := s.node.Step(context.TODO(), msg); err != nil {
						log.Printf("Step error: %v", err)
					}
				}
			}
		} else if op == shared.OP_READ_INDEX {

		} else if op == shared.OP_READ_INDEX_RESP {

		} else {
			panic(fmt.Sprintf("Unknown op: %v", op))
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

		peerConn := shared.PeerConnection{
			Connection: &conn,
			Channel:    make(chan func(), 1000000),
		}

		go func() {
			for task := range peerConn.Channel {
				task()
			}
		}()
		s.peerConnections[peerIdx][connIdx] = peerConn
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

	for {
		conn, err := listener.Accept()
		if err != nil {
			panic(err)
		}

		if err := conn.(*net.TCPConn).SetNoDelay(true); err != nil {
			panic(err)
		}

		go s.handlePeerConnection(conn)
	}
}
