package server

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
)

//
//func getConfStateSize(state raftpb.ConfState) int {
//
//}
//
//// ConfState ConfState `protobuf:"bytes,1,opt,name=conf_state,json=confState" json:"conf_state"`
//// Index     uint64    `protobuf:"varint,2,opt,name=index" json:"index"`
//// Term      uint64    `protobuf:"varint,3,opt,name=term" json:"term"`
//func getSnapshotMetadataSize(metadata raftpb.SnapshotMetadata) int {
//	return 16 + getConfStateSize(metadata.ConfState)
//}
//
//// Data     []byte           `protobuf:"bytes,1,opt,name=data" json:"data,omitempty"`
//// Metadata SnapshotMetadata `protobuf:"bytes,2,opt,name=metadata" json:"metadata"`
//func getSnapshotSize(snapshot raftpb.Snapshot) int {
//	return len(snapshot.Data) + getSnapshotMetadataSize(snapshot.Metadata)
//}
//
//// Term  uint64    `protobuf:"varint,2,opt,name=Term" json:"Term"`
//// Index uint64    `protobuf:"varint,3,opt,name=Index" json:"Index"`
//// Type  EntryType `protobuf:"varint,1,opt,name=Type,enum=raftpb.EntryType" json:"Type"`
//// Data  []byte    `protobuf:"bytes,4,opt,name=Data" json:"Data,omitempty"`
//func getEntrySize(entry raftpb.Entry) int {
//	return 17 + len(entry.Data)
//}
//
//func getMessageSize(message raftpb.Message) int {
//	contextSize := len(message.Context)
//
//}

func (s *Server) processMessages(msgs []raftpb.Message) {
	//for _, msg := range msgs {
	//	//go func() {
	//
	//	//fmt.Printf("sending to %d, index=%d commit=%d size=%d entries=%d type=%v\n", msg.To, msg.Index, msg.Commit, msg.Size()+4, len(msg.Entries), msg.Type)
	//	//fmt.Printf("sending to %d - %d, %d %d %v\n", msg.To, msg.Index, msg.Size()+4, len(msg.Entries), msg.Type)
	//	buffer := s.pool.Get().([]byte)
	//	buffer = shared.GrowSlice(buffer, uint32(msg.Size())+4)
	//	size, err := msg.MarshalTo(buffer[4:])
	//	if err != nil {
	//		return
	//	}
	//	binary.LittleEndian.PutUint32(buffer[:4], uint32(size))
	//	peerIdx := msg.To - 1
	//	connIdx := atomic.AddUint32(&s.peerConnRoundRobins[peerIdx], 1) % uint32(s.flags.NumPeerConnections)
	//	peer := s.peerConnections[peerIdx][connIdx]
	//	p

	//
	//	r[:size+4]); err != nil {
	//
	//	sg.To, err)
	//		}

	//		s.pool.Put(buffer)

	//	}
	//	//}()
	//}

	var grouped = make(map[uint64][]raftpb.Message)

	for _, m := range msgs {
		grouped[m.To] = append(grouped[m.To], m)
	}

	//fmt.Printf("processing %d messages\n", len(msgs))
	//
	for to, group := range grouped {
		buffer := s.pool.Get().([]byte)
		atomic.AddUint32(&s.poolSize, ^uint32(0))
		offset := 9

		for i := range group {
			msg := group[i]
			buffer = shared.GrowSlice(buffer, uint32(offset+4+msg.Size()))
			size, err := msg.MarshalTo(buffer[offset+4:])
			if err != nil {
				log.Printf("Marshal error to peer %d: %v", to, err)
				s.pool.Put(buffer)
				return
			}
			binary.LittleEndian.PutUint32(buffer[offset:offset+4], uint32(size))
			offset += size + 4
		}

		binary.LittleEndian.PutUint32(buffer[0:4], uint32(offset-4))
		buffer[4] = shared.OP_MESSAGE
		binary.LittleEndian.PutUint32(buffer[5:9], uint32(len(group)))

		peerIdx := to - 1
		connIdx := atomic.AddUint32(&s.peerConnRoundRobins[peerIdx], 1) % uint32(s.flags.NumPeerConnections)
		peer := s.peerConnections[peerIdx][connIdx]
		peer.Channel <- func() {
			//fmt.Printf("Writing out message to %d: size=%d\n", to, offset)
			if err := shared.Write(*peer.Connection, buffer[:offset]); err != nil {
				log.Printf("Write error to peer %d: %v", to, err)
			}
			atomic.AddUint32(&s.poolSize, 1)
			s.pool.Put(buffer)
		}
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
		fmt.Printf("Read size: %d\n", totalSize)
		//fmt.Printf("Total size: %d\n", totalSize)
		readBuffer = shared.GrowSlice(readBuffer, totalSize)
		if err := shared.Read(conn, readBuffer[:totalSize]); err != nil {
			return
		}

		op := readBuffer[0]
		//fmt.Printf("Got op: %d\n", op)
		if op == shared.OP_FORWARD {
			fmt.Printf("Got forward from peer %d, %d\n", peerIndex, totalSize)
			dataCopy := make([]byte, totalSize)
			copy(dataCopy, readBuffer[:totalSize])
			s.proposeChannel <- func() {
				if err := s.node.Propose(context.TODO(), dataCopy); err != nil {
					log.Printf("Propose error: %v", err)
				}
			}
		} else if op == shared.OP_MESSAGE {
			msgCount := binary.LittleEndian.Uint32(readBuffer[1:5])

			offset := uint32(5)
			for i := uint32(0); i < msgCount; i++ {
				size := binary.LittleEndian.Uint32(readBuffer[offset : offset+4])
				var msg raftpb.Message
				if err := msg.Unmarshal(readBuffer[offset+4 : offset+4+size]); err != nil {
					panic(fmt.Sprintf("Error unmarshaling message: %v", err))
				}
				offset += size + 4

				func(msgCopy raftpb.Message) {
					s.stepChannel <- func() {
						if msg.Type == raftpb.MsgHeartbeat {
							s.leader = uint32(msg.From)
						} else if msg.Type == raftpb.MsgHeartbeatResp {
							s.leader = uint32(s.config.ID)
						}
						//fmt.Printf("Stepping with message: %v\n", msgCopy.Type)

						if err := s.node.Step(context.TODO(), msgCopy); err != nil {
							log.Printf("Step error: %v", err)
						}
					}
				}(msg)
			}
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
