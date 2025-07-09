package shared

import (
	"fmt"
	"net"
	"sync"
)

const OP_WRITE = 0
const OP_WRITE_MEMORY = 1
const OP_READ = 2
const OP_READ_MEMORY = 3
const OP_LEADER = 4

type PendingRead struct {
	Connection net.Conn
	WriteLock  *sync.Mutex
	Key        []byte
}

func GrowSlice(buffer []byte, required uint32) []byte {
	if cap(buffer) < int(required) {
		next := make([]byte, required)
		//copy(next, buffer)
		buffer = next
		fmt.Printf("We grew to: required=%d,len=%d,cap=\n", required)
	}
	if len(buffer) < int(required) {
		return buffer[:required]
	}
	return buffer
}

func Read(connection net.Conn, buffer []byte) error {
	for start := 0; start != len(buffer); {
		amount, reason := connection.Read(buffer[start:])
		if reason != nil {
			return reason
		}
		start += amount
	}
	return nil
}

func Write(connection net.Conn, buffer []byte) error {
	for start := 0; start != len(buffer); {
		amount, reason := connection.Write(buffer[start:])
		if reason != nil {
			return reason
		}
		start += amount
	}
	return nil
}

type ClientRequest struct {
	Connection net.Conn
	WriteLock  *sync.Mutex
	//writeChan chan WriteOp
	//Buffer []byte
}

type PeerConnection struct {
	Connection *net.Conn
	WriteLock  *sync.Mutex
}
