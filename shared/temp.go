package shared

import (
	"net"
	"sync"
)

const OP_WRITE = 0
const OP_WRITE_MEMORY = 1
const OP_READ = 2
const OP_READ_MEMORY = 3
const OP_LEADER = 4

func GrowSlice(buffer []byte, requiredSize uint32) []byte {
	if cap(buffer) < int(requiredSize) {
		buffer = append(buffer, make([]byte, int(requiredSize)-len(buffer))...)
		buffer = buffer[:cap(buffer)]
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
