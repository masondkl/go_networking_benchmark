package shared

import (
	"net"
	"sync"
)

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
	Buffer []byte
}

type PeerConnection struct {
	Connection *net.Conn
	WriteLock  *sync.Mutex
}
