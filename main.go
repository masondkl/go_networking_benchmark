package main

import (
	"net"
	"os"
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
	connection *net.Conn
	writeLock  *sync.Mutex
	//writeChan chan WriteOp
	buffer []byte
}

type PeerConnection struct {
	connection *net.Conn
	writeLock  *sync.Mutex
}

//type WriteOp struct {
//	buffer []byte
//	size   uint32
//}

func main() {
	if os.Args[1] == "client" {
		client()
	}
}
