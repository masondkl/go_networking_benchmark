package main

import (
	"log"
	"net"
	"net/http"
	"os"
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
	writeChan chan WriteOp
	buffer    []byte
}

type WriteOp struct {
	buffer []byte
	size   uint32
}

func main() {
	go func() {
		log.Println("Starting pprof server on :6060")
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	if os.Args[1] == "client" {
		client()
	} else {
		server()
	}
}
