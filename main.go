package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
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

func main() {
	value := os.Args[1]
	n, err := strconv.Atoi(os.Args[2])
	if err != nil {
		panic(err)
	}
	clients, err := strconv.Atoi(os.Args[2])
	if err != nil {
		panic(err)
	}
	address := os.Args[3]
	clientCount := n / clients

	if value == "client" {
		bytes := make([]byte, 1)
		connections := make([]net.Conn, clients)
		for i := 0; i < clients; i++ {
			connection, err := net.Dial("tcp", address)
			if err != nil {
				panic(err)
			}
			connections[i] = connection
		}
		group := sync.WaitGroup{}
		group.Add(clients)
		start := time.Now().UnixMilli()
		for i := range clients {
			go func(i int, connection net.Conn) {
				c := 0
				for range clientCount {
					err := Write(connection, bytes)
					if err != nil {
						return
					}
					err = Read(connection, bytes)
					if err != nil {
						return
					}
					c++
					if c%10000 == 0 {
						fmt.Printf("[%d] %d\n", i, c)
					}
				}
				err := connection.Close()
				if err != nil {
					panic(err)
				}
				group.Done()
			}(i, connections[i])
		}
		group.Wait()
		end := time.Now().UnixMilli()
		fmt.Printf("%d round trips in %dms, %d/s", n, end-start, int(float32(n)/(float32(end-start)/1000.0)))
	} else {
		bytes := make([]byte, 1)
		server, err := net.Listen("tcp", address)
		if err != nil {
			panic(err)
		}
		connections := make([]net.Conn, clients)
		for i := 0; i < clients; i++ {
			connection, err := server.Accept()
			if err != nil {
				panic(err)
			}
			connections[i] = connection
		}
		println("Server here?")
		for i := range clients {
			go func(connection net.Conn) {
				for range clientCount {
					err := Read(connection, bytes)
					if err != nil {
						return
					}
					err = Write(connection, bytes)
					if err != nil {
						return
					}
				}
				err := connection.Close()
				if err != nil {
					panic(err)
				}
			}(connections[i])
		}
		select {}
	}
}
