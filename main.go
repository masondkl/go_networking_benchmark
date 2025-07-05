package main

import (
	"log"
	"networking_benchmark/client"
	"networking_benchmark/server"
	"os"
)

func main() {
	if os.Args[1] == "client" {
		client.StartClient(os.Args[2:])
	} else if os.Args[1] == "server" {
		server.StartServer(os.Args[2:])
	} else {
		log.Fatalf("usage: %s {server|client} [options]", os.Args[0])
	}
}
