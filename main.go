package main

import (
	"networking_benchmark/client"
	"networking_benchmark/server"
	"os"
)

func main() {
	if os.Args[1] == "client" {
		client.StartClient()
	} else {
		server.StartServer()
	}
}
