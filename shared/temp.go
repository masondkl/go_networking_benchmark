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
const OP_FORWARD = 5
const OP_MESSAGE = 6
const OP_READ_INDEX = 7
const OP_READ_INDEX_RESP = 8

type Pool[T any] struct {
	Head     uint32
	Size     uint32
	Elements []T
	Lock     sync.Mutex
	New      func() T
}

func (pool *Pool[T]) put(element T) {
	if pool.Head >= pool.Size {
		nextElements := make([]T, pool.Size*2)
		copy(nextElements[:pool.Size], pool.Elements)
		pool.Elements = nextElements
		pool.Size *= 2
	}
	pool.Elements[pool.Head] = element
	pool.Head++
}

func (pool *Pool[T]) Get() T {
	pool.Lock.Lock()
	if pool.Head == 0 {
		pool.put(pool.New())
	}
	pool.Head--
	result := pool.Elements[pool.Head]
	pool.Lock.Unlock()
	return result
}

func (pool *Pool[T]) Put(element T) {
	pool.Lock.Lock()
	pool.put(element)
	pool.Lock.Unlock()
}

func NewPool[T any](initialSize uint32, new func() T) *Pool[T] {
	pool := &Pool[T]{
		Head:     initialSize,
		Size:     initialSize,
		Elements: make([]T, initialSize),
		Lock:     sync.Mutex{},
		New:      new,
	}
	for i := uint32(0); i < initialSize; i++ {
		pool.Elements[i] = new()
	}
	return pool
}

type Client struct {
	Connection     net.Conn
	Channel        chan func()
	ProposeChannel chan func()
}

type PendingRead struct {
	Client Client
	Key    []byte
}

//func GrowSlice(buffer []byte, required uint32) []byte {
//	if cap(buffer) < int(required) {
//		next := make([]byte, required)
//		//copy(next, buffer)
//		buffer = next
//	}
//	if len(buffer) < int(required) {
//		return buffer[:required]
//	}
//	return buffer
//}

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
	Channel    chan func()
}
