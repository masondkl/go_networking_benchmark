package server

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/google/uuid"
	"go.etcd.io/bbolt"
	"networking_benchmark/shared"
)

var (
	db            *bbolt.DB
	keyBucketName = []byte("key")
)

func SetupKeyBucket() {
	boltDb, err := bbolt.Open("data.db", 0600, nil)
	if err != nil {
		panic(err)
	}
	db = boltDb
	tx, err := boltDb.Begin(true)
	if err != nil {
		panic(err)
	}

	_, err = tx.CreateBucketIfNotExists(keyBucketName)
	if err != nil {
		panic(err)
	}

	if err := tx.Commit(); err != nil {
		panic(err)
	}
}

func Get(key []byte) ([]byte, error) {
	tx, err := db.Begin(false)
	if err != nil {
		return nil, err
	}

	value := tx.Bucket(keyBucketName).Get(key)

	if err := tx.Rollback(); err != nil {
		return value, err
	}

	return value, nil
}

func Put(key []byte, value []byte) error {
	tx, err := db.Begin(true)
	if err != nil {
		panic(err)
	}

	writeError := tx.Bucket(keyBucketName).Put(key, value)

	if err := tx.Commit(); err != nil {
		panic(err)
	}

	return writeError
}

func Range(start, end []byte) (map[string][]byte, error) {
	results := make(map[string][]byte)

	tx, err := db.Begin(false)
	if err != nil {
		return nil, err
	}

	c := tx.Bucket(keyBucketName).Cursor()
	for k, v := c.Seek(start); k != nil && bytes.Compare(k, end) < 0; k, v = c.Next() {
		keyCopy := append([]byte(nil), k...)
		valCopy := append([]byte(nil), v...)
		results[string(keyCopy)] = valCopy
	}

	if err := tx.Rollback(); err != nil {
		return nil, err
	}

	return results, nil
}

var memoryDb map[string][]byte

func (s *Server) DbHandler() {
	memoryDb = make(map[string][]byte)
	for {
		select {
		case data := <-s.dbChannel:
			//messageIndex := binary.LittleEndian.Uint32(data[1:5])
			messageId := uuid.UUID(data[:16])
			ownerIndex := binary.LittleEndian.Uint32(data[16:20])
			op := data[20]
			keySize := binary.LittleEndian.Uint32(data[21:25])
			key := data[25 : keySize+25]
			if op == shared.OP_WRITE_MEMORY {
				valueSize := binary.LittleEndian.Uint32(data[keySize+25:])
				value := data[keySize+29 : keySize+29+valueSize]
				memoryDb[string(key)] = value
				if !s.flags.FastPathWrites && ownerIndex == uint32(s.config.ID) {
					go s.respondToClient(shared.OP_WRITE_MEMORY, messageId, nil)
				}
			} else if op == shared.OP_WRITE {
				valueSize := binary.LittleEndian.Uint32(data[keySize+25:])
				value := data[keySize+29 : keySize+29+valueSize]
				memoryDb[string(key)] = value
				err := Put(key, value)
				if err != nil {
					panic(err)
				}
				if !s.flags.FastPathWrites && ownerIndex == uint32(s.config.ID) {
					go s.respondToClient(shared.OP_WRITE, messageId, nil)
				}
			} else if ownerIndex == uint32(s.config.ID) {
				if op == shared.OP_READ_MEMORY {
					value := memoryDb[string(key)]
					if value == nil {
						fmt.Println("No key found")
					}
					go s.respondToClient(shared.OP_READ_MEMORY, messageId, value)
				} else if op == shared.OP_READ {
					value, err := Get(key)
					if err != nil {
						panic(err)
					}
					go s.respondToClient(shared.OP_READ, messageId, value)
				}
			}
		}
	}
}
