package server

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"go.etcd.io/bbolt"
	"networking_benchmark/shared"
	"os"
)

var (
	db            *bbolt.DB
	keyBucketName = []byte("key")
)

func SetupKeyBucket() {
	err := os.Remove("data.db")
	if err != nil && !os.IsNotExist(err) {
		panic(err)
	}

	db, err = bbolt.Open("data.db", 0600, nil)
	if err != nil {
		panic(err)
	}

	tx, err := db.Begin(true)
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
			messageIndex := binary.LittleEndian.Uint32(data[1:5])
			ownerIndex := binary.LittleEndian.Uint32(data[5:9])
			op := data[9]
			keySize := binary.LittleEndian.Uint32(data[10:14])
			key := data[14 : keySize+14]
			if op == shared.OP_WRITE_MEMORY {
				valueSize := binary.LittleEndian.Uint32(data[keySize+14:])
				value := data[keySize+18 : keySize+18+valueSize]
				memoryDb[string(key)] = value
			} else if op == shared.OP_WRITE {
				valueSize := binary.LittleEndian.Uint32(data[keySize+14:])
				value := data[keySize+18 : keySize+18+valueSize]
				err := Put(key, value)
				if err != nil {
					panic(err)
				}
			} else if ownerIndex == uint32(s.config.ID) {
				if op == shared.OP_READ_MEMORY {
					value := memoryDb[string(key)]
					if value == nil {
						fmt.Println("No key found")
					}
					go s.respondToClient(shared.OP_READ_MEMORY, messageIndex, value)
				} else if op == shared.OP_READ {
					value, err := Get(key)
					if err != nil {
						panic(err)
					}
					go s.respondToClient(shared.OP_READ, messageIndex, value)
				}
			}
		}
	}
}
