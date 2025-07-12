package server

import (
	"encoding/binary"
	"fmt"
	"github.com/google/uuid"
	"go.etcd.io/bbolt"
	"networking_benchmark/shared"
	"strconv"
	"syscall"
)

var (
	keyBucketName = []byte("key")
)

func Get(db *bbolt.DB, key []byte) ([]byte, error) {
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

func Put(db *bbolt.DB, key []byte, value []byte) error {
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

func (s *Server) DbHandler(channel chan []byte, dbIndex int) {
	memoryDb := make(map[int][]byte)
	bopts := &bbolt.Options{}

	bopts.NoSync = false
	bopts.NoGrowSync = false
	bopts.NoFreelistSync = true
	bopts.FreelistType = bbolt.FreelistMapType
	bopts.MmapFlags = syscall.MAP_POPULATE
	bopts.Mlock = false
	bopts.InitialMmapSize = 10737418240
	bopts.PageSize = 0

	fmt.Printf("Options: %v\n", bopts)

	boltDb, err := bbolt.Open(fmt.Sprintf("db.%d", dbIndex), 0600, bopts)

	//boltDb.M
	//Bolt options: {
	//	Timeout: 0s,
	//	NoGrowSync: false,
	//	NoFreelistSync: true,
	//	PreLoadFreelist: false,
	//	FreelistType: hashmap,
	//	ReadOnly: false,
	//	MmapFlags: 8000,
	//	InitialMmapSize: 10737418240,
	//	PageSize: 0,
	//	NoSync: false,
	//	OpenFile: 0x0,
	//	Mlock: false,
	//	Logger: 0xc0000d0c30
	//}
	if err != nil {
		panic(err)
	}
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

	for {
		select {
		case data := <-channel:
			//fmt.Printf("Handling data\n")
			//messageIndex := binary.LittleEndian.Uint32(data[1:5])
			messageId := uuid.UUID(data[1:17])
			//fmt.Printf("Processing messageid: %v\n", messageId)
			ownerIndex := binary.LittleEndian.Uint32(data[17:21])
			op := data[21]
			keySize := binary.LittleEndian.Uint32(data[22:26])
			key := data[26 : keySize+26]
			if op == shared.OP_WRITE_MEMORY {
				//fmt.Printf("Write memory\n")
				valueSize := binary.LittleEndian.Uint32(data[keySize+26:])
				value := data[keySize+30 : keySize+30+valueSize]
				index, err := strconv.Atoi(string(key))
				if err != nil {
					panic(err)
				}
				memoryDb[index] = value
				if !s.flags.FastPathWrites && ownerIndex == uint32(s.config.ID) {
					//fmt.Printf("We are the owner: %d\n")
					s.respondToClient(shared.OP_WRITE_MEMORY, messageId, nil)
				}
			} else if op == shared.OP_WRITE {
				valueSize := binary.LittleEndian.Uint32(data[keySize+26:])
				value := data[keySize+30 : keySize+30+valueSize]
				index, err := strconv.Atoi(string(key))
				if err != nil {
					panic(err)
				}
				memoryDb[index] = value
				err = Put(boltDb, key, value)
				if err != nil {
					panic(err)
				}
				if !s.flags.FastPathWrites && ownerIndex == uint32(s.config.ID) {
					s.respondToClient(shared.OP_WRITE, messageId, nil)
				}
			} else if ownerIndex == uint32(s.config.ID) {
				if op == shared.OP_READ_MEMORY {
					index, err := strconv.Atoi(string(key))
					if err != nil {
						panic(err)
					}
					value := memoryDb[index]
					if value == nil {
						fmt.Println("No key found")
					}
					s.respondToClient(shared.OP_READ_MEMORY, messageId, value)
				} else if op == shared.OP_READ {
					value, err := Get(boltDb, key)
					if err != nil {
						panic(err)
					}
					s.respondToClient(shared.OP_READ, messageId, value)
				} else {
					fmt.Printf("1 Unknown op %v\n", op)
				}
			}
		}
	}
}
