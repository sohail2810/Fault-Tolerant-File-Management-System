package surfstore

import (
	"bufio"
	"encoding/json"
	"google.golang.org/grpc"
	"io"
	"log"
	"net"
	"os"
	"sync"
)

type RaftConfig struct {
	RaftAddrs  []string
	BlockAddrs []string
}

func LoadRaftConfigFile(filename string) (cfg RaftConfig) {
	configFD, e := os.Open(filename)
	if e != nil {
		log.Fatal("Error Open config file:", e)
	}
	defer configFD.Close()

	configReader := bufio.NewReader(configFD)
	decoder := json.NewDecoder(configReader)

	if err := decoder.Decode(&cfg); err == io.EOF {
		return
	} else if err != nil {
		log.Fatal(err)
	}
	return
}

func NewRaftServer(id int64, config RaftConfig) (*RaftSurfstore, error) {

	isLeaderMutex := sync.RWMutex{}
	isCrashedMutex := sync.RWMutex{}
	matchedIndicesArr := make([]int64, len(config.RaftAddrs))
	for i := range matchedIndicesArr {
		matchedIndicesArr[i] = -1
	}
	server := RaftSurfstore{
		isLeader:       false,
		isLeaderMutex:  &isLeaderMutex,
		term:           0,
		metaStore:      NewMetaStore(config.BlockAddrs),
		log:            make([]*UpdateOperation, 0),
		isCrashed:      false,
		isCrashedMutex: &isCrashedMutex,
		// Added for discussion
		id:             id,
		peers:          config.RaftAddrs,
		pendingCommits: make([]*chan bool, 0),
		commitIndex:    -1,
		lastApplied:    -1,
		matchedIndices: matchedIndicesArr,
	}

	return &server, nil
}

func ServeRaftServer(server *RaftSurfstore) error {
	grpcServer := grpc.NewServer()
	RegisterRaftSurfstoreServer(grpcServer, server)

	l, e := net.Listen("tcp", server.peers[server.id])
	if e != nil {
		return e
	}

	return grpcServer.Serve(l)
}
