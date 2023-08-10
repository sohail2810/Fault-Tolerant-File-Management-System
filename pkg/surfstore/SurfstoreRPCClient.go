package surfstore

import (
	context "context"
	"os"
	"time"

	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type RPCClient struct {
	MetaStoreAddrs []string
	BaseDir        string
	BlockSize      int
}

func (surfClient *RPCClient) GetBlock(blockHash string, blockStoreAddr string, block *Block) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := c.GetBlock(ctx, &BlockHash{Hash: blockHash})
	if err != nil {
		err := conn.Close()
		if err != nil {
			return err
		}
		return err
	}
	block.BlockData = b.BlockData
	block.BlockSize = b.BlockSize

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) PutBlock(block *Block, blockStoreAddr string, succ *bool) error {
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	s, err := c.PutBlock(ctx, block)
	if err != nil {
		conn.Close()
		return err
	}
	*succ = s.Flag
	return conn.Close()
}

func (surfClient *RPCClient) HasBlocks(blockHashesIn []string, blockStoreAddr string, blockHashesOut *[]string) error {
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	in_blocks := &BlockHashes{Hashes: blockHashesIn}
	b, err := c.HasBlocks(ctx, in_blocks)
	if err != nil {
		conn.Close()
		return err
	}
	*blockHashesOut = b.Hashes
	return conn.Close()
}

func (surfClient *RPCClient) GetFileInfoMap(serverFileInfoMap *map[string]*FileMetaData) error {
	for _, addr := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			continue
		}
		c := NewRaftSurfstoreClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		f, err := c.GetFileInfoMap(ctx, &emptypb.Empty{})
		if err != nil {
			//fmt.Println("failing GetFileInfoMap for addr", addr, err)
			cancel()
			conn.Close()
			continue
		} else {
			*serverFileInfoMap = f.FileInfoMap
			cancel()
			return conn.Close()
		}
	}
	os.Exit(1)
	return nil
}

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int32) error {
	for _, addr := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			continue
		}
		c := NewRaftSurfstoreClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		v, err := c.UpdateFile(ctx, fileMetaData)
		if err != nil {
			//fmt.Println("failing UpdateFile for addr", addr)
			cancel()
			conn.Close()
			continue
		} else {
			*latestVersion = v.Version
			cancel()
			return conn.Close()
		}
	}
	os.Exit(1)
	return nil
}
func (surfClient *RPCClient) GetBlockHashes(blockStoreAddr string, blockHashes *[]string) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := c.GetBlockHashes(ctx, &emptypb.Empty{})
	if err != nil {
		err := conn.Close()
		if err != nil {
			return err
		}
		return err
	}
	*blockHashes = b.Hashes

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) GetBlockStoreMap(blockHashesIn []string, blockStoreMap *map[string][]string) error {
	for _, addr := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			continue
		}
		c := NewRaftSurfstoreClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		blockHashes := &BlockHashes{Hashes: blockHashesIn}
		bsm, err := c.GetBlockStoreMap(ctx, blockHashes)
		if err != nil {
			//fmt.Println("failing GetBlockStoreMap for addr", addr)
			cancel()
			conn.Close()
			continue
		} else {
			keys := make([]string, 0, len(bsm.BlockStoreMap))
			for k := range bsm.BlockStoreMap {
				keys = append(keys, k)
			}
			tempHashMap := make(map[string][]string)
			for _, key := range keys {
				hashMap := bsm.BlockStoreMap[key]
				tempHashMap[key] = hashMap.Hashes
			}
			*blockStoreMap = tempHashMap
			cancel()
			return conn.Close()
		}
	}
	os.Exit(1)
	return nil
}

func (surfClient *RPCClient) GetBlockStoreAddrs(blockStoreAddrs *[]string) error {
	for _, addr := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			continue
		}
		c := NewRaftSurfstoreClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		s, err := c.GetBlockStoreAddrs(ctx, &emptypb.Empty{})
		if err != nil {
			//fmt.Println("failing GetBlockStoreAddrs for addr", addr)
			cancel()
			conn.Close()
			continue
		} else {
			*blockStoreAddrs = s.BlockStoreAddrs
			cancel()
			return conn.Close()
		}
	}
	os.Exit(1)
	return nil
}

// This line guarantees all method for RPCClient are implemented
var _ ClientInterface = new(RPCClient)

// Create an Surfstore RPC client
func NewSurfstoreRPCClient(addrs []string, baseDir string, blockSize int) RPCClient {
	return RPCClient{
		MetaStoreAddrs: addrs,
		BaseDir:        baseDir,
		BlockSize:      blockSize,
	}
}
