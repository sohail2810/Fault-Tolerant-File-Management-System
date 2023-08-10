package surfstore

import (
	context "context"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap        map[string]*FileMetaData
	BlockStoreAddrs    []string
	ConsistentHashRing *ConsistentHashRing
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	file_map := &FileInfoMap{FileInfoMap: m.FileMetaMap}
	return file_map, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	_, pres := m.FileMetaMap[fileMetaData.Filename]
	if pres {
		if fileMetaData.Version == m.FileMetaMap[fileMetaData.Filename].Version+1 {
			m.FileMetaMap[fileMetaData.Filename] = fileMetaData
		} else {
			return &Version{Version: -1}, nil
		}
	} else {
		m.FileMetaMap[fileMetaData.Filename] = fileMetaData
	}
	return &Version{Version: fileMetaData.Version}, nil
}

func (m *MetaStore) GetBlockStoreMap(ctx context.Context, blockHashesIn *BlockHashes) (*BlockStoreMap, error) {
	// block_address := &BlockStoreAddr{Addr: m.BlockStoreAddr}
	// return block_address, nil
	hash_map := make(map[string]*BlockHashes)
	for _, hash := range blockHashesIn.Hashes {
		server := m.ConsistentHashRing.GetResponsibleServer(hash)
		if val, ok := hash_map[server]; ok {
			hash_vals := val.Hashes
			hash_vals = append(hash_vals, hash)
			hash_map[server] = &BlockHashes{Hashes: hash_vals}
		} else {
			hash_vals := []string{hash}
			hash_map[server] = &BlockHashes{Hashes: hash_vals}
		}
	}
	return &BlockStoreMap{BlockStoreMap: hash_map}, nil
}

func (m *MetaStore) GetBlockStoreAddrs(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddrs, error) {
	return &BlockStoreAddrs{BlockStoreAddrs: m.BlockStoreAddrs}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddrs []string) *MetaStore {
	return &MetaStore{
		FileMetaMap:        map[string]*FileMetaData{},
		BlockStoreAddrs:    blockStoreAddrs,
		ConsistentHashRing: NewConsistentHashRing(blockStoreAddrs),
	}
}
