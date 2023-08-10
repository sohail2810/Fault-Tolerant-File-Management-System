package surfstore

import (
	"bufio"
	"errors"
	"io/ioutil"
	"os"
)

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	file_meta_map, err := LoadMetaFromMetaFile(client.BaseDir)
	if err != nil {
		return
	}
	file_list, err := ioutil.ReadDir(client.BaseDir)
	if err != nil {
		return
	}
	// blockStoreAddr := ""
	// err = client.GetBlockStoreAddr(&blockStoreAddr)
	if err != nil {
		return
	}
	remote_meta_map := make(map[string]*FileMetaData)
	err = client.GetFileInfoMap(&remote_meta_map)
	if err != nil {
		return
	}
	db_file, _ := os.Create(client.BaseDir + "/index.db")
	db_file.Close()

	hash_map := make(map[string][]string)
	for _, file := range file_list {
		file_name := file.Name()
		if file_name != "index.db" {
			file_data, err := os.Open(client.BaseDir + "/" + file_name)
			if err != nil {
				return
			}

			for {
				bytesRead := make([]byte, client.BlockSize)
				len, err := file_data.Read(bytesRead)
				if len == 0 {
					break
				}
				if err != nil {
					return
				}
				bytesRead = bytesRead[:len]
				hash := GetBlockHashString(bytesRead)
				hash_map[file_name] = append(hash_map[file_name], hash)
			}

			file_metadata, pres := file_meta_map[file_name]
			if pres {
				if !check_equal(hash_map[file_name], file_metadata.BlockHashList) {
					file_meta_map[file_name].BlockHashList = hash_map[file_name]
					file_meta_map[file_name].Version += 1
				}
			} else {
				file_meta_map[file_name] = &FileMetaData{Filename: file_name, Version: 1, BlockHashList: hash_map[file_name]}
			}
		}
	}

	for fileName, metadata := range file_meta_map {
		_, pres := hash_map[fileName]
		if !pres {
			if len(metadata.BlockHashList) != 1 || metadata.BlockHashList[0] != "0" {
				metadata.Version += 1
				metadata.BlockHashList = []string{"0"}
			}
		}
	}

	for fileName, metadata := range file_meta_map {
		if remote_metadata, ok := remote_meta_map[fileName]; ok {
			if metadata.Version > remote_metadata.Version {
				upload(client, metadata)
			}
		} else {
			upload(client, metadata)
		}
	}

	for filename, remote_metadata := range remote_meta_map {
		metadata, pres := file_meta_map[filename]
		if pres {
			if metadata.Version < remote_metadata.Version {
				download(client, remote_metadata)
				file_meta_map[filename] = remote_metadata
			} else if metadata.Version == remote_metadata.Version && !check_equal(metadata.BlockHashList, remote_metadata.BlockHashList) {
				download(client, remote_metadata)
				file_meta_map[filename] = remote_metadata
			}
		} else {
			file_meta_map[filename] = &FileMetaData{}
			download(client, remote_metadata)
			file_meta_map[filename] = remote_metadata
		}
	}

	WriteMetaFile(file_meta_map, client.BaseDir)
}
func check_equal(arr1 []string, arr2 []string) (flag bool) {
	flag = true
	if len(arr1) != len(arr2) {
		flag = false
		return flag
	}
	for i := range arr1 {
		if arr1[i] != arr2[i] {
			flag = false
			break
		}
	}
	return flag
}

func upload(client RPCClient, meta_data *FileMetaData) error {
	latest_version := int32(0)
	_, err := os.Stat(client.BaseDir + "/" + meta_data.Filename)
	if errors.Is(err, os.ErrNotExist) {
		err = client.UpdateFile(meta_data, &latest_version)
		if err != nil {
			return err
		}
		meta_data.Version = latest_version
		return err
	}

	file_data := []Block{}
	file, err := os.Open(client.BaseDir + "/" + meta_data.Filename)
	if err != nil {
		return err
	}
	defer file.Close()
	reader := bufio.NewReader(file)
	for {
		buffer := make([]byte, client.BlockSize)
		bytesRead, err := reader.Read(buffer)
		if err != nil {
			break
		}
		file_data = append(file_data, Block{BlockData: buffer[:bytesRead], BlockSize: int32(bytesRead)})
	}
	hash_list := []string{}
	hash_block_map := make(map[string]Block)
	for _, block := range file_data {
		hash := GetBlockHashString(block.BlockData)
		hash_list = append(hash_list, hash)
		hash_block_map[hash] = block
	}
	blockStoreMap := make(map[string][]string)
	client.GetBlockStoreMap(hash_list, &blockStoreMap)

	for key := range blockStoreMap {
		for _, val := range blockStoreMap[key] {
			var succ bool
			block := hash_block_map[val]
			client.PutBlock(&block, key, &succ)
		}
	}

	err = client.UpdateFile(meta_data, &latest_version)
	if err != nil {
		meta_data.Version = -1
	}
	meta_data.Version = latest_version

	return nil
}

func download(client RPCClient, remote_metadata *FileMetaData) error {
	file, err := os.Create(client.BaseDir + "/" + remote_metadata.Filename)
	if err != nil {
		return err
	}
	defer file.Close()

	if remote_metadata.BlockHashList[0] == "0" {
		err := os.Remove(client.BaseDir + "/" + remote_metadata.Filename)
		if err != nil {
			return err
		}
		return nil
	}

	blockStoreMap := make(map[string][]string)
	client.GetBlockStoreMap(remote_metadata.BlockHashList, &blockStoreMap)
	data := ""
	for _, hash := range remote_metadata.BlockHashList {
		var block Block
		server := ""
		for key := range blockStoreMap {
			for _, element := range blockStoreMap[key] {
				if element == hash {
					server = key
					err := client.GetBlock(hash, server, &block)
					if err != nil {
						return err
					}
					data += string(block.BlockData)
				}
			}
		}

	}
	file.WriteString(data)

	return nil
}
