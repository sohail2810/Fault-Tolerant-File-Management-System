package surfstore

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"os"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"
)

/* Hash Related */
func GetBlockHashBytes(blockData []byte) []byte {
	h := sha256.New()
	h.Write(blockData)
	return h.Sum(nil)
}

func GetBlockHashString(blockData []byte) string {
	blockHash := GetBlockHashBytes(blockData)
	return hex.EncodeToString(blockHash)
}

/* File Path Related */
func ConcatPath(baseDir, fileDir string) string {
	return baseDir + "/" + fileDir
}

/*
	Writing Local Metadata File Related
*/

const createTable string = `create table if not exists indexes (
		fileName TEXT, 
		version INT,
		hashIndex INT,
		hashValue TEXT
	);`

const insertTuple string = `INSERT INTO indexes (fileName, version, hashIndex, hashValue) VALUES (?,?,?,?)`

// WriteMetaFile writes the file meta map back to local metadata file index.db
func WriteMetaFile(fileMetas map[string]*FileMetaData, baseDir string) error {
	// remove index.db file if it exists
	outputMetaPath := ConcatPath(baseDir, DEFAULT_META_FILENAME)
	if _, err := os.Stat(outputMetaPath); err == nil {
		e := os.Remove(outputMetaPath)
		if e != nil {
			// log.Fatal("Error During Meta Write Back")
			return e
		}
	}
	db, err := sql.Open("sqlite3", outputMetaPath)
	if err != nil {
		// log.Fatal("Error During Meta Write Back")
		return err
	}
	statement, err := db.Prepare(createTable)
	if err != nil {
		// log.Fatal("Error During Meta Write Back")
		return err
	}
	statement.Exec()
	for file_name, metadata := range fileMetas {
		to_insert, err := db.Prepare(insertTuple)
		if err != nil {
			return err
		}
		defer to_insert.Close()
		for hash, value := range metadata.BlockHashList {
			_, err = to_insert.Exec(file_name, metadata.Version, hash, value)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

/*
Reading Local Metadata File Related
*/
const getDistinctFileName string = `SELECT DISTINCT fileName FROM indexes`

const getTuplesByFileName string = `SELECT * FROM indexes where fileName = ?`

// LoadMetaFromMetaFile loads the local metadata file into a file meta map.
// The key is the file's name and the value is the file's metadata.
// You can use this function to load the index.db file in this project.
func LoadMetaFromMetaFile(baseDir string) (fileMetaMap map[string]*FileMetaData, e error) {
	metaFilePath, _ := filepath.Abs(ConcatPath(baseDir, DEFAULT_META_FILENAME))
	fileMetaMap = make(map[string]*FileMetaData)
	metaFileStats, e := os.Stat(metaFilePath)
	if e != nil || metaFileStats.IsDir() {
		return fileMetaMap, nil
	}
	db, err := sql.Open("sqlite3", metaFilePath)
	if err != nil {
		// log.Fatal("Error When Opening Meta")
		return
	}
	fileMetaMap = make(map[string]*FileMetaData)
	file_names, err := db.Query(getDistinctFileName)
	if err != nil {
		return
	}
	for file_names.Next() {
		file_name := ""
		err := file_names.Scan(&file_name)
		if err != nil {
			return
		}
		tuples, err := db.Prepare(getTuplesByFileName)
		if err != nil {
			return
		}
		defer tuples.Close()
		metadata, err := tuples.Query(file_name)
		if err != nil {
			return
		}
		hash_list := []string{}
		file_name_2 := ""
		version := int32(0)
		for metadata.Next() {
			hash := ""
			value := ""
			err := metadata.Scan(&file_name_2, &version, &hash, &value)
			if err != nil {
				return
			}
			hash_list = append(hash_list, value)
		}
		file_metadata := &FileMetaData{Filename: file_name_2, Version: int32(version), BlockHashList: hash_list}
		fileMetaMap[file_name] = file_metadata
	}
	return fileMetaMap, nil
}

/*
	Debugging Related
*/

// PrintMetaMap prints the contents of the metadata map.
// You might find this function useful for debugging.
func PrintMetaMap(metaMap map[string]*FileMetaData) {

	// fmt.Println("--------BEGIN PRINT MAP--------")

	// for _, filemeta := range metaMap {
	// 	// fmt.Println("\t", filemeta.Filename, filemeta.Version)
	// 	for _, blockHash := range filemeta.BlockHashList {
	// 		// fmt.Println("\t", blockHash)
	// 	}
	// }

	// fmt.Println("---------END PRINT MAP--------")

}
