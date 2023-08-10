package surfstore

import (
	context "context"
	"fmt"
	"google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
	"sync"
	"time"
)

type RaftSurfstore struct {
	isLeader      bool
	isLeaderMutex *sync.RWMutex
	term          int64
	log           []*UpdateOperation

	metaStore *MetaStore

	// Added for discussion
	id             int64
	peers          []string
	pendingCommits []*chan bool
	commitIndex    int64
	lastApplied    int64
	matchedIndices []int64

	/*--------------- Chaos Monkey --------------*/
	isCrashed      bool
	isCrashedMutex *sync.RWMutex
	UnimplementedRaftSurfstoreServer
}

func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	s.isLeaderMutex.RLock()
	if !s.isLeader {
		s.isLeaderMutex.RUnlock()
		return nil, ERR_NOT_LEADER
	}
	s.isLeaderMutex.RUnlock()
	s.isCrashedMutex.Lock()
	if s.isCrashed {
		s.isCrashedMutex.Unlock()
		return nil, ERR_SERVER_CRASHED
	}
	s.isCrashedMutex.Unlock()
	for {
		s.isCrashedMutex.Lock()
		if s.isCrashed {
			s.isCrashedMutex.Unlock()
			return nil, ERR_SERVER_CRASHED
		}
		s.isCrashedMutex.Unlock()
		successCount := 0
		for idx, addr := range s.peers {
			s.isCrashedMutex.Lock()
			if s.isCrashed {
				s.isCrashedMutex.Unlock()
				return nil, ERR_SERVER_CRASHED
			}
			s.isCrashedMutex.Unlock()
			if int64(idx) == s.id {
				successCount++
				continue
			}
			conn, err := grpc.Dial(addr, grpc.WithInsecure())
			if err != nil {
				//fmt.Println("Dial err", err)
				continue
			}
			client := NewRaftSurfstoreClient(conn)
			//entries := s.log
			term := int64(-1)
			if s.matchedIndices[idx] != -1 {
				term = s.log[s.matchedIndices[idx]].Term
				//	entries = s.log[s.matchedIndices[idx]:]
			}
			var entries []*UpdateOperation
			if int(s.matchedIndices[idx]+1) < len(s.log) {
				entries = s.log[s.matchedIndices[idx]+1:]
			}
			dummyAppendEntriesInput := AppendEntryInput{
				Term:         s.term,
				PrevLogTerm:  term,
				PrevLogIndex: s.matchedIndices[idx],
				Entries:      entries,
				LeaderCommit: s.commitIndex,
			}
			ctxNew, cancel := context.WithTimeout(context.Background(), time.Second)
			response, err := client.AppendEntries(ctxNew, &dummyAppendEntriesInput)
			cancel()
			if err != nil {
				//fmt.Println("AppendEntries err", err)
				continue
			}
			if response.Success {
				successCount++
			}
		}
		//fmt.Println("successCount", successCount, len(s.peers), len(s.peers)/2)
		if successCount > len(s.peers)/2 {
			break
		}
	}
	fileInfoMap := new(FileInfoMap)
	fileInfoMap.FileInfoMap = s.metaStore.FileMetaMap
	return fileInfoMap, nil
}

func (s *RaftSurfstore) GetBlockStoreMap(ctx context.Context, hashes *BlockHashes) (*BlockStoreMap, error) {
	if !s.isLeader {
		return nil, ERR_NOT_LEADER
	}
	for {
		s.isCrashedMutex.Lock()
		if s.isCrashed {
			s.isCrashedMutex.Unlock()
			return nil, ERR_SERVER_CRASHED
		}
		s.isCrashedMutex.Unlock()
		successCount := 0
		for idx, addr := range s.peers {
			s.isCrashedMutex.Lock()
			if s.isCrashed {
				s.isCrashedMutex.Unlock()
				return nil, ERR_SERVER_CRASHED
			}
			s.isCrashedMutex.Unlock()
			if int64(idx) == s.id {
				successCount++
				continue
			}
			conn, err := grpc.Dial(addr, grpc.WithInsecure())
			if err != nil {
				continue
			}
			client := NewRaftSurfstoreClient(conn)
			term := int64((-1))
			//entries := s.log
			if s.matchedIndices[idx] != -1 {
				term = s.log[s.matchedIndices[idx]].Term
				//	entries = s.log[s.matchedIndices[idx]:]
			}
			var entries []*UpdateOperation
			if int(s.matchedIndices[idx]+1) < len(s.log) {
				entries = s.log[s.matchedIndices[idx]+1:]
			}
			dummyAppendEntriesInput := AppendEntryInput{
				Term:         s.term,
				PrevLogTerm:  term,
				PrevLogIndex: s.matchedIndices[idx],
				Entries:      entries,
				LeaderCommit: s.commitIndex,
			}
			ctxNew, cancel := context.WithTimeout(context.Background(), time.Second)
			response, err := client.AppendEntries(ctxNew, &dummyAppendEntriesInput)
			cancel()
			if err != nil {
				continue
			}
			if response.Success {
				successCount++
			}
		}
		if successCount > len(s.peers)/2 {
			break
		}
	}
	hashMap := make(map[string]*BlockHashes)
	for _, hash := range hashes.Hashes {
		server := s.metaStore.ConsistentHashRing.GetResponsibleServer(hash)
		if val, ok := hashMap[server]; ok {
			hashVals := val.Hashes
			hashVals = append(hashVals, hash)
			hashMap[server] = &BlockHashes{Hashes: hashVals}
		} else {
			hashVals := []string{hash}
			hashMap[server] = &BlockHashes{Hashes: hashVals}
		}
	}
	return &BlockStoreMap{BlockStoreMap: hashMap}, nil
}

func (s *RaftSurfstore) GetBlockStoreAddrs(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddrs, error) {
	if !s.isLeader {
		return nil, ERR_NOT_LEADER
	}
	for {
		s.isCrashedMutex.Lock()
		if s.isCrashed {
			s.isCrashedMutex.Unlock()
			return nil, ERR_SERVER_CRASHED
		}
		s.isCrashedMutex.Unlock()
		successCount := 0
		for idx, addr := range s.peers {
			s.isCrashedMutex.Lock()
			if s.isCrashed {
				s.isCrashedMutex.Unlock()
				return nil, ERR_SERVER_CRASHED
			}
			s.isCrashedMutex.Unlock()
			if int64(idx) == s.id {
				successCount++
				continue
			}
			conn, err := grpc.Dial(addr, grpc.WithInsecure())
			if err != nil {
				continue
			}
			client := NewRaftSurfstoreClient(conn)
			term := int64(-1)
			//entries := s.log
			if s.matchedIndices[idx] != -1 {
				term = s.log[s.matchedIndices[idx]].Term
				//	entries = s.log[s.matchedIndices[idx]:]
			}
			var entries []*UpdateOperation
			if int(s.matchedIndices[idx]+1) < len(s.log) {
				entries = s.log[s.matchedIndices[idx]+1:]
			}
			dummyAppendEntriesInput := AppendEntryInput{
				Term:         s.term,
				PrevLogTerm:  term,
				PrevLogIndex: s.matchedIndices[idx],
				Entries:      entries,
				LeaderCommit: s.commitIndex,
			}
			ctxNew, cancel := context.WithTimeout(context.Background(), time.Second)
			response, err := client.AppendEntries(ctxNew, &dummyAppendEntriesInput)
			cancel()
			if err != nil {
				continue
			}
			if response.Success {
				successCount++
			}
		}
		if successCount > len(s.peers)/2 {
			break
		}
	}
	return &BlockStoreAddrs{BlockStoreAddrs: s.metaStore.BlockStoreAddrs}, nil
}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {

	s.isLeaderMutex.RLock()
	if !s.isLeader {
		s.isLeaderMutex.RUnlock()
		return nil, ERR_NOT_LEADER
	}
	s.isLeaderMutex.RUnlock()
	s.isCrashedMutex.Lock()
	if s.isCrashed {
		s.isCrashedMutex.Unlock()
		return nil, ERR_SERVER_CRASHED
	}
	s.isCrashedMutex.Unlock()

	update := new(UpdateOperation)
	var version Version
	_, pres := s.metaStore.FileMetaMap[filemeta.Filename]
	if pres {
		if filemeta.Version == s.metaStore.FileMetaMap[filemeta.Filename].Version+1 {
			update.Term = s.term
			update.FileMetaData = filemeta
			version = Version{Version: filemeta.Version}
		} else {
			version = Version{Version: -1}
			return &version, nil
		}
	} else {
		update.Term = s.term
		update.FileMetaData = filemeta
		version = Version{Version: filemeta.Version}
	}
	s.log = append(s.log, update)
	canUpdate := false
	for {
		s.isCrashedMutex.Lock()
		if s.isCrashed {
			s.isCrashedMutex.Unlock()
			return nil, ERR_SERVER_CRASHED
		}
		s.isCrashedMutex.Unlock()
		successCount := 0
		for idx, addr := range s.peers {
			s.isCrashedMutex.Lock()
			if s.isCrashed {
				s.isCrashedMutex.Unlock()
				return nil, ERR_SERVER_CRASHED
			}
			s.isCrashedMutex.Unlock()
			if int64(idx) == s.id {
				successCount++
				continue
			}
			conn, err := grpc.Dial(addr, grpc.WithInsecure())
			if err != nil {
				continue
			}
			client := NewRaftSurfstoreClient(conn)
			term := int64(-1)
			if s.matchedIndices[idx] != -1 {
				term = s.log[s.matchedIndices[idx]].Term
			}
			var entries []*UpdateOperation
			if int(s.matchedIndices[idx]+1) < len(s.log) {
				entries = s.log[s.matchedIndices[idx]+1:]
			}
			ctxNew, cancel := context.WithTimeout(context.Background(), time.Second)
			response, err := client.AppendEntries(ctxNew, &AppendEntryInput{
				Term:         s.term,
				PrevLogTerm:  term,
				PrevLogIndex: s.matchedIndices[idx],
				Entries:      entries,
				LeaderCommit: s.commitIndex,
			})
			cancel()
			if err != nil {
				continue
			}
			if response.Success {
				successCount++
				s.matchedIndices[idx] = response.MatchedIndex
			} else {
				if s.matchedIndices[idx] >= 0 {
					s.matchedIndices[idx] -= 1
				}
			}
		}
		if successCount > len(s.peers)/2 {
			canUpdate = true
			break
		}
	}

	if canUpdate {
		index := int(s.commitIndex + 1)
		for {
			if index >= len(s.log) {
				break
			}
			s.metaStore.FileMetaMap[s.log[index].FileMetaData.Filename] = s.log[index].FileMetaData
			index++
		}

		s.commitIndex = int64(len(s.log) - 1)
	}
	return &version, nil
}

func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {
	s.isCrashedMutex.RLock()

	if s.isCrashed {
		s.isCrashedMutex.RUnlock()
		return &AppendEntryOutput{Success: false}, ERR_SERVER_CRASHED
	}
	s.isCrashedMutex.RUnlock()

	if input.Term < s.term {
		return &AppendEntryOutput{Success: false}, nil
	}

	if input.PrevLogIndex != -1 {
		if int64(len(s.log))-1 < input.PrevLogIndex || s.log[input.PrevLogIndex].Term != input.PrevLogTerm {
			return &AppendEntryOutput{Success: false}, nil
		}
	}

	s.log = s.log[:input.PrevLogIndex+1]
	s.log = append(s.log, input.Entries...)

	if input.LeaderCommit > s.commitIndex {
		if int64(len(s.log)-1) < input.LeaderCommit {
			s.commitIndex = int64(len(s.log) - 1)
		} else {
			s.commitIndex = input.LeaderCommit
		}
	}

	if input.Term > s.term {
		s.isLeaderMutex.Lock()
		s.isLeader = false
		s.isLeaderMutex.Unlock()
		fmt.Println("Updating Term", s.id, s.term, input.Term)
		s.term = input.Term
	}

	for s.lastApplied < input.LeaderCommit {
		entry := s.log[s.lastApplied+1]
		_, err := s.metaStore.UpdateFile(ctx, entry.FileMetaData)
		if err != nil {
			return &AppendEntryOutput{Success: false}, err
		}
		s.lastApplied++
	}
	return &AppendEntryOutput{ServerId: s.id, Term: s.term, Success: true, MatchedIndex: int64(len(s.log) - 1)}, nil
}

func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.RLock()

	if s.isCrashed {
		s.isCrashedMutex.RUnlock()
		return &Success{Flag: false}, ERR_SERVER_CRASHED
	}
	s.isCrashedMutex.RUnlock()

	s.isLeaderMutex.Lock()
	s.isLeader = true
	s.isLeaderMutex.Unlock()
	s.term++
	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {

	s.isLeaderMutex.RLock()

	if !s.isLeader {
		// Do nothing if not leader
		s.isLeaderMutex.RUnlock()
		return &Success{Flag: false}, nil
	}
	s.isLeaderMutex.RUnlock()

	// contact all the follower, send some AppendEntries call
	for idx, addr := range s.peers {
		s.isCrashedMutex.Lock()
		if s.isCrashed {
			s.isCrashedMutex.Unlock()
			return nil, ERR_SERVER_CRASHED
		}
		s.isCrashedMutex.Unlock()
		if int64(idx) == s.id {
			continue
		}

		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return &Success{Flag: false}, err
		}
		client := NewRaftSurfstoreClient(conn)

		logTerm := int64(-1)
		if s.matchedIndices[idx] != -1 {
			logTerm = s.log[s.matchedIndices[idx]].Term
		}

		dummyAppendEntriesInput := AppendEntryInput{
			Term:         s.term,
			PrevLogTerm:  logTerm,
			PrevLogIndex: s.matchedIndices[idx],
			Entries:      s.log[s.matchedIndices[idx]+1:],
			LeaderCommit: s.commitIndex,
		}

		ctxNew, cancel := context.WithTimeout(context.Background(), time.Second)
		_, err = client.AppendEntries(ctxNew, &dummyAppendEntriesInput)
		cancel()
		if err != nil {
			continue
			//return &Success{Flag: false}, err
		}

		_ = conn.Close()
	}
	return &Success{Flag: true}, nil
}

// ========== DO NOT MODIFY BELOW THIS LINE =====================================

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = true
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = false
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {
	fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, empty)
	s.isLeaderMutex.RLock()
	state := &RaftInternalState{
		IsLeader: s.isLeader,
		Term:     s.term,
		Log:      s.log,
		MetaMap:  fileInfoMap,
	}
	s.isLeaderMutex.RUnlock()

	return state, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)
