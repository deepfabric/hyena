package raftstore

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// local is in (0x01, 0x02);
var (
	localPrefix byte = 0x01
	localMinKey      = []byte{localPrefix}
	localMaxKey      = []byte{localPrefix + 1}

	maxKey = []byte{}
	minKey = []byte{0xff}

	storeIdentKey = []byte{localPrefix, 0x01}

	dbStateSuffix        byte = 0x01
	raftLogSuffix        byte = 0x02
	raftLocalStateSuffix byte = 0x03
	raftApplyStateSuffix byte = 0x04
)

var (
	// We save two types data in DB, for raft and other meta data.
	// When the store starts, we should iterate all db meta data to
	// construct peer, no need to travel large raft data, so we separate them
	// with different prefixes.
	raftPrefix    byte = 0x02
	raftPrefixKey      = []byte{localPrefix, raftPrefix}

	// db meta data contains: DBLocalState, RaftLocalState, RaftApplyState
	// a db meta key format is: dbMetaPrefix(localPrefix bytes + 1) + db_id(8bytes) + suffix(1bytes)
	dbMetaPrefix    byte = 0x03
	dbMetaPrefixKey      = []byte{localPrefix, dbMetaPrefix}
	dbMetaMinKey         = []byte{localPrefix, dbMetaPrefix}
	dbMetaMaxKey         = []byte{localPrefix, dbMetaPrefix + 1}
)

func getDBStateKey(id uint64) []byte {
	return getPrefixIDKey(dbMetaPrefixKey, id, true, dbStateSuffix)
}

func getDBRaftPrefix(id uint64) []byte {
	return getPrefixIDKey(raftPrefixKey, id, false, 0)
}

func getDBMetaPrefix(id uint64) []byte {
	return getPrefixIDKey(dbMetaPrefixKey, id, false, 0)
}

func getPrefixIDKey(prefix []byte, id uint64, hasSuffix bool, suffix byte) []byte {
	var data []byte
	l := len(prefix)

	if hasSuffix {
		data = make([]byte, l+9)
	} else {
		data = make([]byte, l+8)
	}

	copy(data, prefix)
	binary.BigEndian.PutUint64(data[l:], id)

	if hasSuffix {
		data[l+8] = suffix
	}

	return data
}

func getRaftLocalStateKey(id uint64) []byte {
	return getDBRaftKey(id, raftLocalStateSuffix, 0, 0)
}

func getRaftApplyStateKey(id uint64) []byte {
	return getDBRaftKey(id, raftApplyStateSuffix, 0, 0)
}

func getRaftLogKey(id uint64, logIndex uint64) []byte {
	return getDBRaftKey(id, raftLogSuffix, 8, logIndex)
}

func getRaftLogIndex(key []byte) (uint64, error) {
	expectKeyLen := len(raftPrefixKey) + 8*2 + 1
	if len(key) != expectKeyLen {
		return 0, fmt.Errorf("key<%v> is not a valid raft log key", key)
	}

	return binary.BigEndian.Uint64(key[len(raftPrefixKey)+9:]), nil
}

func getDBRaftKey(id uint64, suffix byte, extraCap int, extra uint64) []byte {
	l := len(raftPrefixKey)
	data := make([]byte, l+9+extraCap)
	copy(data, raftPrefixKey)
	binary.BigEndian.PutUint64(data[l:], id)
	data[l+8] = suffix
	if extra > 0 {
		binary.BigEndian.PutUint64(data[l+9:], extra)
	}
	return data
}

func decodeMetaKey(key []byte) (uint64, byte, error) {
	prefixLen := len(dbMetaPrefixKey)
	keyLen := len(key)

	if prefixLen+9 != len(key) {
		return 0, 0, fmt.Errorf("invalid meta key length for key %v", key)
	}

	if !bytes.HasPrefix(key, dbMetaPrefixKey) {
		return 0, 0, fmt.Errorf("invalid meta prefix for key %v", key)
	}

	return binary.BigEndian.Uint64(key[prefixLen:keyLen]), key[keyLen-1], nil
}
