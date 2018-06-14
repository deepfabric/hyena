package storage

// WriteBatch batch operation
type WriteBatch interface {
	Delete(key []byte) error
	Set(key []byte, value []byte) error
}

// Storage is the storage
type Storage interface {
	// Set store the key,value
	Set(key []byte, value []byte, sync bool) error
	// Get returns the value
	Get(key []byte) ([]byte, error)
	// Delete deletes the key, if `sync` is true, will flush the disk immediate
	Delete(key []byte, sync bool) error
	// RangeDelete deletes the range [start,end) values
	RangeDelete(start, end []byte) error
	// Seek returns the first key >= given key, if no found, return None.
	Seek(key []byte) ([]byte, []byte, error)
	// Scan scans the range [start, end) and execute the handler func.
	// returns false means end the scan.
	Scan(start, end []byte, handler func(key, value []byte) (bool, error)) error
	// ScanWithPooledKey scans the range [start, end) and execute the handler func.
	// returns false means end the scan.
	ScanWithPooledKey(start, end []byte, handler func(key, value []byte) (bool, error), pooledKey bool) error
	// Free free the pooled bytes
	Free(pooled []byte)
	// NewWriteBatch returns a writebatch
	NewWriteBatch() WriteBatch
	// Write writes the writebatch
	Write(wb WriteBatch, sync bool) error
}

// NewStorage returns a storage using options
func NewStorage(opts ...Option) Storage {
	sopts := &options{}
	for _, opt := range opts {
		opt(sopts)
	}

	return newNemoStorage(sopts)
}
