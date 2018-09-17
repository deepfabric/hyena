package vectordb

// DB vectordb
type DB interface {
	UpdateWithIds(extXb []float32, extXids []int64) error
	AddWithIds(newXb []float32, newXids []int64) error
	Search(xq, distances []float32, xids []int64) (int, error)
	UpdateIndex() error
	Destroy() error

	Clean() error
	Records() (uint64, error)
	CreateSnap(path string) error
	ApplySnap(path string) error
}

// NewDB create a db
func NewDB(path string, dim, flatThr int, distThr float32) (DB, error) {
	return newVectodb(path, dim, flatThr, distThr)
}
