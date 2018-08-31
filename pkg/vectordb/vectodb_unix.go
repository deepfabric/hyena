// +build freebsd openbsd netbsd dragonfly linux

package vectordb

import (
	"os"
	"sync"

	"github.com/infinivision/hyena/pkg/util"
	"github.com/infinivision/vectodb"
)

const (
	metricType  = 0
	indexKey    = "IVF4096,PQ32"
	queryParams = "nprobe=256,ht=256"
)

func newVectodb(path string, dim, flatThr int, distThr float32) (DB, error) {
	vdb, err := vectodb.NewVectoDB(path, dim, metricType, indexKey, queryParams, distThr, flatThr)
	if err != nil {
		return nil, err
	}

	return &db{
		path:    path,
		dim:     dim,
		distThr: distThr,
		flatThr: flatThr,
		vdb:     vdb,
	}, nil
}

type db struct {
	sync.RWMutex

	path         string
	dim, flatThr int
	distThr      float32
	vdb          *vectodb.VectoDB
}

func (d *db) UpdateWithIds(extXb []float32, extXids []int64) error {
	d.Lock()
	err := d.vdb.UpdateWithIds(extXb, extXids)
	d.Unlock()
	return err
}

func (d *db) AddWithIds(newXb []float32, newXids []int64) error {
	d.Lock()
	err := d.vdb.AddWithIds(newXb, newXids)
	d.Unlock()
	return err
}

func (d *db) Search(xq, distances []float32, xids []int64) (int, error) {
	return d.vdb.Search(xq, distances, xids)
}

func (d *db) UpdateIndex() error {
	return d.vdb.UpdateIndex()
}

func (d *db) Clean() error {
	d.Lock()
	err := os.RemoveAll(d.path)
	d.Unlock()
	return err
}

func (d *db) Records() (uint64, error) {
	d.RLock()
	value, err := d.vdb.GetTotal()
	d.RUnlock()
	return uint64(value), err
}

func (d *db) CreateSnap(path string) error {
	d.Lock()
	err := util.GZIPTo(d.path, path)
	d.Unlock()
	return err
}

func (d *db) ApplySnap(path string) error {
	d.Lock()
	err := os.RemoveAll(d.path)
	if err != nil {
		d.Unlock()
		return err
	}
	err = os.Rename(path, d.path)
	if err != nil {
		return err
	}

	err = d.resetDB()
	d.Unlock()
	return err
}

func (d *db) resetDB() error {
	vdb, err := vectodb.NewVectoDB(d.path, d.dim, metricType, indexKey, queryParams, d.distThr, d.flatThr)
	if err != nil {
		return err
	}

	d.vdb = vdb
	return nil
}
