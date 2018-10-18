// +build freebsd openbsd netbsd dragonfly linux

package vectordb

import (
	"fmt"
	"os"
	"sync"

	"github.com/infinivision/hyena/pkg/util"
	"github.com/infinivision/vectodb"
)

var (
	destoryErr = fmt.Errorf("db was already destoryed")
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
		destroy: false,
		hasData: true,
	}, nil
}

type db struct {
	sync.RWMutex

	path             string
	dim, flatThr     int
	distThr          float32
	vdb              *vectodb.VectoDB
	destroy, hasData bool
}

func (d *db) UpdateWithIds(extXb []float32, extXids []int64) error {
	d.RLock()
	if d.destroy {
		d.RUnlock()
		return destoryErr
	}
	err := d.vdb.UpdateWithIds(extXb, extXids)
	d.RUnlock()
	return err
}

func (d *db) AddWithIds(newXb []float32, newXids []int64) error {
	d.RLock()
	if d.destroy {
		d.RUnlock()
		return destoryErr
	}
	err := d.vdb.AddWithIds(newXb, newXids)
	d.RUnlock()
	return err
}

func (d *db) Search(xq, distances []float32, xids []int64) (int, error) {
	d.RLock()
	if d.destroy {
		d.RUnlock()
		return 0, destoryErr
	}
	value, err := d.vdb.Search(xq, distances, xids)
	d.RUnlock()
	return value, err
}

func (d *db) UpdateIndex() error {
	d.RLock()
	if d.destroy {
		d.RUnlock()
		return destoryErr
	}
	if !d.hasData {
		d.RUnlock()
		return nil
	}
	err := d.vdb.UpdateIndex()
	d.RUnlock()
	return err
}

func (d *db) Destroy() error {
	d.Lock()
	err := d.vdb.Destroy()
	d.destroy = true
	d.Unlock()
	return err
}

func (d *db) Clean() error {
	d.Lock()
	if d.destroy {
		d.Unlock()
		return destoryErr
	}
	d.hasData = false
	err := os.RemoveAll(d.path)
	d.Unlock()
	return err
}

func (d *db) Records() (uint64, error) {
	d.RLock()
	if d.destroy {
		d.RUnlock()
		return 0, destoryErr
	}
	value, err := d.vdb.GetTotal()
	d.RUnlock()
	return uint64(value), err
}

func (d *db) CreateSnap(path string) error {
	d.Lock()
	if d.destroy {
		d.Unlock()
		return destoryErr
	}
	err := util.GZIPTo(d.path, path)
	d.Unlock()
	return err
}

func (d *db) ApplySnap(path string) error {
	d.Lock()
	if d.destroy {
		d.Unlock()
		return destoryErr
	}
	err := os.RemoveAll(d.path)
	if err != nil {
		d.Unlock()
		return err
	}
	err = os.Rename(path, d.path)
	if err != nil {
		d.Unlock()
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
	d.destroy = false
	d.hasData = true
	return nil
}
