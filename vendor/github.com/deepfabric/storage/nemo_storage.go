// +build freebsd openbsd netbsd dragonfly linux

package storage

import (
	gonemo "github.com/deepfabric/go-nemo"
)

type nemoWriteBatch struct {
	wb *gonemo.WriteBatch
}

func newNemoWriteBatch(wb *gonemo.WriteBatch) WriteBatch {
	return &nemoWriteBatch{
		wb: wb,
	}
}

func (n *nemoWriteBatch) Delete(key []byte) error {
	n.wb.WriteBatchDel(key)
	return nil
}

func (n *nemoWriteBatch) Set(key []byte, value []byte) error {
	n.wb.WriteBatchPut(key, value)
	return nil
}

type nemoStorage struct {
	opts   *options
	nemo   *gonemo.NEMO
	nemoDB *gonemo.DBNemo
}

func newNemoStorage(opts *options) Storage {
	var nemoOpts *gonemo.Options
	if opts.nemoOptions != "" {
		nemoOpts, _ = gonemo.NewOptions(opts.nemoOptions)
	} else {
		nemoOpts = gonemo.NewDefaultOptions()
	}

	nemo := gonemo.OpenNemo(nemoOpts, opts.nemoDataPath)
	nemoDB := nemo.GetMetaHandle()

	return &nemoStorage{
		nemo:   nemo,
		nemoDB: nemoDB,
	}
}

func (ns *nemoStorage) Set(key []byte, value []byte, sync bool) error {
	return ns.nemo.PutWithHandle(ns.nemoDB, key, value, sync)
}

func (ns *nemoStorage) Get(key []byte) ([]byte, error) {
	return ns.nemo.GetWithHandle(ns.nemoDB, key)
}

func (ns *nemoStorage) Delete(key []byte, sync bool) error {
	return ns.nemo.DeleteWithHandle(ns.nemoDB, key, sync)
}

func (ns *nemoStorage) RangeDelete(start, end []byte) error {
	return ns.nemo.RangeDelWithHandle(ns.nemoDB, start, end)
}

func (ns *nemoStorage) Seek(key []byte) ([]byte, []byte, error) {
	return ns.nemo.SeekWithHandle(ns.nemoDB, key)
}

func (ns *nemoStorage) Scan(start, end []byte, handler func(key, value []byte) (bool, error)) error {
	return ns.ScanWithPooledKey(start, end, handler, false)
}

func (ns *nemoStorage) ScanWithPooledKey(start, end []byte, handler func(key, value []byte) (bool, error), pooledKey bool) error {
	var key []byte
	var err error
	c := false

	it := ns.nemo.KScanWithHandle(ns.nemoDB, start, end, true)
	for ; it.Valid(); it.Next() {
		if pooledKey {
			key = it.PooledKey()
		} else {
			key = it.Key()
		}

		c, err = handler(key, it.Value())
		if err != nil || !c {
			break
		}
	}
	it.Free()

	return err
}

func (ns *nemoStorage) Free(pooled []byte) {
	gonemo.MemPool.Free(pooled)
}

func (ns *nemoStorage) NewWriteBatch() WriteBatch {
	wb := gonemo.NewWriteBatch()
	return newNemoWriteBatch(wb)
}

func (ns *nemoStorage) Write(wb WriteBatch, sync bool) error {
	nwb := wb.(*nemoWriteBatch)
	return ns.nemo.BatchWrite(ns.nemoDB, nwb.wb, sync)
}
