// +build windows

package vectordb

import (
	"fmt"
)

const (
	metricType     = 0
	indexKey       = "IVF4096,PQ32"
	queryParams    = "nprobe=256,ht=256"
	ageCacheWindow = 30 * 60 //cache age lookup result for 30 minutes
)

func newVectodb(path string, dim, flatThr int, distThr float32) (DB, error) {
	return nil, fmt.Errorf("windows not support")
}
