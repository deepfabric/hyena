package util

import (
	"github.com/shirou/gopsutil/disk"
)

// DiskStats returns the disk usage stats
func DiskStats(path string) (*disk.UsageStat, error) {
	stats, err := disk.Usage(path)
	if err != nil {
		return nil, err
	}
	return stats, nil
}
