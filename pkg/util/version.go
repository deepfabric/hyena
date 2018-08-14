package util

import (
	"fmt"
)

// set on build time
var (
	GitCommit = ""
	BuildTime = ""
	GoVersion = ""
)

// PrintVersion prints the current version
func PrintVersion() {
	fmt.Println("GitCommit: ", GitCommit)
	fmt.Println("BuildTime: ", BuildTime)
	fmt.Println("GoVersion: ", GoVersion)
}
