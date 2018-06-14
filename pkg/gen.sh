#!/bin/bash
#
# Generate all elasticell protobuf bindings.
# Run from repository root.
#
set -e

PRJ="hyena"

# directories containing protos to be built
DIRS="./pb/rpc ./pb/meta ./pb/raft"

GOGOPROTO_ROOT="${GOPATH}/src/github.com/gogo/protobuf"
GOGOPROTO_PATH="${GOGOPROTO_ROOT}:${GOGOPROTO_ROOT}/protobuf"
HYENA_PB_PATH="${GOPATH}/src/github.com/infinivision/${PRJ}/pkg"

for dir in ${DIRS}; do
	pushd ${dir}
		protoc --gofast_out=plugins=grpc,import_prefix=github.com/infinivision/${PRJ}/pkg/:. -I=.:"${GOGOPROTO_PATH}":"${HYENA_PB_PATH}":"${GOPATH}/src" *.proto
		sed -i.bak -E 's/github\.com\/infinivision\/'"${PRJ}"'\/pkg\/(gogoproto|github\.com|golang\.org|google\.golang\.org)/\1/g' *.pb.go
		sed -i.bak -E 's/github\.com\/infinivision\/'"${PRJ}"'\/pkg\/(errors|fmt|io)/\1/g' *.pb.go
		sed -i.bak -E 's/github\.com\/infinivision\/'"${PRJ}"'\/pkg\/(encoding\/binary)/\1/g' *.pb.go
		sed -i.bak -E 's/import _ \"gogoproto\"//g' *.pb.go
		sed -i.bak -E 's/import fmt \"fmt\"//g' *.pb.go
		sed -i.bak -E 's/import math \"github.com\/infinivision\/'"${PRJ}"'\/pkg\/math\"//g' *.pb.go
		rm -f *.bak
		goimports -w *.pb.go
	popd
done
