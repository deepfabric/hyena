ROOT_DIR 		= $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))/
VERSION_PATH	= $(shell echo $(ROOT_DIR) | sed -e "s;${GOPATH}/src/;;g")pkg/util
LD_GIT_COMMIT   = -X '$(VERSION_PATH).GitCommit=`git rev-parse --short HEAD`'
LD_BUILD_TIME   = -X '$(VERSION_PATH).BuildTime=`date +%FT%T%z`'
LD_GO_VERSION   = -X '$(VERSION_PATH).GoVersion=`go version`'
LD_FLAGS        = -ldflags "$(LD_GIT_COMMIT) $(LD_BUILD_TIME) $(LD_GO_VERSION) -w -s"

GOOS 		= linux
PLATFORM    = ""
CGO_ENABLED = 1
DIST_DIR 	= $(ROOT_DIR)dist/

.PHONY: release
release: dist_dir hyena;

.PHONY: hyena
hyena: ; $(info ======== compiled hyena:)
	env CGO_ENABLED=$(CGO_ENABLED) GOOS=$(GOOS) go build  -o $(DIST_DIR)hyena $(LD_FLAGS) $(ROOT_DIR)cmd/hyena/*.go

.PHONY: dist_dir
dist_dir: ; $(info ======== prepare distribute dir:)
	mkdir -p $(DIST_DIR)
	@rm -rf $(DIST_DIR)*

.PHONY: clean
clean: ; $(info ======== clean all:)
	rm -rf $(DIST_DIR)*
	rm -rf $(ROOT_DIR)Library

.PHONY: help
help:
	@echo "build release binary: \n\t\tmake release\n"
	@echo "clean all binary: \n\t\tmake clean\n"
