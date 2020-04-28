PWD := $(shell pwd)
GOPATH := $(shell go env GOPATH)
LDFLAGS := $(shell go run buildscripts/gen-ldflags.go)

GOARCH := $(shell go env GOARCH)
GOOS := $(shell go env GOOS)

VERSION ?= $(shell git describe --tags)
TAG ?= "rtradetech/s3x:$(VERSION)"

BUILD_LDFLAGS := '$(LDFLAGS)'

all: build

checks:
	@echo "Checking dependencies"
	@(env bash $(PWD)/buildscripts/checkdeps.sh)

getdeps:
	@mkdir -p ${GOPATH}/bin
	@which golint 1>/dev/null || (echo "Installing golint" && GO111MODULE=off go get -u golang.org/x/lint/golint)
ifeq ($(GOARCH),s390x)
	@which staticcheck 1>/dev/null || (echo "Installing staticcheck" && GO111MODULE=off go get honnef.co/go/tools/cmd/staticcheck)
else
	@which staticcheck 1>/dev/null || (echo "Installing staticcheck" && wget --quiet https://github.com/dominikh/go-tools/releases/download/2020.1.3/staticcheck_${GOOS}_${GOARCH}.tar.gz && tar xf staticcheck_${GOOS}_${GOARCH}.tar.gz && mv staticcheck/staticcheck ${GOPATH}/bin/staticcheck && chmod +x ${GOPATH}/bin/staticcheck && rm -f staticcheck_${GOOS}_${GOARCH}.tar.gz && rm -rf staticcheck)
endif
	@which misspell 1>/dev/null || (echo "Installing misspell" && GO111MODULE=off go get -u github.com/client9/misspell/cmd/misspell)

crosscompile:
	@(env bash $(PWD)/buildscripts/cross-compile.sh)

verifiers: getdeps vet fmt lint staticcheck spelling

vet:
	@echo "Running $@ check"
	@GO111MODULE=on go vet github.com/RTradeLtd/s3x/...

fmt:
	@echo "Running $@ check"
	@GO111MODULE=on gofmt -d cmd/
	@GO111MODULE=on gofmt -d pkg/

lint:
	@echo "Running $@ check"
	@GO111MODULE=on ${GOPATH}/bin/golint -set_exit_status github.com/RTradeLtd/s3x/...

staticcheck:
	@echo "Running $@ check"
	@GO111MODULE=on ${GOPATH}/bin/staticcheck github.com/RTradeLtd/s3x/...

spelling:
	@echo "Running $@ check"
	@GO111MODULE=on ${GOPATH}/bin/misspell -locale US -error `find gateway/`
	@GO111MODULE=on ${GOPATH}/bin/misspell -locale US -error `find buildscripts/`
	@GO111MODULE=on ${GOPATH}/bin/misspell -locale US -error `find dockerscripts/`

# Builds s3x, runs the verifiers then runs the tests.
check: test
test: verifiers build
	@echo "Running unit tests"
	@GO111MODULE=on CGO_ENABLED=0 go test -tags kqueue ./... 1>/dev/null

test-race: verifiers build
	@echo "Running unit tests under -race"
	@(env bash $(PWD)/buildscripts/race.sh)

# Verify s3x binary
verify:
	@echo "Verifying build with race"
	@GO111MODULE=on CGO_ENABLED=1 go build -race -tags kqueue -trimpath --ldflags $(BUILD_LDFLAGS) -o $(PWD)/minio-s3x 1>/dev/null
	@(env bash $(PWD)/buildscripts/verify-build.sh)

# Verify healing of disks with s3x binary
verify-healing:
	@echo "Verify healing build with race"
	@GO111MODULE=on CGO_ENABLED=1 go build -race -tags kqueue -trimpath --ldflags $(BUILD_LDFLAGS) -o $(PWD)/minio-s3x 1>/dev/null
	@(env bash $(PWD)/buildscripts/verify-healing.sh)

# Builds s3x locally.
build: checks
	@echo "Building s3x binary to './minio-s3x'"
	@GO111MODULE=on CGO_ENABLED=0 go build -tags kqueue -trimpath  --ldflags $(BUILD_LDFLAGS) -o $(PWD)/minio-s3x 1>/dev/null

docker: build
	@docker build -t $(TAG) . -f Dockerfile.dev

# Builds s3x and installs it to $GOPATH/bin.
install: build
	@echo "Installing s3x binary to '$(GOPATH)/bin/minio-s3x'"
	@mkdir -p $(GOPATH)/bin && cp -f $(PWD)/s3x $(GOPATH)/bin/minio-s3x
	@echo "Installation successful. To learn more, try \"minio-s3x --help\"."

clean:
	@echo "Cleaning up all the generated files"
	@find . -name '*.test' | xargs rm -fv
	@find . -name '*~' | xargs rm -fv
	@rm -rvf minio-s3x
	@rm -rvf build
	@rm -rvf release
	@rm -rvf .verify*

# run standard go tooling for better readability
.PHONY: tidy
tidy: 
	find . -type f -name '*.go' -exec goimports -w {} \;
	find . -type f -name '*.go' -exec gofmt -s -w {} \;