# Copyright 2017 The Kubernetes Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

VERSION=`git describe --tags`
BUILD=`date +%FT%T%z`

LDFLAGS=-ldflags "-w -s -X main.version=${VERSION} -X main.build=${BUILD}"
GOSRC = $(shell find . -type f -name '*.go')

REGISTRY_NAME = zdnscloud/lvmcsi
IMAGE_VERSION = v0.6.3
VERSION=${IMAGE_VERSION}

.PHONY: all lvm clean

all: lvm

lvm:
	if [ ! -d ./vendor ]; then dep ensure; fi
	CGO_ENABLED=0 GOOS=linux go build -a -ldflags '-extldflags "-static"' -o ./lvmcsi ./cmd/

lvm-container:
	docker build -t $(REGISTRY_NAME):$(VERSION) .

push-lvm-container: lvm-container
	docker push $(REGISTRY_NAME):$(VERSION)

clean:
	go clean -r -x
	rm -f lvmcsi
