gen:
	# check out dependcies
	# git clone https://github.com/gogo/protobuf.git ${GOPATH}/src/github.com/gogo/protobuf/
	# git clone https://github.com/grpc-ecosystem/grpc-gateway.git ${GOPATH}/src/github.com/grpc-ecosystem/grpc-gateway/
	# go get -u github.com/gogo/protobuf/protoc-gen-gogofaster 
	# go get -u github.com/grpc-ecosystem/grpc-gateway/protoc-gen-grpc-gateway
	# go get -u github.com/grpc-ecosystem/grpc-gateway/protoc-gen-swagger
	# go get -u github.com/golang/protobuf/protoc-gen-go
	# generate golang bindings (dag)
	protoc \
		-I=. \
		-I=${GOPATH}/src \
		-I=${GOPATH}/src/github.com/gogo/protobuf/protobuf \
		-I=${GOPATH}/src/github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis \
		s3.proto \
		--gogofaster_out=plugins=grpc:. \
		--grpc-gateway_out=.
	sed -i '/_ \"google\/protobuf\"/d' s3.pb.go
	go vet ./...