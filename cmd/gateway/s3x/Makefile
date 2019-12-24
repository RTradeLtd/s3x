gen:
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