gen:
	# generate golang bindings (dag)
	protoc \
		-I=. \
		-I=${GOPATH}/src \
		-I=${GOPATH}/src/github.com/gogo/protobuf/protobuf \
		s3.proto \
		--gogofaster_out=.
	sed -i '/_ \"google\/protobuf\"/d' s3.pb.go