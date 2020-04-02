module github.com/RTradeLtd/s3x

go 1.13

require (
	cloud.google.com/go v0.41.0
	github.com/Azure/azure-pipeline-go v0.2.2
	github.com/Azure/azure-storage-blob-go v0.8.0
	github.com/RTradeLtd/TxPB/v3 v3.2.2
	github.com/RTradeLtd/go-ds-badger/v2 v2.1.0
	github.com/Shopify/sarama v1.24.1
	github.com/StackExchange/wmi v0.0.0-20190523213315-cbe66965904d // indirect
	github.com/alecthomas/participle v0.2.1
	github.com/aliyun/aliyun-oss-go-sdk v0.0.0-20190307165228-86c17b95fcd5
	github.com/aws/aws-sdk-go v1.29.14
	github.com/bcicen/jstream v0.0.0-20190220045926-16c1f8af81c2
	github.com/beevik/ntp v0.2.0
	github.com/cespare/xxhash/v2 v2.1.1
	github.com/cheggaaa/pb v1.0.28
	github.com/coredns/coredns v1.6.7
	github.com/coreos/bbolt v1.3.3 // indirect
	github.com/coreos/etcd v3.3.12+incompatible
	github.com/coreos/go-systemd v0.0.0-20190321100706-95778dfbb74e // indirect
	github.com/dgraph-io/badger/v2 v2.0.1-rc1.0.20200127094334-3e25d771067d // indirect
	github.com/dgrijalva/jwt-go v3.2.0+incompatible
	github.com/djherbis/atime v1.0.0
	github.com/dustin/go-humanize v1.0.0
	github.com/eapache/go-resiliency v1.2.0 // indirect
	github.com/eclipse/paho.mqtt.golang v1.2.0
	github.com/elazarl/go-bindata-assetfs v1.0.0
	github.com/fatih/color v1.9.0
	github.com/fatih/structs v1.1.0
	github.com/ghodss/yaml v1.0.1-0.20190212211648-25d852aebe32 // indirect
	github.com/go-ole/go-ole v1.2.4 // indirect
	github.com/go-sql-driver/mysql v1.5.0
	github.com/gogo/protobuf v1.3.1
	github.com/golang/groupcache v0.0.0-20190702054246-869f871628b6 // indirect
	github.com/golang/protobuf v1.3.3
	github.com/gomodule/redigo v2.0.0+incompatible
	github.com/gorilla/handlers v1.4.2
	github.com/gorilla/mux v1.7.3
	github.com/gorilla/rpc v1.2.0
	github.com/gorilla/websocket v1.4.1 // indirect
	github.com/grpc-ecosystem/grpc-gateway v1.13.0
	github.com/hashicorp/go-hclog v0.9.2 // indirect
	github.com/hashicorp/raft v1.1.1-0.20190703171940-f639636d18e0 // indirect
	github.com/hashicorp/vault/api v1.0.4
	github.com/inconshreveable/go-update v0.0.0-20160112193335-8152e7eb6ccf
	github.com/ipfs/go-cid v0.0.5
	github.com/ipfs/go-datastore v0.3.1
	github.com/ipfs/go-ipld-format v0.0.2
	github.com/ipfs/go-merkledag v0.3.1
	github.com/ipfs/go-unixfs v0.2.4
	github.com/json-iterator/go v1.1.9
	github.com/klauspost/compress v1.10.3
	github.com/klauspost/cpuid v1.2.2
	github.com/klauspost/pgzip v1.2.1
	github.com/klauspost/readahead v1.3.1
	github.com/klauspost/reedsolomon v1.9.3
	github.com/kurin/blazer v0.5.4-0.20190613185654-cf2f27cc0be3
	github.com/lib/pq v1.1.1
	github.com/mattn/go-colorable v0.1.4
	github.com/mattn/go-ieproxy v0.0.0-20190805055040-f9202b1cfdeb // indirect
	github.com/mattn/go-isatty v0.0.11
	github.com/miekg/dns v1.1.27
	github.com/minio/cli v1.22.0
	github.com/minio/gokrb5/v7 v7.2.5
	github.com/minio/hdfs/v3 v3.0.1
	github.com/minio/highwayhash v1.0.0
	github.com/minio/lsync v1.0.1
	github.com/minio/minio-go/v6 v6.0.50-0.20200306231101-b882ba63d570
	github.com/minio/parquet-go v0.0.0-20200125064549-a1e49702e174
	github.com/minio/sha256-simd v0.1.1
	github.com/minio/simdjson-go v0.1.5-0.20200303142138-b17fe061ea37
	github.com/minio/sio v0.2.0
	github.com/mitchellh/go-homedir v1.1.0
	github.com/mmcloughlin/avo v0.0.0-20200303042253-6df701fe672f // indirect
	github.com/nats-io/gnatsd v1.4.1 // indirect
	github.com/nats-io/go-nats v1.7.2 // indirect
	github.com/nats-io/go-nats-streaming v0.4.4 // indirect
	github.com/nats-io/nats-server v1.4.1 // indirect
	github.com/nats-io/nats-server/v2 v2.1.2
	github.com/nats-io/nats-streaming-server v0.14.2 // indirect
	github.com/nats-io/nats.go v1.9.1
	github.com/nats-io/stan.go v0.6.0
	github.com/ncw/directio v1.0.5
	github.com/nsqio/go-nsq v1.0.8
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.5.1
	github.com/rcrowley/go-metrics v0.0.0-20190704165056-9c2d0518ed81 // indirect
	github.com/rjeczalik/notify v0.9.2
	github.com/rs/cors v1.6.0
	github.com/secure-io/sio-go v0.3.0
	github.com/segmentio/ksuid v1.0.2
	github.com/shirou/gopsutil v2.20.2+incompatible
	github.com/sirupsen/logrus v1.4.2
	github.com/skyrings/skyring-common v0.0.0-20160929130248-d1c0bb1cbd5e
	github.com/streadway/amqp v0.0.0-20190404075320-75d898a42a94
	github.com/tinylib/msgp v1.1.1
	github.com/ugorji/go v1.1.5-pre // indirect
	github.com/valyala/tcplisten v0.0.0-20161114210144-ceec8f93295a
	go.uber.org/atomic v1.3.2
	golang.org/x/crypto v0.0.0-20191206172530-e9b2fee46413
	golang.org/x/sys v0.0.0-20191220142924-d4481acd189f
	golang.org/x/tools v0.0.0-20200216192241-b320d3a0f5a2 // indirect
	google.golang.org/api v0.20.0
	google.golang.org/genproto v0.0.0-20200212174721-66ed5ce911ce
	google.golang.org/grpc v1.27.0
	gopkg.in/cheggaaa/pb.v1 v1.0.28 // indirect
	gopkg.in/ini.v1 v1.48.0 // indirect
	gopkg.in/ldap.v3 v3.0.3
	gopkg.in/olivere/elastic.v5 v5.0.80
	gopkg.in/yaml.v2 v2.2.8
)
