# s3x

`s3x` is an open-source gateway for `github.com/minio/minio` to work with TemporalX as an S3 gateway. It allows using IPFS from any application that currently uses S3, or minio, without needing to redesign your application architecture. It represents an incredible leap forward in usability for IPFS, and up until now no implementation of IPFS allowed you to use it, without needing to build your application specifically for IPFS. Additionally this means your S3 applications can leverage the benefits of IPFS as if it were a native IPFS application. One might say its better than...

## Upstream Compatibility

s3x targets to be fully compatible with S3 and the latest minio release. We discussed with one of the MinIO lead devs, and they agreed that once we finish the iteration and have the first "v1 release" of s3x they will accept a PR from us to merge this functionality upstream.

After that we will continue to develop on this fork of the codebase, however anytime we release bug fixes, new features, etc... we will be porting them upstream.

If you observe any divergence from S3 or minio behaviour, please report them in the issues.

### Supported minio versions

Because of frequent changes in minio's interface types and project structure, only the version refenced in the go.mod file is supported for every release.

## Development/Testing

Unit testing uses the environment variable `TEST_XAPI` to set the TemporalX server address, if not set, `xapi-dev.temporal.cloud:9090` is used.

You can also start the gateway with the optional flag `temporalx.endpoint` to use your preferred instance of TemporalX. If you have a valid copy, and license of TemporalX running you can spin that up, set the flag, and you'll your own S3X running!

To develop or test S3X you'll need to have a valid go 1.13 installation, as well as git. To download, install, and run you can use the following commands:

```shell
$> git clone https://github.com/RTradeLtd/s3x.git
$> cd s3x
$> go mod download
$> make
$> export MINIO_ACCESS_KEY=minio
$> export MINIO_SECRET_KEY=miniostorage
$> ./minio-s3x gateway s3x
```

Now you literally have a minio gateway that's serving an IPFS API that can be consumed by S3 applications, and you've made any S3 application an IPFS application. Pretty cool eh?

By default we spin up a gRPC API and a grpc-gateway HTTP API allowing you to interact with our "info api". The info api is used to query the underlying ledger and get the IPFS CID for buckets, or objects.

## End-To-End Example

Note that this example assumes you have a local version of S3X running, for that see the previous section, and have the `mc` command available locally.

```shell
# add the host to our mc config
$> mc config host add s3x http://127.0.0.1:9000 minio miniostorage --api "s3v2"
# create a bucket called testbucket
$> mc mb s3x/testbucket
# copy a local file called file.txt inside of testbucket
$> mc cp file.txt s3x/testbucket
# copy file.txt out of testbucket to a local file called lol.txt
$> mc cp s3x/testbucket/file.txt lol.txt
# get the hash of testbucket as it exists on ipfs
$> curl http://localhost:8889/info?bucket=testbucket
# get the hash of the object in the bucket on ipfs
$> curl "http://localhost:8889/info?bucket=testbucket&object=file.txt"
# get the hash of the object data on ipfs, this will return only the data contained by the object
$> curl "http://localhost:8889/info?bucket=testbucket&object=file.txt&objectDataOnly=true"
```

## Supported Feature Set

Supported Bucket Calls:

| Name | Supported |
|------|-----------|
| MakeBucketWithLocation | Yes (fully) |
| GetBucketInfo | Yes (fully) |
| ListBuckets | Yes (fully) |
| DeleteBucket | Yes (partial) |

Supported Object Calls:

| Name | Supported |
|------|-----------|
| ListObjects | Yes (fully) |
| ListObjectsV2 | Yes (fully) |
| GetObjectNInfo | Yes (fully) |
| GetObject | Yes (fully) |
| GetObjectInfo | Yes (fully) |
| PutObject | Yes (fully) |
| CopyObject | Yes (fully) |
| DeleteObject | Yes (fully) |
| DeleteObjects | Yes (fully) |

Supported Multipart Calls:

| Name | Supported |
|------|-----------|
| ListMultipartUploads | Yes (fully)  |
| NewMultipartUpload | Yes (fully)  |
| PutObjectPart | Yes (fully)  |
| CopyObjectPart | Yes (fully)  |
| ListObjectParts | Yes (fully) |
| AbortMultipartUpload | Yes (fully)  |
| CompleteMultipartUpload | Yes (fully)  |

Supported Policy Calls:

| Name | Supported |
|------|-----------|
| SetBucketPolicy | No |
| GetBucketPolicy | No |
| DeleteBucketPolicy | No |

## Design

### Overview

In order to enable the speed needed by S3 applications we used TemporalX to provide the interface for storing data on IPFS for the following reasons:

* gRPC is much faster than HTTP
* Compared to go-ipfs and existing IPFS implementations it is **[fast](https://medium.com/temporal-cloud/temporalx-vs-go-ipfs-official-node-benchmarks-8457037a77cf)**
* Built in data replication

The default setting of s3x is to use a publicly available TemporalX server, however this is only sufficient for demo/trial purposes and in production you'll want to be running a TemporalX server yourself. To reduce network delays, and latency decreasing performance of s3x, it is recommended that you run TemporalX on the same box as you are running s3x, however this is only recommendation and it is totally fine to use a remote TemporalX node.

### Storage

All data, that is all the objects, buckets, metadata, is stored on IPFS. Because IPFS uses hashes, and S3 uses arbitrary names for the buckets and objects, we perform bookkeeping with a "ledger". The actual "on-ipfs" structure of the objects, and buckets themselves is defined via protocol buffers, saved as an IPLD "raw" object type. They contain the data if there, as well as object and bucket metadata.

### Ledger Store

The "ledger store" is an internal book keeper responsible for keeping a map of bucket and objects names, as well as the latest version of their IPFS CID. The ledger is what bridges TemporalX/IPFS to S3/MinIO, and is solely responsible for this `name->hash` map. All object/bucket data is stored on IPFS, with the only "off-IPFS" being this `name->hash` map, sometimes called "ledger store metadata".  The default implementation is done using a badger v2 datastore using `dgraph-io/badger` sufficient for running single s3x node installations.

There is a second available option for this ledger store that enables highly available s3x setups with multiple nodes. By using a CRDT datastore on top of a badger datastore, updates to the ledger state can be broadcasted between any s3x instance configured to listen to the same topic. One thing to note is that the performance of this will be slightly worse than the pure badger ledger store.

Please note that this doesn't give high availability for the actual bucket/object data. For this you would want to use TemporalX's built-in replication system.

## Kubernetes

Include in `kubernetes_local.yml` is a deployment that enables running all components of S3X in Kubernetes including the TemporalX node that is needed. This will require you to have the TemporalX docker image locally, which currently must be built locally. As such only those with access to the TemporalX repository can use this.

For those who don't have access to the repository, you can use the `kubernetes_remote.yml` file, which is configured to use the publicly accessible production TemporalX service.

### Docker

TODO: some docker files are copied and edited from minio, but not fully ready

## Product Comparisons

### S3X vs Storj IPFS

S3X provides an S3 compatible API via a custom minio gateway that uses TemporalX to facilitate data storage operations. This means that by using TemporalX, all your data is being stored on IPFS, automatically inheriting the benefits that IPFS has into your application. Storj IPFS on the other hand uses the Storj network to provide an S3 datastore for an IPFS node. It is in essence an IPFS node using the `ipfs/go-ds-s3` datastore, connected to Storj instead of an S3 service like AWS. A major downside to Storj IPFS is that it still suffers from the issues `go-ipfs` has, and also inherits all the performance hits.

### S3X vs Storj

Both S3X and Storj provides an S3 compatible API. The main difference is that the Storj S3 API is backed by nodes on the Storj network, while S3X is backed by the IPFS network via TemporalX. That means any application you use S3X with automatically inherits the benefits of the IPFS network. Additionally for STorj to work requires that the Storj network remain online and functional. For S3X to work, all you need is a running version of TemporalX.

## License

This code base depends greatly on minio and is targeted to be officially include in their gateway collection, as such, we follow the same rules for licenses and contributions.

If you notice any divergence, please point them out and we will make the change.
