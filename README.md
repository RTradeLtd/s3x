# s3x

`s3x` is an open-source fork of `github.com/minio/minio` modified to work with TemporalX and an S3 gateway. It allows using IPFS from any application that currently uses S3, or minio, without needing to redesign your application architecture. It represents an incredible leap forward in usability for IPFS, and up until now no implementation of IPFS allowed you to use it, without needing to build your application specifically for IPFS. Additionally this means your S3 applications can leverage the benefits of IPFS as if it were a native IPFS application. One might say its better than...

The long-term goal is to merge the TemporalX gateway upstream, but that will obviously depend on whether or not minio would accept it, although tif they were it is definitely something we will do.

# Development/Testing

The current setup is using the TemporalX development environment located at `xapi-dev.temporal.cloud:9090`, as a hard coded parameter in the codebase. If you have a valid copy, and license of TemporalX running you can spin that up, update the hard coded address, and you'll your own S3X running!

To develop or test S3X you'll need to have a valid go 1.13 installation, as well as git. To download, install, and run you can use the following commands:

```shell
$> git clone https://github.com/RTradeLtd/s3x.git
$> cd s3x
$> go mod download
$> make
$> export MINIO_ACCESS_KEY=minio
$> export MINIO_SECRET_KEY=miniostorage
$> ./minio gateway s3x
```

Now you literally have a minio gateway that's serving an IPFS API that can be consumed by S3 applications, and you've made any S3 application an IPFS application. Pretty cool eh?

By default we spin up a gRPC API and a grpc-gateway HTTP API allowing you to interact with our "info api". The info api is used to query the underlying ledger and get the IPFS CID for buckets, or objects.

# End-To-End Example

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

# Supported Feature Set

Supported Bucket Calls:

| Name | Supported |
|------|-----------|
| MakeBucketWithLocation | Yes (fully) |
| GetBucketInfo | Yes (fully) |
| ListBuckets | Yes (fully) | 
| DeleteBucket | Yes (fully) |

Supported Object Calls:

| Name | Supported |
|------|-----------|
| ListObjects | Yes (partial) |
| ListObjectsV2 | Yes (partial) |
| GetObjectNInfo | Yes (partial) |
| GetObject | Yes (partial) |
| GetObjectInfo | Yes (fully) |
| PutObject | Yes (fully) |
| CopyObject | Yes (partial) |
| DeleteObject | Yes (fully) |
| DeleteObjects | Yes (fully) |

Supported Multipart Calls:

| Name | Supported |
|------|-----------|
| ListMultipartUploads | In-Development |
| NewMultipartUpload | In-Development |
| PutObjectPart | In-Development |
| CopyObjectPart | In-Development |
| ListObjectParts | In-Development |
| AbortMultipartUpload | In-Development |
| CompleteMultipartUpload | In-Development | 

Supported Policy Calls:

| Name | Supported |
|------|-----------|
| SetBucketPolicy | No |
| GetBucketPolicy | No |
| DeleteBucketPolicy | No |

# Design

## Overview

In order to enable the speed needed by S3 applications we used TemporalX to provide the interface for storing data on IPFS for the following reasons:

* gRPC is much faster than HTTP
* Compared to go-ipfs and existing IPFS implementations it is **[fast](https://medium.com/temporal-cloud/temporalx-vs-go-ipfs-official-node-benchmarks-8457037a77cf)**
* Built in data replication

## Storage

All data, that is all the objects, buckets, metadata, is stored on IPFS. Because IPFS uses hashes, and S3 uses arbitrary names for the buckets and objects, we perform bookkeeping with a "ledger". The actual "on-ipfs" structure of the objects, and buckets themselves is defined via protocol buffers, saved as an IPLD "raw" object type. They contain the data if there, as well as object and bucket metadata.

## Ledger

The "ledger" is an internal book keeper responsible for keeping track of the latest IPFS CID's that belong to each and every object, and bucket stored, and is currently implemented as a `dgraph-io/badger/v2` key-value datastore.

# Product Comparisons

## S3X vs Storj IPFS

S3X provides an S3 compatible API via a custom minio gateway that uses TemporalX to facilitate data storage operations. This means that by using TemporalX, all your data is being stored on IPFS, automatically inheriting the benefits that IPFS has into your application. Storj IPFS on the other hand uses the Storj network to provide an S3 datastore for an IPFS node. It is in essence an IPFS node using the `ipfs/go-ds-s3` datastore, connected to Storj instead of an S3 service like AWS. A major downside to Storj IPFS is that it still suffers from the issues `go-ipfs` has, and also inherits all the performance hits.

## S3X vs Storj

Both S3X and Storj provides an S3 compatible API. The main difference is that the Storj S3 API is backed by nodes on the Storj network, while S3X is backd by the IPFS network via TemporalX. That means any application you use S3X with automatically inherits the benefits of the IPFS network. Additionally for STorj to work requires that the Storj network remain online and functional. For S3X to work, all you need is a running version of TemporalX.

# License

All original works and copyrights are those of minio and the corresponding open-source contributors. The only unique code is present in `cmd/gateway/temx`.

While forking the repository, I may have accidentally changed some of the license details. I used an invocation of `sed` to replace `github.com/minio/minio` with `github.com/RTrade/s3x` so its possible there were some unintended changes. If you notice any please point them out and we will make the change.
