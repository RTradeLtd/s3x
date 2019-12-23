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

Now you literally have a minio gateway that's serving an IPFS API that can be consumed by S3 applications, and you've made any S3 application an IPFS application. [Pretty cool eh?](https://gateway.temporal.cloud/ipfs/QmZ3MNegfWjDikun6BPRDeJe7NzNUqhEX2oLCf47Fu3Aua)

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
| ListObjectsV2 | No |
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
| ListMultipartUploads | No |
| NewMultipartUpload | No |
| PutObjectPart | No |
| CopyObjectPart | No |
| ListObjectParts | No |
| AbortMultipartUpload | No |
| CompleteMultipartUpload | No | 

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

# License

All original works and copyrights are those of minio and the corresponding open-source contributors. The only unique code is present in `cmd/gateway/temx`.

While forking the repository, I may have accidentally changed some of the license details. I used an invocation of `sed` to replace `github.com/minio/minio` with `github.com/RTrade/s3x` so its possible there were some unintended changes. If you notice any please point them out and we will make the change.
