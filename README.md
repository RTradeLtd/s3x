# s3x

`s3x` is an open-source fork of `github.com/minio/minio` modified to work with TemporalX and an S3 gateway. It allows using IPFS from any application that currently uses S3, or minio, without needing to redesign your application architecture. It represents an incredible leap forward in usability for IPFS, and up until now no implementation of IPFS allowed you to use it, without needing to build your application specifically for IPFS. Additionally this means your S3 applications can leverage the benefits of IPFS as if it were a native IPFS application. One might say its better than...

# Long Term Plan

The long-term goal is to merge the TemporalX gateway upstream, but that will obviously depend on whether or not minio would accept it, although tif they were it is definitely something we will do.

# Current Supported Features

The current supported feature is constantly evolving. In general all "basic"  operations like:

  * Creating buckets
  * Putting objects
  * Getting objects
  * Getting bucket/object info
  * Removing buckets
  * Removing objects


One thing to keep in mind however, is that we might not fully support these features. For exmaple the "list objects" functionality as of dec 22nd 2019 only supports returning all objects, and not fine-grained filtering. 

# Design

## Overview

In order to enable the speed needed by S3 applications we used TemporalX to provide the interface for storing data on IPFS for the following reasons:

* gRPC is much faster than HTTP
* Compared to go-ipfs and existing IPFS implementations it is **[fast](https://medium.com/temporal-cloud/temporalx-vs-go-ipfs-official-node-benchmarks-8457037a77cf)**
* Built in data replication

## Storage

All data, that is all the objects, buckets, metadata, is stored on IPFS. Because IPFS uses hashes, and S3 uses arbitrary names for the buckets and objects, we use a "Ledger" which currently takes the form of an on-disk key-value store using `ipfs/go-datastore`.

The actual "on-ipfs" structure of the objects, and buckets themselves is defined via protocol buffers, saved as an IPLD "raw" object type. They contain the data if there, as well as object and bucket metadata.

# License

All original works and copyrights are those of minio and the corresponding open-source contributors. The only unique code is present in `cmd/gateway/temx`.

While forking the repository, I may have accidentally changed some of the license details. I used an invocation of `sed` to replace `github.com/minio/minio` with `github.com/RTrade/s3x` so its possible there were some unintended changes. If you notice any please point them out and we will make the change.
