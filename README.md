# s3x

`s3x` is an open-source fork of `github.com/minio/minio` modified to work with TemporalX and an S3 gateway. It allows using IPFS from any application that currently uses S3, or minio, without needing to redesign your application architecture. It represents an incredibly leap forward in usability for IPFS, and up until now no implementation of IPFS allowed you to use it, without needing to build your application specifically for IPFS. Additionally this means your S3 applications can leverage the benefits of IPFS as if it were a native IPFS application. One might say its better than...

All original works and copyrights are those of minio and the corresponding open-source contributors. The only unique code is present in `cmd/gateway/temx`.
