// +build go1.13

/*
 * Below main package has canonical imports for 'go get' and 'go build'
 * to work with all other clones of github.com/RTradeLtd/s3x repository. For
 * more information refer https://golang.org/doc/go1.4#canonicalimports
 */

package main // import "github.com/RTradeLtd/s3x"

import (
	"os"

	minio "github.com/RTradeLtd/s3x/cmd"

	// Import gateway
	_ "github.com/RTradeLtd/s3x/gateway"
)

func main() {
	minio.Main(os.Args)
}
