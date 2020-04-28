// s3x/gateway imports s3x gateway only.
// To enable all minio gateways please also import "github.com/minio/minio/cmd/gateway".
package gateway

import (
	_ "github.com/RTradeLtd/s3x/gateway/s3x"
)
