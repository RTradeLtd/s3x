/*
 * MinIO Cloud Storage, (C) 2017 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gateway

import (
	// Import all gateways.
	_ "github.com/RTradeLtd/s3x/cmd/gateway/azure"
	_ "github.com/RTradeLtd/s3x/cmd/gateway/gcs"
	_ "github.com/RTradeLtd/s3x/cmd/gateway/hdfs"
	_ "github.com/RTradeLtd/s3x/cmd/gateway/nas"
	_ "github.com/RTradeLtd/s3x/cmd/gateway/oss"
	_ "github.com/RTradeLtd/s3x/cmd/gateway/s3"
	_ "github.com/RTradeLtd/s3x/cmd/gateway/temx"
	// B2 is specifically kept here to avoid re-ordering by goimports,
	// please ask on github.com/RTradeLtd/s3x/issues before changing this.
	_ "github.com/RTradeLtd/s3x/cmd/gateway/b2"
	// Add your gateway here.
)
