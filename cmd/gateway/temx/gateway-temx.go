package temx

import (
	"context"
	"crypto/tls"
	"net/http"
	"os"
	"sync"

	pb "github.com/RTradeLtd/TxPB/go"
	badger "github.com/RTradeLtd/go-ds-badger/v2"
	minio "github.com/RTradeLtd/s3x/cmd"
	"github.com/RTradeLtd/s3x/pkg/auth"
	"github.com/minio/cli"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

const (
	temxBackend = "temx"
)

func init() {
	// TODO(bonedaddy): add help command
	minio.RegisterGatewayCommand(cli.Command{
		Name:   temxBackend,
		Usage:  "TemporalX IPFS",
		Action: temxGatewayMain,
	})
}
func temxGatewayMain(ctx *cli.Context) {
	minio.StartGateway(ctx, &TEMX{})
}

// TEMX implements MinIO Gateway
type TEMX struct{}

// newLedgerStore returns an instance of ledgerStore that uses badgerv2
func (g *TEMX) newLedgerStore(dsPath string) (*LedgerStore, error) {
	opts := badger.DefaultOptions
	ds, err := badger.NewDatastore(dsPath, &opts)
	if err != nil {
		return nil, err
	}
	return newLedgerStore(ds), nil
}

func (g *TEMX) getDSPath() string {
	path := os.Getenv("S3X_DS_PATH")
	if path == "" {
		path = "s3xstore"
	}
	return path
}

// NewGatewayLayer creates a minio gateway layer powered y TemporalX
func (g *TEMX) NewGatewayLayer(creds auth.Credentials) (minio.ObjectLayer, error) {
	conn, err := grpc.Dial("xapi-dev.temporal.cloud:9090", grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
		InsecureSkipVerify: true,
	})))
	if err != nil {
		return nil, err
	}
	ledger, err := g.newLedgerStore(g.getDSPath())
	if err != nil {
		return nil, err
	}
	return &xObjects{
		creds:     creds,
		dagClient: pb.NewDagAPIClient(conn),
		httpClient: &http.Client{
			Transport: minio.NewCustomHTTPTransport(),
		},
		ctx:         context.Background(),
		ledgerStore: ledger,
	}, nil
}

// Name returns the name of the TemporalX gateway backend
func (g *TEMX) Name() string {
	return temxBackend
}

// Production indicates that this backend is suitable for production use
func (g *TEMX) Production() bool {
	return true
}

type xObjects struct {
	minio.GatewayUnsupported
	mu         sync.Mutex
	creds      auth.Credentials
	dagClient  pb.DagAPIClient
	httpClient *http.Client
	ctx        context.Context

	// ledgerStore is responsible for updating our internal ledger state
	ledgerStore *LedgerStore
}

func (x *xObjects) Shutdown(ctx context.Context) error {
	return nil
}

// StorageInfo is not relevant to TemporalX backend.
func (x *xObjects) StorageInfo(ctx context.Context) (si minio.StorageInfo) {
	si.Backend.Type = minio.BackendGateway
	//si.Backend.GatewayOnline = minio.IsBackendOnline(ctx, x.httpClient, "https://docsx.temporal.cloud")
	return si
}

// IsCompressionSupported returns whether compression is applicable for this layer.
func (x *xObjects) IsCompressionSupported() bool {
	return false
}

// IsEncryptionSupported returns whether server side encryption is implemented for this layer.
func (x *xObjects) IsEncryptionSupported() bool {
	return minio.GlobalKMS != nil || len(minio.GlobalGatewaySSE) > 0
}
