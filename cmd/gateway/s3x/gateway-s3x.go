package s3x

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"os"
	"sync"

	pb "github.com/RTradeLtd/TxPB/go"
	badger "github.com/RTradeLtd/go-ds-badger/v2"
	minio "github.com/RTradeLtd/s3x/cmd"
	"github.com/RTradeLtd/s3x/pkg/auth"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"github.com/minio/cli"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

const (
	temxBackend  = "s3x"
	grpcendpoint = "0.0.0.0:8888"
	httpendpoint = "0.0.0.0:8889"
)

func init() {
	// TODO(bonedaddy): add help command
	minio.RegisterGatewayCommand(cli.Command{
		Name:        temxBackend,
		Usage:       "TemporalX IPFS Gateway",
		Description: "s3x provides a minio gateway that uses IPFS as the datastore through TemporalX's gRPC API",
		Action:      temxGatewayMain,
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:  "info.endpoint",
				Usage: "the endpoint to serve the info api on",
			},
		},
	})
}

func temxGatewayMain(ctx *cli.Context) {
	minio.StartGateway(ctx, &TEMX{})
}

// TEMX implements a MinIO gateway ontop of TemporalX
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
	conn, err := grpc.Dial("xapi-dev.temporal.cloud:9090",
		grpc.WithTransportCredentials(
			credentials.NewTLS(
				&tls.Config{
					InsecureSkipVerify: true,
				},
			),
		),
	)
	if err != nil {
		return nil, err
	}
	ledger, err := g.newLedgerStore(g.getDSPath())
	if err != nil {
		return nil, err
	}
	xobj := &xObjects{
		creds:     creds,
		dagClient: pb.NewDagAPIClient(conn),
		httpClient: &http.Client{
			Transport: minio.NewCustomHTTPTransport(),
		},
		ctx:         context.Background(),
		ledgerStore: ledger,
		infoAPI: &apiServer{
			httpMux:    runtime.NewServeMux(),
			grpcServer: grpc.NewServer(),
		},
	}
	RegisterInfoAPIServer(xobj.infoAPI.grpcServer, xobj)
	if err := RegisterInfoAPIHandlerFromEndpoint(
		xobj.ctx,
		xobj.infoAPI.httpMux,
		httpendpoint,
		[]grpc.DialOption{grpc.WithInsecure()},
	); err != nil {
		return nil, err
	}
	listener, err := net.Listen("tcp", grpcendpoint)
	if err != nil {
		return nil, err
	}
	go func() {
		httpServer := &http.Server{
			Addr:    httpendpoint,
			Handler: xobj.infoAPI.httpMux,
		}
		go func() {
			select {
			case <-xobj.ctx.Done():
				xobj.infoAPI.grpcServer.Stop()
				httpServer.Close()
			}
		}()
		go httpServer.ListenAndServe()
		xobj.infoAPI.grpcServer.Serve(listener)
	}()
	return xobj, nil
}

// Name returns the name of the TemporalX gateway backend
func (g *TEMX) Name() string {
	return temxBackend
}

// Production indicates that this backend is suitable for production use
func (g *TEMX) Production() bool {
	return true
}

type apiServer struct {
	InfoAPIServer
	httpMux    *runtime.ServeMux
	grpcServer *grpc.Server
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

	infoAPI *apiServer
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

func (x *xObjects) GetHash(ctx context.Context, req *InfoRequest) (*InfoRequest, error) {
	return nil, errors.New("not yet implemented")
}
