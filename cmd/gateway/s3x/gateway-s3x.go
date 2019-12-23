package s3x

import (
	"context"
	"crypto/tls"
	"log"
	"net"
	"net/http"
	"sync"

	pb "github.com/RTradeLtd/TxPB/go"
	badger "github.com/RTradeLtd/go-ds-badger/v2"
	minio "github.com/RTradeLtd/s3x/cmd"
	"github.com/RTradeLtd/s3x/pkg/auth"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"github.com/minio/cli"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

const (
	temxBackend = "s3x"
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
				Name:  "info.http.endpoint",
				Usage: "the endpoint to serve the info http api on",
				Value: "0.0.0.0:8889",
			},
			cli.StringFlag{
				Name:  "info.grpc.endpoint",
				Usage: "the endpoint to serve the info grpc api on",
				Value: "0.0.0.0:8888",
			},
			cli.StringFlag{
				Name:  "ds.path",
				Usage: "the path to store ledger data in",
				Value: "s3xstore",
			},
			cli.StringFlag{
				Name:  "temporalx.endpoint",
				Usage: "the endpoint of the temporalx api server",
				Value: "xapi-dev.temporal.cloud:9090",
			},
		},
	})
}

func temxGatewayMain(ctx *cli.Context) {
	minio.StartGateway(ctx, &TEMX{
		HTTPAddr: ctx.String("info.http.endpoint"),
		GRPCAddr: ctx.String("info.grpc.endpoint"),
		DSPath:   ctx.String("ds.path"),
		XAddr:    ctx.String("temporalx.endpoint"),
	})
}

// TEMX implements a MinIO gateway ontop of TemporalX
type TEMX struct {
	HTTPAddr string
	GRPCAddr string
	DSPath   string
	XAddr    string
}

// newLedgerStore returns an instance of ledgerStore that uses badgerv2
func (g *TEMX) newLedgerStore(dsPath string) (*LedgerStore, error) {
	opts := badger.DefaultOptions
	ds, err := badger.NewDatastore(dsPath, &opts)
	if err != nil {
		return nil, err
	}
	return newLedgerStore(ds), nil
}

// returns an instance of xObjects
func (g *TEMX) getXObjects(creds auth.Credentials) (*xObjects, error) {
	conn, err := grpc.Dial(g.XAddr,
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
	ledger, err := g.newLedgerStore(g.DSPath)
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
		g.GRPCAddr,
		[]grpc.DialOption{grpc.WithInsecure()},
	); err != nil {
		return nil, err
	}
	return xobj, nil
}

// NewGatewayLayer creates a minio gateway layer powered y TemporalX
func (g *TEMX) NewGatewayLayer(creds auth.Credentials) (minio.ObjectLayer, error) {
	xobj, err := g.getXObjects(creds)
	if err != nil {
		return nil, err
	}
	listener, err := net.Listen("tcp", g.GRPCAddr)
	if err != nil {
		return nil, err
	}
	// TODO(bonedaddy): clean this trash up
	go func() {
		httpServer := &http.Server{
			Addr:    g.HTTPAddr,
			Handler: xobj.infoAPI.httpMux,
		}
		go func() {
			select {
			case <-xobj.ctx.Done():
				xobj.infoAPI.grpcServer.Stop()
				httpServer.Close()
			}
		}()
		go xobj.infoAPI.grpcServer.Serve(listener)
		if err := httpServer.ListenAndServe(); err != nil {
			log.Print("error in http server", err)
		}
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

func (x *xObjects) GetHash(ctx context.Context, req *InfoRequest) (*InfoResponse, error) {
	var (
		err            error
		resp           *InfoResponse
		hash           string
		emptyBucketErr = "bucket name is empty"
		emptyObjectErr = "object name is empty"
	)
	switch req.GetObject() {
	case "":
		if req.GetBucket() == "" {
			err = status.Error(codes.InvalidArgument, emptyBucketErr)
		}
		hash, err = x.ledgerStore.GetBucketHash(req.GetBucket())
		if err != nil {
			err = status.Error(codes.Internal, err.Error())
		} else {
			resp = &InfoResponse{
				Bucket: req.GetBucket(),
				Hash:   hash,
			}
		}
	default:
		if req.GetBucket() == "" {
			err = status.Error(codes.InvalidArgument, emptyBucketErr)
		}
		if req.GetObject() == "" {
			err = status.Error(codes.InvalidArgument, emptyObjectErr)
		}
		hash, err = x.ledgerStore.GetObjectHash(req.GetBucket(), req.GetObject())
		if err != nil {
			err = status.Error(codes.Internal, err.Error())
		} else {
			resp = &InfoResponse{
				Bucket: req.GetBucket(),
				Object: req.GetObject(),
				Hash:   hash,
			}
		}
	}
	return resp, err
}
