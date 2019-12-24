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

// TEMX implements a MinIO gateway ontop of TemporalX
type TEMX struct {
	HTTPAddr string
	GRPCAddr string
	DSPath   string
	XAddr    string
}

// infoAPIServer provides access to the InfoAPI
// allowing retrieval of the corresponding ipfs cids
// for our various buckets and objects
type infoAPIServer struct {
	InfoAPIServer
	httpServer *http.Server
	httpMux    *runtime.ServeMux
	grpcServer *grpc.Server
}

// xObjects bridges S3 -> TemporalX (IPFS)
type xObjects struct {
	minio.GatewayUnsupported
	mu        sync.Mutex
	dagClient pb.DagAPIClient
	ctx       context.Context

	// ledgerStore is responsible for updating our internal ledger state
	ledgerStore *LedgerStore

	infoAPI *infoAPIServer

	listener net.Listener
}

func init() {
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
	// connect to TemporalX
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
	// instantiate our internal ledger
	ledger, err := g.newLedgerStore(g.DSPath)
	if err != nil {
		return nil, err
	}
	// create a grpc listener
	listener, err := net.Listen("tcp", g.GRPCAddr)
	if err != nil {
		return nil, err
	}
	// instantiate initial xObjects type
	// responsible for bridging S3 -> TemporalX (IPFS)
	xobj := &xObjects{
		dagClient:   pb.NewDagAPIClient(conn),
		ctx:         context.Background(),
		ledgerStore: ledger,
		infoAPI: &infoAPIServer{
			httpMux:    runtime.NewServeMux(),
			grpcServer: grpc.NewServer(),
		},
		listener: listener,
	}
	xobj.infoAPI.httpServer = &http.Server{
		Addr:    g.HTTPAddr,
		Handler: xobj.infoAPI.httpMux,
	}
	// register the grpc server
	RegisterInfoAPIServer(xobj.infoAPI.grpcServer, xobj)
	// register the grpc-gateway http endpoint
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
	go func() {
		go func() {
			<-xobj.ctx.Done()
			xobj.infoAPI.grpcServer.Stop()
			xobj.infoAPI.httpServer.Close()
		}()
		go xobj.infoAPI.grpcServer.Serve(xobj.listener)
		if err := xobj.infoAPI.httpServer.ListenAndServe(); err != nil {
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

// Shutdown is used to shutdown our xObjects service layer
func (x *xObjects) Shutdown(ctx context.Context) error {
	return x.ledgerStore.Close()
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
			break
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
			break
		}
		if req.GetObject() == "" {
			err = status.Error(codes.InvalidArgument, emptyObjectErr)
			break
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
