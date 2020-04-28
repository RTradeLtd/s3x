package s3x

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"

	pb "github.com/RTradeLtd/TxPB/v3/go"
	badger "github.com/RTradeLtd/go-ds-badger/v2"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"github.com/ipfs/go-datastore"
	crdt "github.com/ipfs/go-ds-crdt"
	"github.com/minio/cli"
	minio "github.com/minio/minio/cmd"
	"github.com/minio/minio/pkg/auth"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

const (
	temxBackend = "s3x"
)

//DSType is a type of datastore that s3x supports, please remove all existing data before changing the datastore
type DSType string

const (
	//DSTypeBadger is a badger(v2) backed datastore
	DSTypeBadger = DSType("badger")
	//DSTypeCrdt is a crdt backed backed datastore
	DSTypeCrdt = DSType("crdt")
)

// TEMX implements a MinIO gateway on top of TemporalX
type TEMX struct {
	HTTPAddr  string
	GRPCAddr  string
	DSType    DSType
	DSPath    string
	CrdtTopic string
	XAddr     string
	Insecure  bool // whether or not we have an insecure connection to TemporalX
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
	ctx        context.Context
	dagClient  pb.NodeAPIClient
	fileClient pb.FileAPIClient

	// ledgerStore is responsible for updating our internal ledger state
	ledgerStore *ledgerStore

	infoAPI *infoAPIServer

	listener net.Listener
}

func init() {
	if err := minio.RegisterGatewayCommand(cli.Command{
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
				Name:  "ds.type",
				Usage: "the type backend to store ledger data in, supported values are [badger, crdt]",
				Value: "badger",
			},
			cli.StringFlag{
				Name:  "ds.topic",
				Usage: "the topic used for crdt pubsub",
				Value: "s3x-ledger",
			},
			cli.StringFlag{
				Name:  "temporalx.endpoint",
				Usage: "the endpoint of the temporalx api server",
				Value: "xapi.temporal.cloud:9090",
			},
			cli.BoolFlag{
				Name:  "temporalx.insecure",
				Usage: "initiate an insecure connection to the temporalx endpoint",
			},
		},
	}); err != nil {
		panic(err)
	}
}

func temxGatewayMain(ctx *cli.Context) {
	minio.StartGateway(ctx, &TEMX{
		HTTPAddr:  ctx.String("info.http.endpoint"),
		GRPCAddr:  ctx.String("info.grpc.endpoint"),
		DSPath:    ctx.String("ds.path"),
		DSType:    DSType(ctx.String("ds.type")),
		CrdtTopic: ctx.String("ds.topic"),
		XAddr:     ctx.String("temporalx.endpoint"),
		Insecure:  ctx.Bool("temporalx.insecure"),
	})
}

// newLedgerStore returns an instance of ledgerStore
func (g *TEMX) newLedgerStore(ctx context.Context, dag pb.NodeAPIClient, pub pb.PubSubAPIClient) (*ledgerStore, error) {
	switch g.DSType {
	case DSTypeBadger:
		return g.newBadgerLedgerStore(dag)
	case DSTypeCrdt:
		return g.newCrdtLedgerStore(ctx, dag, pub)
	}
	return nil, fmt.Errorf(`data store type "%v" not supported`, g.DSType)
}

// newBadgerLedgerStore returns an instance of ledgerStore that uses badgerv2
func (g *TEMX) newBadgerLedgerStore(dag pb.NodeAPIClient) (*ledgerStore, error) {
	opts := badger.DefaultOptions
	ds, err := badger.NewDatastore(g.DSPath, &opts)
	if err != nil {
		return nil, err
	}
	return newLedgerStore(ds, dag)
}

// newCrdtLedgerStore returns an instance of ledgerStore that uses crdt and backed by badgerv2
func (g *TEMX) newCrdtLedgerStore(ctx context.Context, dag pb.NodeAPIClient, pub pb.PubSubAPIClient) (*ledgerStore, error) {
	store, err := badger.NewDatastore(g.DSPath, &badger.DefaultOptions)
	if err != nil {
		return nil, err
	}
	//from the doc: The broadcaster can be shut down by canceling the given context. This must be done before Closing the crdt.Datastore, otherwise things may hang.
	ctx, cancel := context.WithCancel(ctx)
	cleanup := func() error {
		cancel()
		return store.Close()
	}
	defer func() {
		if cleanup != nil {
			_ = cleanup() //this condition can only be triggered after an error, so this error is ignored
		}
	}()
	pubsubBC, err := newCrdtBroadcaster(ctx, pub, g.CrdtTopic)
	if err != nil {
		return nil, err
	}
	opts := crdt.DefaultOptions()
	crdtds, err := crdt.New(store, datastore.NewKey("crdt"), newCrdtDAGSyncer(dag, store), pubsubBC, opts)
	if err != nil {
		return nil, err
	}
	ls, err := newLedgerStore(crdtds, dag)
	if err != nil {
		return nil, err
	}
	ls.cleanup = append(ls.cleanup, cleanup)
	cleanup = nil //disable defer cleanup
	return ls, nil
}

// returns an instance of xObjects
func (g *TEMX) getXObjects(creds auth.Credentials) (*xObjects, error) {
	ctx := context.TODO()
	var dialOpts []grpc.DialOption
	if g.Insecure {
		dialOpts = append(dialOpts, grpc.WithInsecure())
	} else {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(
			credentials.NewTLS(
				&tls.Config{
					InsecureSkipVerify: true,
				},
			),
		))
	}
	// connect to TemporalX
	conn, err := grpc.Dial(g.XAddr, dialOpts...)
	if err != nil {
		return nil, err
	}
	dag := pb.NewNodeAPIClient(conn)
	pub := pb.NewPubSubAPIClient(conn)
	// instantiate our internal ledger
	ledger, err := g.newLedgerStore(ctx, dag, pub)
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
		ctx:         ctx,
		dagClient:   dag,
		fileClient:  pb.NewFileAPIClient(conn),
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
		_ = xobj.infoAPI.grpcServer.Serve(xobj.listener)
	}()
	go func() {
		_ = xobj.infoAPI.httpServer.ListenAndServe()
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
	x.infoAPI.grpcServer.Stop()
	x.infoAPI.httpServer.Close()
	return x.ledgerStore.Close()
}

// StorageInfo is not relevant to TemporalX backend.
func (x *xObjects) StorageInfo(ctx context.Context, local bool) (si minio.StorageInfo) {
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
		hash string
		err  error
	)
	if req.GetBucket() == "" {
		return nil, status.Error(codes.InvalidArgument, "bucket name is empty")
	}
	if req.GetObject() == "" {
		// get bucket hash when object is not specified
		hash, err = x.ledgerStore.GetBucketHash(req.GetBucket())
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
	} else if req.ObjectDataOnly {
		// get object data hash
		h, _, err := x.ledgerStore.GetObjectDataHash(ctx, req.GetBucket(), req.GetObject())
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		hash = h
	} else {
		// get protocol buffer object hash
		hash, err = x.ledgerStore.GetObjectHash(ctx, req.GetBucket(), req.GetObject())
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
	}
	return &InfoResponse{
		Bucket: req.GetBucket(),
		Object: req.GetObject(),
		Hash:   hash,
	}, nil
}
