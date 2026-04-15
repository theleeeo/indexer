package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"

	"github.com/theleeeo/indexer/core"
	"github.com/theleeeo/indexer/dsl"
	"github.com/theleeeo/indexer/es"
	"github.com/theleeeo/indexer/gen/index/v1"
	"github.com/theleeeo/indexer/gen/search/v1"
	"github.com/theleeeo/indexer/jobqueue"
	"github.com/theleeeo/indexer/projection"
	"github.com/theleeeo/indexer/resource"
	"github.com/theleeeo/indexer/server"
	"github.com/theleeeo/indexer/source"
	"github.com/theleeeo/indexer/store"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/goccy/go-yaml"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	grpcAddr := env("GRPC_ADDR", ":9000")

	esAddrs := strings.Split(env("ES_ADDRS", "http://localhost:9200"), ",")
	esUser := env("ES_USERNAME", "")
	esPass := env("ES_PASSWORD", "")

	resourceConfigPath := env("RESOURCE_CONFIG_PATH", "resources.yml")
	resources, err := loadResourceConfig(resourceConfigPath)
	if err != nil {
		log.Fatalf("load resource config: %v", err)
	}

	if err := resources.Validate(); err != nil {
		log.Fatalf("invalid resource config: %v", err)
	}

	log.Printf("loaded %d resource configurations", len(resources))
	for _, rc := range resources {
		log.Printf(" - resource %q with %d field/s and %d relation/s", rc.Resource, len(rc.Fields), len(rc.Relations))
	}

	esClient, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: esAddrs,
		Username:  esUser,
		Password:  esPass,
	})
	if err != nil {
		log.Fatalf("setting up es client: %v", err)
	}

	esClientImpl := es.New(esClient, false)

	pgAddr := env("PG_ADDR", "postgres://user:pass@localhost:5432/indexer")
	dbpool, err := pgxpool.New(context.Background(), pgAddr)
	if err != nil {
		log.Fatalf("pgxpool: %v", err)
	}
	defer dbpool.Close()

	slog.SetLogLoggerLevel(slog.LevelDebug)

	st := store.NewPostgresStore(dbpool)

	queue := jobqueue.NewQueue(dbpool)

	// Source provider - connect to the provider plugin over gRPC.
	providerAddr := env("PROVIDER_ADDR", "")
	if providerAddr == "" {
		log.Fatalf("PROVIDER_ADDR is required (address of the gRPC provider plugin)")
	}
	sourceProvider, err := source.NewGRPCProvider(providerAddr)
	if err != nil {
		log.Fatalf("connect to provider plugin: %v", err)
	}
	defer sourceProvider.Close()

	plans := dsl.BuildPlansFromConfig(sourceProvider, resources)
	builder := projection.NewBuilder(plans, resources, st)

	idx := core.New(core.Config{
		Builder:   builder,
		Resources: resources,
		ES:        esClientImpl,
		Store:     st,
		Queue:     queue,
	})

	worker := jobqueue.NewWorker(dbpool, idx.HandlerFunc(), jobqueue.WorkerConfig{
		Logger: log.Default(),
	})

	idxSrv := server.NewIndexer(idx)
	searchSrv := server.NewSearcher(idx)

	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	g := grpc.NewServer()
	index.RegisterIndexServiceServer(g, idxSrv)
	search.RegisterSearchServiceServer(g, searchSrv)
	reflection.Register(g)

	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt)

	wg := sync.WaitGroup{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wg.Go(func() {
		log.Printf("starting job queue worker")
		worker.Run(ctx)
		log.Printf("job queue worker stopped")
	})

	wg.Go(func() {
		log.Printf("gRPC server listening on %s", grpcAddr)
		if err := g.Serve(lis); err != nil {
			log.Printf("gRPC server error: %v", err)
		}
		log.Printf("gRPC server stopped")
	})

	<-stopChan
	log.Printf("shutting down")

	go func() {
		<-stopChan
		log.Printf("force shutdown")
		os.Exit(1)
	}()

	cancel()

	g.GracefulStop()

	wg.Wait()
}

func loadResourceConfig(path string) (resource.Configs, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading file: %w", err)
	}
	var cfg map[string]*resource.Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("unmarshal yaml: %w", err)
	}

	resources := make([]*resource.Config, 0, len(cfg))
	for name, rc := range cfg {
		rc.Resource = name
		resources = append(resources, rc)
	}

	return resources, nil
}
