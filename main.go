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

	"indexer/app"
	"indexer/es"
	"indexer/gen/index/v1"
	"indexer/gen/search/v1"
	"indexer/jobqueue"
	"indexer/resource"
	"indexer/server"
	"indexer/store"

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

	// st := store.NewMemoryStore()
	st := store.NewPostgresStore(dbpool)

	queue := jobqueue.NewQueue(dbpool)

	app := app.New(st, esClientImpl, resources, queue)

	worker := jobqueue.NewWorker(dbpool, app.HandlerFunc(), jobqueue.WorkerConfig{
		Logger: log.Default(),
	})

	idxSrv := server.NewIndexer(app)
	searchSrv := server.NewSearcher(app)

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
