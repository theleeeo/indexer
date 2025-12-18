package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"strings"

	"indexer/app"
	"indexer/es"
	"indexer/gen/index/v1"
	"indexer/gen/search/v1"
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

	for _, rc := range resources {
		if err := rc.Validate(); err != nil {
			log.Fatalf("error validating resource %q: %v", rc.Resource, err)
		}
	}

	log.Printf("loaded %d resource configurations", len(resources))
	for _, rc := range resources {
		log.Printf(" - resource %q index %q with %d field/s and %d relation/s", rc.Resource, rc.IndexName, len(rc.Fields), len(rc.Relations))
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

	dbpool, err := pgxpool.New(context.Background(), "postgres://user:pass@localhost:5432/indexer")
	if err != nil {
		log.Fatalf("pgxpool: %v", err)
	}
	defer dbpool.Close()

	// st := store.NewMemoryStore()
	st := store.NewPostgresStore(dbpool)
	app := app.New(st, esClientImpl)
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

	log.Printf("indexer listening on %s", grpcAddr)
	if err := g.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}

func loadResourceConfig(path string) ([]ResourceConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading file: %w", err)
	}
	var cfg map[string]ResourceConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("unmarshal yaml: %w", err)
	}

	resources := make([]ResourceConfig, 0, len(cfg))
	for name, rc := range cfg {
		rc.Resource = name
		resources = append(resources, rc)
	}

	return resources, nil
}
