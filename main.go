package main

import (
	"context"
	"log"
	"net"
	"os"
	"strings"

	"indexer/es"
	"indexer/gen/index/v1"
	"indexer/gen/search/v1"
	"indexer/server"
	"indexer/store"

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

	esClient, err := es.New(es.Config{
		Addresses: esAddrs,
		Username:  esUser,
		Password:  esPass,
	})
	if err != nil {
		log.Fatalf("es client: %v", err)
	}

	dbpool, err := pgxpool.New(context.Background(), "postgres://user:pass@localhost:5432/indexer")
	if err != nil {
		log.Fatalf("pgxpool: %v", err)
	}
	defer dbpool.Close()

	// st := store.NewMemoryStore()
	st := store.NewPostgresStore(dbpool)
	idxSrv := server.NewIndexer(st, esClient)
	searchSrv := server.NewSearcher(esClient)

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
