package main

import (
	"log"
	"net"
	"os"
	"strings"

	"indexer/es"
	"indexer/gen/indexer/v1"
	"indexer/gen/searcher"
	"indexer/server"
	"indexer/store"

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

	st := store.New()
	idxSrv := server.NewIndexer(st, esClient)
	searchSrv := server.NewSearcher(esClient)

	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	g := grpc.NewServer()
	indexer.RegisterIndexerServer(g, idxSrv)
	searcher.RegisterSearcherServer(g, searchSrv)
	reflection.Register(g)

	log.Printf("indexer listening on %s", grpcAddr)
	if err := g.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
