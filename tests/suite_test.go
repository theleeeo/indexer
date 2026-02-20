package tests

import (
	"context"
	"crypto/tls"
	"fmt"
	"indexer/app"
	"indexer/es"
	"indexer/jobqueue"
	"indexer/resource"
	"indexer/store"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	esContainer "github.com/testcontainers/testcontainers-go/modules/elasticsearch"
	pgContainer "github.com/testcontainers/testcontainers-go/modules/postgres"
)

type TestSuite struct {
	suite.Suite

	esContainer *esContainer.ElasticsearchContainer
	pgContainer *pgContainer.PostgresContainer

	pool *pgxpool.Pool

	esClient *elasticsearch.Client

	app *app.App

	cancelWorker context.CancelFunc
	worker       *jobqueue.Worker
}

func (t *TestSuite) SetupSuite() {
	log.SetOutput(os.Stderr)
	t.T().Log("setting up the suite")

	wg := sync.WaitGroup{}
	containerCtx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	wg.Go(func() {
		elasticsearchContainer, err := esContainer.Run(containerCtx, "docker.elastic.co/elasticsearch/elasticsearch:8.9.0")
		if err != nil {
			t.FailNow("failed to start elasticsearch container", err)
		}
		t.esContainer = elasticsearchContainer
	})

	var (
		pgDB   = "indexer"
		pgUser = "user"
		pgPass = "pass"
	)

	wg.Go(func() {
		postgresContainer, err := pgContainer.Run(containerCtx,
			"postgres:17",
			// pgContainer.WithInitScripts(filepath.Join("testdata", "init-user-db.sh")),
			// pgContainer.WithConfigFile(filepath.Join("testdata", "my-postgres.conf")),
			pgContainer.WithDatabase(pgDB),
			pgContainer.WithUsername(pgUser),
			pgContainer.WithPassword(pgPass),
			pgContainer.BasicWaitStrategies(),
		)
		if err != nil {
			t.FailNow("failed to start postgres container", err)
		}
		t.pgContainer = postgresContainer
	})

	wg.Wait()

	esAddr, err := t.esContainer.Endpoint(containerCtx, "https")
	if err != nil {
		t.FailNow("failed to get elasticsearch endpoint", err)
	}

	esClient, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{esAddr},
		// Trust the self-signed certs used by elasticsearch
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
		Username: t.esContainer.Settings.Username,
		Password: t.esContainer.Settings.Password,
	})
	if err != nil {
		log.Fatalf("setting up es client: %v", err)
	}
	t.esClient = esClient

	pgAddr, err := t.pgContainer.Endpoint(containerCtx, "")
	if err != nil {
		t.FailNow("failed to get postgres endpoint", err)
	}
	dbpool, err := pgxpool.New(context.Background(), fmt.Sprintf("postgres://%s:%s@%s/%s", pgUser, pgPass, pgAddr, pgDB))
	if err != nil {
		log.Fatalf("pgxpool: %v", err)
	}
	t.pool = dbpool

	appSchema, err := os.ReadFile(filepath.Join("..", "store", "pg_schema.sql"))
	if err != nil {
		t.T().Fatal(err)
	}

	if _, err := t.pool.Exec(t.T().Context(), string(appSchema)); err != nil {
		t.T().Fatalf("failed to apply schema: %v", err)
	}

	jobQueueSchema, err := os.ReadFile(filepath.Join("..", "jobqueue", "schema.sql"))
	if err != nil {
		t.T().Fatal(err)
	}

	if _, err := t.pool.Exec(t.T().Context(), string(jobQueueSchema)); err != nil {
		t.T().Fatalf("failed to apply job queue schema: %v", err)
	}

	resources := []*resource.Config{
		{
			Resource: "a",
			Fields: []resource.FieldConfig{
				{
					Name: "field1",
				},
				{
					Name: "field2",
				},
			},
			Relations: []resource.RelationConfig{
				{
					Resource: "b",
					Fields: []resource.FieldConfig{
						{
							Name: "field1",
						},
						{
							Name: "field2",
						},
					},
				},
			},
		},
		{
			Resource: "b",
			Fields: []resource.FieldConfig{
				{
					Name: "field1",
				},
				{
					Name: "field2",
				},
			},
			Relations: []resource.RelationConfig{},
		},
	}

	t.app = app.New(store.NewPostgresStore(dbpool), es.New(esClient, true), resources, jobqueue.NewQueue(dbpool))

	t.worker = jobqueue.NewWorker(t.pool, t.app.HandlerFunc(), jobqueue.WorkerConfig{
		Logger: log.Default(),
	})

	workerCtx, cancelWorker := context.WithCancel(context.Background())
	t.cancelWorker = cancelWorker
	go t.worker.Run(workerCtx)
}

func (t *TestSuite) TearDownSuite() {
	if err := testcontainers.TerminateContainer(t.esContainer); err != nil {
		log.Printf("failed to terminate elasticsearch container: %s", err)
	}

	t.pool.Close()

	if err := testcontainers.TerminateContainer(t.pgContainer); err != nil {
		log.Printf("failed to terminate postgres container: %s", err)
	}

	t.cancelWorker()

	t.worker.Wait()
}

func (t *TestSuite) SetupTest() {
}

func (t *TestSuite) BeforeTest(suiteName, testName string) {
}

func (t *TestSuite) AfterTest(suiteName, testName string) {
	_, err := t.esClient.Indices.Delete([]string{"_all"})
	if err != nil {
		t.T().Fatalf("failed to clear all indices: %v", err)
	}

	rows, err := t.pool.Query(t.T().Context(), `
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = 'public'
    `)
	if err != nil {
		t.T().Fatalf("failed to list tables: %v", err)
	}
	defer rows.Close()

	var tableNames []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			t.T().Fatalf("failed to scan table name: %v", err)
		}
		tableNames = append(tableNames, tableName)
	}
	if err := rows.Err(); err != nil {
		t.T().Fatalf("rows error: %v", err)
	}

	for _, tableName := range tableNames {
		if _, err := t.pool.Exec(t.T().Context(), fmt.Sprintf("TRUNCATE TABLE %s CASCADE", tableName)); err != nil {
			t.T().Fatalf("failed to drop table %s: %v", tableName, err)
		}
	}
}

func Test_TestSuite(t *testing.T) {
	suite.Run(t, new(TestSuite))
}
