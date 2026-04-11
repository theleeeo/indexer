package tests

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/theleeeo/indexer/core"
	"github.com/theleeeo/indexer/es"
	"github.com/theleeeo/indexer/jobqueue"
	"github.com/theleeeo/indexer/projection"
	"github.com/theleeeo/indexer/resource"
	"github.com/theleeeo/indexer/source"
	"github.com/theleeeo/indexer/store"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	esContainer "github.com/testcontainers/testcontainers-go/modules/elasticsearch"
	pgContainer "github.com/testcontainers/testcontainers-go/modules/postgres"
)

// FakeProvider is a test source.Provider that serves data from in-memory maps.
type FakeProvider struct {
	mu        sync.Mutex
	resources map[string]map[string]any   // "type|id" -> data
	relations map[string][]map[string]any // "type|key" -> []data
}

func NewFakeProvider() *FakeProvider {
	return &FakeProvider{
		resources: make(map[string]map[string]any),
		relations: make(map[string][]map[string]any),
	}
}

func (f *FakeProvider) Clear() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.resources = make(map[string]map[string]any)
	f.relations = make(map[string][]map[string]any)
}

func (f *FakeProvider) SetResource(resourceType, resourceID string, data map[string]any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.resources[resourceType+"|"+resourceID] = data
}

func (f *FakeProvider) DeleteResource(resourceType, resourceID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.resources, resourceType+"|"+resourceID)
}

func (f *FakeProvider) SetRelated(resourceType string, keyValues []string, related []map[string]any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := resourceType
	for _, v := range keyValues {
		key += "|" + v
	}
	f.relations[key] = related
}

func (f *FakeProvider) FetchResource(_ context.Context, resourceType, resourceID string) (map[string]any, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	data, ok := f.resources[resourceType+"|"+resourceID]
	if !ok {
		return nil, nil
	}
	return data, nil
}

func (f *FakeProvider) FetchRelated(_ context.Context, params source.FetchRelatedParams) (source.FetchRelatedResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	// Build lookup key by joining all field values in order.
	key := params.ResourceType
	for _, kv := range params.Keys {
		key += "|" + kv.Value
	}
	data, ok := f.relations[key]
	if !ok {
		return source.FetchRelatedResult{}, nil
	}
	return source.FetchRelatedResult{Related: data}, nil
}

type TestSuite struct {
	suite.Suite

	esContainer *esContainer.ElasticsearchContainer
	pgContainer *pgContainer.PostgresContainer

	pool *pgxpool.Pool

	esClient *elasticsearch.Client

	idx *core.Indexer

	cancelWorker context.CancelFunc
	worker       *jobqueue.Worker

	fakeProvider *FakeProvider
	st           *store.PostgresStore
}

var DefaultResourceConfig = resource.Configs{
	{
		Resource: "a",
		Fields: []resource.FieldConfig{
			{Name: "field1"},
			{Name: "field2"},
		},
		Relations: []resource.RelationConfig{
			{
				Resource: "b",
				Key:      resource.KeyConfig{Source: "a", Fields: []string{"id"}},
				Fields: []resource.FieldConfig{
					{Name: "field1"},
					{Name: "field2"},
				},
			},
		},
	},
	{
		Resource: "b",
		Fields: []resource.FieldConfig{
			{Name: "field1"},
			{Name: "field2"},
		},
		Relations: []resource.RelationConfig{},
	},
}

var RelatedResourceConfig = resource.Configs{
	{
		Resource: "a",
		Fields: []resource.FieldConfig{
			{Name: "f1"},
		},
		Relations: []resource.RelationConfig{
			{
				Resource: "b",
				Key:      resource.KeyConfig{Source: "a", Fields: []string{"id"}},
				Fields:   []resource.FieldConfig{{Name: "f1"}},
			},
		},
	},
	{
		Resource: "b",
		Fields: []resource.FieldConfig{
			{Name: "f1"},
		},
		Relations: []resource.RelationConfig{
			{
				Resource: "a",
				Key:      resource.KeyConfig{Source: "b", Fields: []string{"id"}},
				Fields:   []resource.FieldConfig{{Name: "f1"}},
			},
		},
	},
	{
		Resource: "c",
		Fields: []resource.FieldConfig{
			{Name: "f1"},
		},
		Relations: []resource.RelationConfig{
			{
				Resource: "a",
				Key:      resource.KeyConfig{Source: "c", Fields: []string{"id"}},
				Fields:   []resource.FieldConfig{{Name: "f1"}},
			},
			{
				Resource: "b",
				Key:      resource.KeyConfig{Source: "c", Fields: []string{"id"}},
				Fields:   []resource.FieldConfig{{Name: "f1"}},
			},
		},
	},
}

func (t *TestSuite) verifyResourceConfigs() {
	must(t.T(), DefaultResourceConfig.Validate())
	must(t.T(), RelatedResourceConfig.Validate())
}

func must(t *testing.T, err error) {
	if err != nil {
		t.Fatal(err)
	}
}

func (t *TestSuite) SetupSuite() {
	log.SetOutput(os.Stderr)
	t.T().Log("setting up the suite")

	t.verifyResourceConfigs()

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

	t.st = store.NewPostgresStore(dbpool)
	t.fakeProvider = NewFakeProvider()

	plans := projection.BuildPlansFromConfig(t.fakeProvider, DefaultResourceConfig, t.st)
	builder := projection.NewBuilder(plans, DefaultResourceConfig, t.st)

	t.idx = core.New(core.Config{
		Builder:   builder,
		Resources: DefaultResourceConfig,
		ES:        es.New(esClient, true),
		Store:     t.st,
		Queue:     jobqueue.NewQueue(dbpool),
	})

	t.worker = jobqueue.NewWorker(t.pool, t.idx.HandlerFunc(), jobqueue.WorkerConfig{
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

// setResourceConfig rebuilds the aggregation plans from the given resource
// config and updates the indexer's builder. This is the test equivalent of
// dynamically changing the resource configuration at runtime.
func (t *TestSuite) setResourceConfig(resources resource.Configs) {
	plans := projection.BuildPlansFromConfig(t.fakeProvider, resources, t.st)
	builder := projection.NewBuilder(plans, resources, t.st)
	t.idx.SetBuilder(builder, resources)
}

func (t *TestSuite) BeforeTest(suiteName, testName string) {
}

func (t *TestSuite) AfterTest(suiteName, testName string) {
	// Clear fake provider data between tests.
	t.fakeProvider.Clear()

	// ES 8.x disallows _all / wildcard deletes by default.
	// List concrete index names first, then delete each one.
	catRes, err := t.esClient.Cat.Indices(
		t.esClient.Cat.Indices.WithFormat("json"),
	)
	if err != nil {
		t.T().Fatalf("failed to list indices: %v", err)
	}
	defer catRes.Body.Close()

	var indices []struct {
		Index string `json:"index"`
	}
	if err := json.NewDecoder(catRes.Body).Decode(&indices); err != nil {
		t.T().Fatalf("failed to decode cat indices: %v", err)
	}

	for _, idx := range indices {
		res, err := t.esClient.Indices.Delete([]string{idx.Index})
		if err != nil {
			t.T().Fatalf("failed to delete index %s: %v", idx.Index, err)
		}
		res.Body.Close()
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
