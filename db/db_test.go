package db

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jmoiron/sqlx"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/fikrimohammad/go-dev-sdk/observability/attributes"
	"github.com/fikrimohammad/go-dev-sdk/observability/metrics"
	"github.com/fikrimohammad/go-dev-sdk/observability/tracer"
)

// --- fake metrics client -----------------------------------------------------------

type metricRec struct {
	name  string
	value float64
	attrs map[string]any
}

type fakeMetrics struct {
	mu     sync.Mutex
	counts []metricRec
	hists  []metricRec
}

func (f *fakeMetrics) Count(_ context.Context, name string, value int64, attrs map[string]any) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.counts = append(f.counts, metricRec{name, float64(value), normalizeAttrs(attrs)})
	return nil
}

func (f *fakeMetrics) Histogram(_ context.Context, name string, value float64, attrs map[string]any) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hists = append(f.hists, metricRec{name, value, normalizeAttrs(attrs)})
	return nil
}

func (f *fakeMetrics) Stop(context.Context) error { return nil }

func (f *fakeMetrics) lastCount() (metricRec, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.counts) == 0 {
		return metricRec{}, false
	}
	return f.counts[len(f.counts)-1], true
}

func (f *fakeMetrics) lastHist() (metricRec, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.hists) == 0 {
		return metricRec{}, false
	}
	return f.hists[len(f.hists)-1], true
}

func (f *fakeMetrics) nums() (int, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.counts), len(f.hists)
}

// --- fake: span capture ------------------------------------------------------------

type recordingExporter struct {
	mu    sync.Mutex
	spans []sdktrace.ReadOnlySpan
}

func (e *recordingExporter) ExportSpans(_ context.Context, spans []sdktrace.ReadOnlySpan) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.spans = append(e.spans, spans...)
	return nil
}

func (e *recordingExporter) Shutdown(context.Context) error { return nil }

func (e *recordingExporter) last() (sdktrace.ReadOnlySpan, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.spans) == 0 {
		return nil, false
	}
	return e.spans[len(e.spans)-1], true
}

func (e *recordingExporter) count() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.spans)
}

func spanAttrs(s sdktrace.ReadOnlySpan) map[string]any {
	out := make(map[string]any)
	for _, kv := range s.Attributes() {
		out[string(kv.Key)] = kv.Value.String()
	}
	return out
}

// --- fake: sql stub driver ---------------------------------------------------------

var testBackend = &stubDB{}

type stubCall struct {
	query string
	args  []driver.Value
}

type stubDB struct {
	mu      sync.Mutex
	queries []stubCall
	err     error
	cols    []string
	rows    [][]driver.Value
}

type stubDriver struct{}

func init() { sql.Register("dbstub", stubDriver{}) }

func (stubDriver) Open(string) (driver.Conn, error) {
	return &stubConn{backend: testBackend}, nil
}

type stubConn struct{ backend *stubDB }

func (c *stubConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare unsupported")
}
func (c *stubConn) Close() error              { return nil }
func (c *stubConn) Begin() (driver.Tx, error) { return stubTx{}, nil }

type stubTx struct{}

func (stubTx) Commit() error   { return nil }
func (stubTx) Rollback() error { return nil }

func (c *stubConn) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	return c.backend.exec(query, args)
}

func (c *stubConn) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	return c.backend.query(query, args)
}

func (b *stubDB) exec(query string, args []driver.NamedValue) (driver.Result, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.queries = append(b.queries, stubCall{query, namedValuesToValues(args)})
	if b.err != nil {
		err := b.err
		b.err = nil
		return nil, err
	}
	return stubResult{}, nil
}

func (b *stubDB) query(query string, args []driver.NamedValue) (driver.Rows, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.queries = append(b.queries, stubCall{query, namedValuesToValues(args)})
	return &stubRows{cols: b.cols, rows: b.rows}, nil
}

func namedValuesToValues(args []driver.NamedValue) []driver.Value {
	out := make([]driver.Value, len(args))
	for i, a := range args {
		out[i] = a.Value
	}
	return out
}

type stubResult struct{}

func (stubResult) LastInsertId() (int64, error) { return 42, nil }
func (stubResult) RowsAffected() (int64, error) { return 1, nil }

type stubRows struct {
	cols  []string
	rows  [][]driver.Value
	index int
}

func (r *stubRows) Columns() []string { return r.cols }
func (r *stubRows) Close() error      { return nil }
func (r *stubRows) Next(dest []driver.Value) error {
	if r.index >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.index])
	r.index++
	return nil
}

// setupTest installs a fresh stub DB plus a capturing tracer and metrics client,
// and returns the instrumented DB alongside assertion handles. It runs against
// the MySQL driver by default; see setupTestWithDriver for other dialects.
func setupTest(t *testing.T, d *stubDB) (DB, *fakeMetrics, *recordingExporter) {
	t.Helper()
	return setupTestWithDriver(t, d, mysqlDriver, "mysql", "report_db")
}

func setupTestWithDriver(t *testing.T, d *stubDB, spec driverSpec, systemName, namespace string) (DB, *fakeMetrics, *recordingExporter) {
	t.Helper()

	testBackend.mu.Lock()
	testBackend.queries = nil
	testBackend.err = nil
	testBackend.cols = d.cols
	testBackend.rows = d.rows
	testBackend.mu.Unlock()

	sqlDB, err := sql.Open("dbstub", "test")
	if err != nil {
		t.Fatalf("open sql: %v", err)
	}
	sqlDB.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = sqlDB.Close() })

	ex := &recordingExporter{}
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(ex)))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	tracer.SetDefault(tracer.Wrap(tp))

	fm := &fakeMetrics{}
	metrics.SetDefault(fm)

	return &dbConn{DB: sqlx.NewDb(sqlDB, spec.name), driver: spec, meta: meta{
		systemName: systemName,
		namespace:  namespace,
		serverAddr: "db.example.com",
		serverPort: spec.port,
	}}, fm, ex
}

func stubExecErr(t *testing.T, err error) {
	t.Helper()
	testBackend.mu.Lock()
	defer testBackend.mu.Unlock()
	testBackend.err = err
}

func recordedQuery(t *testing.T) stubCall {
	t.Helper()
	testBackend.mu.Lock()
	defer testBackend.mu.Unlock()
	if len(testBackend.queries) == 0 {
		t.Fatal("no query recorded")
	}
	return testBackend.queries[0]
}

func numRecordedQueries(t *testing.T) int {
	t.Helper()
	testBackend.mu.Lock()
	defer testBackend.mu.Unlock()
	return len(testBackend.queries)
}

// normalizeAttrs mirrors the OTel key normalization applied by the real metrics
// and tracer clients (attributes.NormalizeKey: order_id -> order.id), so the
// fake records the keys as they are emitted.
func normalizeAttrs(attrs map[string]any) map[string]any {
	out := make(map[string]any, len(attrs))
	for k, v := range attrs {
		out[attributes.NormalizeKey(k)] = v
	}
	return out
}

// --- tests ----------------------------------------------------------------------------

func TestConnect(t *testing.T) {
	db, err := Connect(Config{
		Driver:       "dbstub",
		DSN:          "test",
		Host:         "localhost",
		Port:         3306,
		Database:     "report_db",
		Username:     "user",
		Password:     "pass",
		MaxOpenConns: 5,
	})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	c := db.(*dbConn)
	if c.systemName != "dbstub" {
		t.Fatalf("systemName = %q, want dbstub", c.systemName)
	}
	if c.namespace != "report_db" {
		t.Fatalf("namespace = %q, want report_db", c.namespace)
	}
	if c.serverAddr != "localhost" || c.serverPort != 3306 {
		t.Fatalf("server = %s:%d, want localhost:3306", c.serverAddr, c.serverPort)
	}
	if got := c.DB.Stats().MaxOpenConnections; got != 5 {
		t.Fatalf("MaxOpenConnections = %d, want 5", got)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestConnect_InjectedClients(t *testing.T) {
	testBackend.mu.Lock()
	testBackend.queries = nil
	testBackend.err = nil
	testBackend.cols = []string{"id", "name"}
	testBackend.rows = [][]driver.Value{{int64(1), "a"}}
	testBackend.mu.Unlock()

	// Package-level defaults: fresh, must receive nothing.
	defEx := &recordingExporter{}
	defTp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(defEx)))
	t.Cleanup(func() { _ = defTp.Shutdown(context.Background()) })
	tracer.SetDefault(tracer.Wrap(defTp))
	defFm := &fakeMetrics{}
	metrics.SetDefault(defFm)

	// Injected clients: must receive the events.
	injEx := &recordingExporter{}
	injTp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(injEx)))
	t.Cleanup(func() { _ = injTp.Shutdown(context.Background()) })
	injFm := &fakeMetrics{}

	db, err := Connect(Config{Driver: "dbstub", DSN: "test", Host: "localhost", Port: 3306, Database: "report_db", Username: "user"},
		WithMetrics(injFm), WithTracer(tracer.Wrap(injTp)))
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}

	var items []report
	if err := db.NamedSelectContext(context.Background(), &items,
		"SELECT id, name FROM t WHERE id = :id", map[string]any{"id": int64(1)}); err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(items) != 1 || items[0].ID != 1 {
		t.Fatalf("items = %+v", items)
	}

	if n := injEx.count(); n != 1 {
		t.Fatalf("injected tracer spans = %d, want 1", n)
	}
	if _, ok := injFm.lastCount(); !ok {
		t.Fatal("injected metrics client received no count")
	}

	if n := defEx.count(); n != 0 {
		t.Fatalf("package default tracer spans = %d, want 0", n)
	}
	nc, nh := defFm.nums()
	if nc != 0 || nh != 0 {
		t.Fatalf("package default metrics received counts=%d hists=%d, want 0", nc, nh)
	}
}

func TestNamedExecContext_Success(t *testing.T) {
	dbx, fm, ex := setupTest(t, &stubDB{})

	res, err := dbx.NamedExecContext(context.Background(),
		"INSERT INTO t (id, name) VALUES (:id, :name)",
		map[string]any{"id": int64(1), "name": "a"})
	if err != nil {
		t.Fatalf("NamedExecContext: %v", err)
	}
	if id, _ := res.LastInsertId(); id != 42 {
		t.Fatalf("LastInsertId = %d, want 42", id)
	}

	q := recordedQuery(t)
	if q.query != "INSERT INTO t (id, name) VALUES (?, ?)" {
		t.Fatalf("query = %q, want bound query", q.query)
	}
	if len(q.args) != 2 || q.args[0] != int64(1) || q.args[1] != "a" {
		t.Fatalf("args = %v, want [1 a]", q.args)
	}

	count, ok := fm.lastCount()
	if !ok {
		t.Fatal("no count recorded")
	}
	if count.name != metricCount || count.value != 1 {
		t.Fatalf("count = %+v", count)
	}
	if count.attrs["db.system.name"] != "mysql" || count.attrs["db.operation.name"] != "INSERT" {
		t.Fatalf("count attrs = %v", count.attrs)
	}
	if count.attrs["db.namespace"] != "report_db" {
		t.Fatalf("count db.namespace = %v, want report_db", count.attrs["db.namespace"])
	}
	if count.attrs["server.address"] != "db.example.com" {
		t.Fatalf("count server.address = %v, want db.example.com", count.attrs["server.address"])
	}
	if count.attrs["server.port"] != 3306 {
		t.Fatalf("count server.port = %v, want 3306", count.attrs["server.port"])
	}
	if count.attrs["db.query.text"] == "" {
		t.Fatalf("db.query.text missing on count: %v", count.attrs)
	}
	if count.attrs["db.response.status_code"] != "" {
		t.Fatalf("status_code should be empty on success, got %v", count.attrs["db.response.status_code"])
	}
	if count.attrs["error.type"] != "" {
		t.Fatalf("error.type should be empty on success, got %v", count.attrs["error.type"])
	}
	if hist, ok := fm.lastHist(); !ok || hist.name != metricDuration {
		t.Fatalf("hist = %+v", hist)
	}

	sp, ok := ex.last()
	if !ok {
		t.Fatal("no span recorded")
	}
	if sp.Name() != tracerScope || sp.SpanKind() != trace.SpanKindClient {
		t.Fatalf("span name=%q kind=%v", sp.Name(), sp.SpanKind())
	}
	attrs := spanAttrs(sp)
	if attrs["db.system.name"] != "mysql" || attrs["db.operation.name"] != "INSERT" {
		t.Fatalf("span attrs = %v", attrs)
	}
	if attrs["db.namespace"] != "report_db" {
		t.Fatalf("span db.namespace = %v, want report_db", attrs["db.namespace"])
	}
	if attrs["db.query.text"] == "" {
		t.Fatal("db.query.text missing on span")
	}
	if attrs["db.response.status_code"] != "" {
		t.Fatalf("span status_code should be empty on success, got %v", attrs["db.response.status_code"])
	}
	if attrs["error.type"] != "" {
		t.Fatalf("span error.type should be empty on success, got %v", attrs["error.type"])
	}
	if sp.Status().Code != codes.Unset {
		t.Fatalf("status = %v, want unset", sp.Status().Code)
	}
}

func TestNamedExecContext_StructArg(t *testing.T) {
	dbx, _, _ := setupTest(t, &stubDB{})

	if _, err := dbx.NamedExecContext(context.Background(),
		"INSERT INTO t (id, name) VALUES (:id, :name)",
		report{ID: 3, Name: "c"}); err != nil {
		t.Fatalf("NamedExecContext: %v", err)
	}

	q := recordedQuery(t)
	if q.query != "INSERT INTO t (id, name) VALUES (?, ?)" {
		t.Fatalf("query = %q, want struct binding", q.query)
	}
	if len(q.args) != 2 || q.args[0] != int64(3) || q.args[1] != "c" {
		t.Fatalf("args = %v, want [3 c]", q.args)
	}
}

func TestNamedExecContext_MySQLError(t *testing.T) {
	dbx, fm, ex := setupTest(t, &stubDB{})
	stubExecErr(t, &mysql.MySQLError{Number: 1066, Message: "duplicate entry"})

	_, err := dbx.NamedExecContext(context.Background(),
		"UPDATE t SET name = :name WHERE id = :id",
		map[string]any{"id": int64(1), "name": "a"})
	if err == nil {
		t.Fatal("expected error")
	}

	count, ok := fm.lastCount()
	if !ok {
		t.Fatal("no count recorded")
	}
	if count.attrs["error.type"] != "duplicate entry" {
		t.Fatalf("error.type = %v, want the error message", count.attrs["error.type"])
	}
	if count.attrs["db.response.status_code"] != "1066" {
		t.Fatalf("status_code = %v, want 1066", count.attrs["db.response.status_code"])
	}

	sp, ok := ex.last()
	if !ok {
		t.Fatal("no span recorded")
	}
	if sp.Status().Code != codes.Error {
		t.Fatalf("status = %v, want error", sp.Status().Code)
	}
	attrs := spanAttrs(sp)
	if attrs["error.type"] != "duplicate entry" {
		t.Fatalf("span error.type = %v, want the error message", attrs["error.type"])
	}
	if attrs["db.response.status_code"] != "1066" {
		t.Fatalf("span status_code = %v, want 1066", attrs["db.response.status_code"])
	}
}

func TestBindFailure_EmitsNoTelemetry(t *testing.T) {
	dbx, fm, ex := setupTest(t, &stubDB{})

	_, err := dbx.NamedExecContext(context.Background(),
		"UPDATE t SET a = :missing WHERE id = :id", map[string]any{"id": int64(1)})
	if err == nil {
		t.Fatal("expected bind error for missing key")
	}

	if got := numRecordedQueries(t); got != 0 {
		t.Fatalf("query executed on bind failure: %d", got)
	}
	if n := ex.count(); n != 0 {
		t.Fatalf("expected no spans, got %d", n)
	}
	nc, nh := fm.nums()
	if nc != 0 || nh != 0 {
		t.Fatalf("expected no metrics, got counts=%d hists=%d", nc, nh)
	}
}

func TestNamedSelectContext_INExpansion(t *testing.T) {
	dbx, _, _ := setupTest(t, &stubDB{
		cols: []string{"id", "name"},
		rows: [][]driver.Value{{int64(1), "a"}, {int64(2), "b"}, {int64(3), "c"}},
	})

	var items []report
	if err := dbx.NamedSelectContext(context.Background(), &items,
		"SELECT id, name FROM t WHERE id IN (:ids)", map[string]any{"ids": []int64{1, 2, 3}}); err != nil {
		t.Fatalf("NamedSelectContext: %v", err)
	}
	if len(items) != 3 || items[0].ID != 1 || items[2].Name != "c" {
		t.Fatalf("items = %+v", items)
	}

	q := recordedQuery(t)
	if q.query != "SELECT id, name FROM t WHERE id IN (?, ?, ?)" {
		t.Fatalf("query = %q, want IN expansion", q.query)
	}
	if len(q.args) != 3 {
		t.Fatalf("args len = %d, want 3", len(q.args))
	}
}

func TestNamedSelectContext_NilArg(t *testing.T) {
	dbx, fm, ex := setupTest(t, &stubDB{
		cols: []string{"id", "name"},
		rows: [][]driver.Value{{int64(1), "a"}},
	})

	var items []report
	if err := dbx.NamedSelectContext(context.Background(), &items,
		"SELECT id, name FROM t", nil); err != nil {
		t.Fatalf("NamedSelectContext: %v", err)
	}
	if len(items) != 1 || items[0].ID != 1 {
		t.Fatalf("items = %+v", items)
	}

	q := recordedQuery(t)
	if q.query != "SELECT id, name FROM t" || len(q.args) != 0 {
		t.Fatalf("query = %q args = %v", q.query, q.args)
	}

	if n := ex.count(); n != 1 {
		t.Fatalf("spans = %d, want 1", n)
	}
	if _, ok := fm.lastCount(); !ok {
		t.Fatal("no count metric recorded")
	}
}

func TestNamedGetContext(t *testing.T) {
	dbx, _, _ := setupTest(t, &stubDB{
		cols: []string{"id", "name"},
		rows: [][]driver.Value{{int64(7), "seven"}},
	})

	var row report
	if err := dbx.NamedGetContext(context.Background(), &row,
		"SELECT id, name FROM t WHERE id = :id", map[string]any{"id": int64(7)}); err != nil {
		t.Fatalf("NamedGetContext: %v", err)
	}
	if row.ID != 7 || row.Name != "seven" {
		t.Fatalf("row = %+v", row)
	}
}

func TestBeginTx_Commit(t *testing.T) {
	dbx, fm, _ := setupTest(t, &stubDB{})

	tx, err := dbx.Begin(context.Background())
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}

	res, err := tx.NamedExecContext(context.Background(),
		"INSERT INTO t (id, name) VALUES (:id, :name)",
		map[string]any{"id": int64(2), "name": "b"})
	if err != nil {
		t.Fatalf("tx exec: %v", err)
	}
	if id, _ := res.LastInsertId(); id != 42 {
		t.Fatalf("LastInsertId = %d, want 42", id)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	if got := numRecordedQueries(t); got != 1 {
		t.Fatalf("executed %d queries, want 1", got)
	}
	if count, ok := fm.lastCount(); !ok || count.attrs["db.operation.name"] != "INSERT" {
		t.Fatalf("count = %+v", count)
	}
}

func TestBeginTx_Rollback(t *testing.T) {
	dbx, fm, _ := setupTest(t, &stubDB{})

	tx, err := dbx.Begin(context.Background())
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}

	if _, err := tx.NamedExecContext(context.Background(),
		"DELETE FROM t WHERE id = :id", map[string]any{"id": int64(1)}); err != nil {
		t.Fatalf("tx exec: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	if got := numRecordedQueries(t); got != 1 {
		t.Fatalf("executed %d queries, want 1", got)
	}
	if count, ok := fm.lastCount(); !ok || count.attrs["db.operation.name"] != "DELETE" {
		t.Fatalf("count = %+v", count)
	}
}

func TestOperationName(t *testing.T) {
	cases := []struct {
		op, query, want string
	}{
		{"select", "SELECT id FROM t", "SELECT"},
		{"select", "  insert into t (a) values (:a)", "INSERT"},
		{"exec", ":named", "exec"},
		{"get", "", "get"},
	}
	for _, c := range cases {
		if got := operationName(c.op, c.query); got != c.want {
			t.Errorf("operationName(%q, %q) = %q, want %q", c.op, c.query, got, c.want)
		}
	}
}

type report struct {
	ID   int64  `db:"id"`
	Name string `db:"name"`
}

// --- PostgreSQL execution paths -------------------------------------------

func TestPostgresNamedBind(t *testing.T) {
	dbx, _, _ := setupTestWithDriver(t, &stubDB{
		cols: []string{"id", "name"},
		rows: [][]driver.Value{{int64(1), "a"}},
	}, postgresDriver, "postgresql", "report_db")

	var items []report
	if err := dbx.NamedSelectContext(context.Background(), &items,
		"SELECT id, name FROM t WHERE id = :id", map[string]any{"id": int64(1)}); err != nil {
		t.Fatalf("NamedSelectContext: %v", err)
	}
	if len(items) != 1 || items[0].ID != 1 {
		t.Fatalf("items = %+v", items)
	}

	q := recordedQuery(t)
	if q.query != "SELECT id, name FROM t WHERE id = $1" {
		t.Fatalf("query = %q, want $1 placeholder", q.query)
	}
	if len(q.args) != 1 || q.args[0] != int64(1) {
		t.Fatalf("args = %v, want [1]", q.args)
	}
}

func TestPostgresINExpansion(t *testing.T) {
	dbx, _, _ := setupTestWithDriver(t, &stubDB{
		cols: []string{"id", "name"},
		rows: [][]driver.Value{{int64(1), "a"}, {int64(2), "b"}, {int64(3), "c"}},
	}, postgresDriver, "postgresql", "report_db")

	var items []report
	if err := dbx.NamedSelectContext(context.Background(), &items,
		"SELECT id, name FROM t WHERE id IN (:ids)", map[string]any{"ids": []int64{1, 2, 3}}); err != nil {
		t.Fatalf("NamedSelectContext: %v", err)
	}
	if len(items) != 3 {
		t.Fatalf("items = %+v", items)
	}

	q := recordedQuery(t)
	if q.query != "SELECT id, name FROM t WHERE id IN ($1, $2, $3)" {
		t.Fatalf("query = %q, want numbered IN placeholders", q.query)
	}
	if len(q.args) != 3 || q.args[0] != int64(1) || q.args[2] != int64(3) {
		t.Fatalf("args = %v, want [1 2 3]", q.args)
	}
}

func TestPostgresLiteralQuestionMarks(t *testing.T) {
	dbx, _, _ := setupTestWithDriver(t, &stubDB{}, postgresDriver, "postgresql", "report_db")

	_, err := dbx.NamedExecContext(context.Background(),
		"UPDATE t SET note = :note, body = $$?$$ WHERE note = 'why?'", map[string]any{"note": "a"})
	if err != nil {
		t.Fatalf("NamedExecContext: %v", err)
	}

	q := recordedQuery(t)
	want := "UPDATE t SET note = $1, body = $$?$$ WHERE note = 'why?'"
	if q.query != want {
		t.Fatalf("query = %q, want %q", q.query, want)
	}
}

func TestPostgresCommentsAndEscapeStrings(t *testing.T) {
	dbx, _, _ := setupTestWithDriver(t, &stubDB{}, postgresDriver, "postgresql", "report_db")

	query := `UPDATE t SET note = :note WHERE body = E'it\'s ?' -- really?` + "\n"
	if _, err := dbx.NamedExecContext(context.Background(), query, map[string]any{"note": "a"}); err != nil {
		t.Fatalf("NamedExecContext: %v", err)
	}

	q := recordedQuery(t)
	want := `UPDATE t SET note = $1 WHERE body = E'it\'s ?' -- really?` + "\n"
	if q.query != want {
		t.Fatalf("query = %q, want %q", q.query, want)
	}
}

func TestPostgresCasts(t *testing.T) {
	dbx, _, _ := setupTestWithDriver(t, &stubDB{}, postgresDriver, "postgresql", "report_db")

	if _, err := dbx.NamedExecContext(context.Background(),
		"UPDATE t SET ts = :ts::timestamptz, id = (:id)::bigint WHERE id = :id",
		map[string]any{"ts": "2024-01-01", "id": int64(7)}); err != nil {
		t.Fatalf("NamedExecContext: %v", err)
	}

	q := recordedQuery(t)
	want := "UPDATE t SET ts = $1::timestamptz, id = ($2)::bigint WHERE id = $3"
	if q.query != want {
		t.Fatalf("query = %q, want %q", q.query, want)
	}
	if len(q.args) != 3 {
		t.Fatalf("args = %v, want 3 args", q.args)
	}
}

func TestPostgresErrorTelemetry(t *testing.T) {
	dbx, fm, ex := setupTestWithDriver(t, &stubDB{}, postgresDriver, "postgresql", "report_db")
	stubExecErr(t, &pgconn.PgError{
		Code:    "23505",
		Message: "duplicate key value violates unique constraint",
	})

	_, err := dbx.NamedExecContext(context.Background(),
		"INSERT INTO t (id) VALUES (:id)", map[string]any{"id": int64(1)})
	if err == nil {
		t.Fatal("expected error")
	}

	count, ok := fm.lastCount()
	if !ok {
		t.Fatal("no count recorded")
	}
	if count.attrs["db.system.name"] != "postgresql" {
		t.Fatalf("db.system.name = %v, want postgresql", count.attrs["db.system.name"])
	}
	if count.attrs["db.response.status_code"] != "23505" {
		t.Fatalf("status_code = %v, want 23505", count.attrs["db.response.status_code"])
	}
	if count.attrs["error.type"] != "duplicate key value violates unique constraint" {
		t.Fatalf("error.type = %v", count.attrs["error.type"])
	}

	sp, ok := ex.last()
	if !ok {
		t.Fatal("no span recorded")
	}
	if sp.Status().Code != codes.Error {
		t.Fatalf("status = %v, want error", sp.Status().Code)
	}
	if got := spanAttrs(sp)["db.response.status_code"]; got != "23505" {
		t.Fatalf("span status_code = %v, want 23505", got)
	}
}

func TestPostgresTransaction(t *testing.T) {
	dbx, _, _ := setupTestWithDriver(t, &stubDB{}, postgresDriver, "postgresql", "report_db")

	tx, err := dbx.Begin(context.Background())
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if _, err := tx.NamedExecContext(context.Background(),
		"DELETE FROM t WHERE id = :id", map[string]any{"id": int64(1)}); err != nil {
		t.Fatalf("tx exec: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	q := recordedQuery(t)
	if q.query != "DELETE FROM t WHERE id = $1" {
		t.Fatalf("query = %q, want $1 placeholder", q.query)
	}
}

func TestMySQLINExpansionWithLiteralQuestionMark(t *testing.T) {
	dbx, _, _ := setupTest(t, &stubDB{
		cols: []string{"id", "name"},
		rows: [][]driver.Value{{int64(1), "a"}, {int64(2), "b"}},
	})

	var items []report
	if err := dbx.NamedSelectContext(context.Background(), &items,
		"SELECT id, name FROM t WHERE note = 'why?' AND id IN (:ids)",
		map[string]any{"ids": []int64{1, 2}}); err != nil {
		t.Fatalf("NamedSelectContext: %v", err)
	}

	q := recordedQuery(t)
	want := "SELECT id, name FROM t WHERE note = 'why?' AND id IN (?, ?)"
	if q.query != want {
		t.Fatalf("query = %q, want %q", q.query, want)
	}
	if len(q.args) != 2 {
		t.Fatalf("args = %v, want 2", q.args)
	}
}

func TestINExpansionErrors(t *testing.T) {
	dbx, fm, ex := setupTest(t, &stubDB{})

	_, err := dbx.NamedExecContext(context.Background(),
		"DELETE FROM t WHERE id IN (:ids)", map[string]any{"ids": []int64{}})
	if err == nil || !strings.Contains(err.Error(), "empty slice") {
		t.Fatalf("error = %v, want empty slice error", err)
	}
	if got := numRecordedQueries(t); got != 0 {
		t.Fatalf("executed %d queries on bind failure, want 0", got)
	}
	if n := ex.count(); n != 0 {
		t.Fatalf("spans = %d, want 0", n)
	}
	if nc, nh := fm.nums(); nc != 0 || nh != 0 {
		t.Fatalf("metrics = %d/%d, want 0/0", nc, nh)
	}
}
