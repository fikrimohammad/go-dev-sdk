package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jmoiron/sqlx"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/fikrimohammad/go-servicekit/observability/attributes"
	"github.com/fikrimohammad/go-servicekit/observability/metrics"
	"github.com/fikrimohammad/go-servicekit/observability/tracer"
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

func (f *fakeMetrics) Add(_ context.Context, instrument metrics.Instrument, value int64, attrs map[string]any) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.counts = append(f.counts, metricRec{instrument.Name, float64(value), normalizeAttrs(attrs)})
	return nil
}

func (f *fakeMetrics) Record(_ context.Context, instrument metrics.Instrument, value float64, attrs map[string]any) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hists = append(f.hists, metricRec{instrument.Name, value, normalizeAttrs(attrs)})
	return nil
}

func (f *fakeMetrics) AddUpDown(context.Context, metrics.Instrument, int64, map[string]any) error {
	return nil
}

func (f *fakeMetrics) RecordGauge(context.Context, metrics.Instrument, float64, map[string]any) error {
	return nil
}

func (f *fakeMetrics) Provider() metric.MeterProvider { return nil }

func (f *fakeMetrics) Stop(context.Context) error { return nil }

func (f *fakeMetrics) lastHist() (metricRec, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.hists) == 0 {
		return metricRec{}, false
	}
	return f.hists[len(f.hists)-1], true
}

func (f *fakeMetrics) nums() (counts, hists int) {
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

// newCapture returns a tracer provider that exports every span to the returned
// recordingExporter.
func newCapture(t *testing.T) (*recordingExporter, *sdktrace.TracerProvider) {
	t.Helper()
	ex := &recordingExporter{}
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(ex)))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	return ex, tp
}

// --- sqlmock setup -----------------------------------------------------------------

// mockSeq numbers the sqlmock databases so every test gets a DSN no other test
// registered, even under -count=N.
var mockSeq atomic.Int64

// mockDB is a registered sqlmock database: its DSN, the *sql.DB, and the
// expectations embedded so tests call mock.ExpectQuery and friends directly.
type mockDB struct {
	dsn string
	db  *sql.DB
	sqlmock.Sqlmock
}

// newMock registers a uniquely named sqlmock database with exact query matching
// and monitored pings.
func newMock(t *testing.T) mockDB {
	t.Helper()
	dsn := fmt.Sprintf("sqlmock_db_%d", mockSeq.Add(1))
	sqlDB, mock, err := sqlmock.NewWithDSN(dsn,
		sqlmock.MonitorPingsOption(true),
		sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatalf("sqlmock.NewWithDSN: %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	return mockDB{dsn: dsn, db: sqlDB, Sqlmock: mock}
}

// connectMock calls Connect with the sqlmock driver, expecting the initial ping.
// The config sets the discrete connection fields so the telemetry attributes
// (db.namespace, server.address, server.port) are known. Its system name is
// "sqlmock" because the driver is not a built-in one.
func connectMock(t *testing.T, opts ...Option) (DB, mockDB) {
	t.Helper()
	m := newMock(t)
	m.ExpectPing()
	db, err := Connect(context.Background(), Config{
		Driver:   "sqlmock",
		DSN:      m.dsn,
		Host:     "db.example.com",
		Port:     3306,
		Database: "report_db",
		Username: "app",
	}, opts...)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db, m
}

// harness is a DB whose statements run on sqlmock, with its telemetry captured.
type harness struct {
	db   DB
	mock mockDB
	fm   *fakeMetrics
	ex   *recordingExporter
}

// setupTest connects through Connect with injected telemetry clients. The
// statements use the generic dialect ("?" placeholders, system name "sqlmock").
func setupTest(t *testing.T) harness {
	t.Helper()
	fm := &fakeMetrics{}
	ex, tp := newCapture(t)
	db, mock := connectMock(t, WithMetrics(fm), WithTracer(tracer.Wrap(tp)))
	return harness{db: db, mock: mock, fm: fm, ex: ex}
}

// setupDialect builds the DB directly around a sqlmock database so the bind
// rules and system name of spec apply. Connect picks a built-in spec only for a
// built-in driver name, and sqlmock is registered under "sqlmock", so the
// mysql and postgres dialects cannot be reached through Connect in a unit test.
// sqlmock records the bound text verbatim, so $1 and ? are both observable.
func setupDialect(t *testing.T, spec driverSpec, systemName string) harness {
	t.Helper()
	m := newMock(t)
	fm := &fakeMetrics{}
	ex, tp := newCapture(t)
	return harness{
		db: &dbConn{
			DB:     sqlx.NewDb(m.db, "sqlmock"),
			driver: spec,
			meta: meta{
				systemName: systemName,
				namespace:  "report_db",
				serverAddr: "db.example.com",
				serverPort: spec.port,
				metrics:    fm,
				tracer:     tracer.Wrap(tp),
			},
		},
		mock: m,
		fm:   fm,
		ex:   ex,
	}
}

// setOTelProviders installs tp and mp as the OTel global providers for the
// duration of the test. The previous globals are restored in t.Cleanup.
func setOTelProviders(t *testing.T, tp trace.TracerProvider, mp metric.MeterProvider) {
	t.Helper()
	prevTP, prevMP := otel.GetTracerProvider(), otel.GetMeterProvider()
	otel.SetTracerProvider(tp)
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		if otel.GetTracerProvider() != prevTP {
			otel.SetTracerProvider(prevTP)
		}
		if otel.GetMeterProvider() != prevMP {
			otel.SetMeterProvider(prevMP)
		}
	})
}

// collectedMetrics returns the names of the metrics r has collected so far.
func collectedMetrics(t *testing.T, r *sdkmetric.ManualReader) []string {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := r.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect metrics: %v", err)
	}
	var names []string
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			names = append(names, m.Name)
		}
	}
	return names
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
	m := newMock(t)
	m.ExpectPing()
	m.ExpectClose()

	db, err := Connect(context.Background(), Config{
		Driver:       "sqlmock",
		DSN:          m.dsn,
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
	if c.systemName != "sqlmock" {
		t.Fatalf("systemName = %q, want sqlmock", c.systemName)
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
	if err := m.ExpectationsWereMet(); err != nil {
		t.Fatalf("ExpectationsWereMet: %v", err)
	}
}

// TestConnect_ContextDeadline checks that an expired ctx bounds the initial ping:
// Connect fails with the context error instead of waiting on an unreachable host.
func TestConnect_ContextDeadline(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()

	start := time.Now()
	// 10.255.255.1 is non-routable, so only ctx can end the dial quickly; the
	// DSN's 2s timeout keeps a regression from hanging until the OS gives up.
	db, err := Connect(ctx, Config{
		Driver: "mysql",
		DSN:    "user:pass@tcp(10.255.255.1:3306)/report_db?timeout=2s",
	})
	if elapsed := time.Since(start); elapsed >= time.Second {
		t.Fatalf("Connect took %v with an expired ctx, want < 1s", elapsed)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Connect error = %v, want errors.Is(err, context.DeadlineExceeded)", err)
	}
	if db != nil {
		t.Fatalf("Connect returned DB %v with an error, want nil", db)
	}
}

func TestConnect_InjectedClients(t *testing.T) {
	// OTel global providers are the fallback defaults: fresh, must receive nothing.
	defEx, defTp := newCapture(t)
	defReader := sdkmetric.NewManualReader()
	defMp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(defReader))
	t.Cleanup(func() { _ = defMp.Shutdown(context.Background()) })
	setOTelProviders(t, defTp, defMp)

	// Injected clients: must receive the events.
	injEx, injTp := newCapture(t)
	injFm := &fakeMetrics{}

	db, mock := connectMock(t, WithMetrics(injFm), WithTracer(tracer.Wrap(injTp)))
	mock.ExpectQuery("SELECT id, name FROM t WHERE id = ?").
		WithArgs(int64(1)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(int64(1), "a"))

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
	if _, ok := injFm.lastHist(); !ok {
		t.Fatal("injected metrics client received no duration")
	}

	if n := defEx.count(); n != 0 {
		t.Fatalf("OTel global tracer spans = %d, want 0", n)
	}
	if names := collectedMetrics(t, defReader); len(names) != 0 {
		t.Fatalf("OTel global meter provider collected %v, want none", names)
	}
}

func TestInstrument_FallsBackToOTelGlobal(t *testing.T) {
	ex, tp := newCapture(t)
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	setOTelProviders(t, tp, mp)

	// No injected clients: the execution must fall back to the OTel globals.
	dbx, mock := connectMock(t)
	mock.ExpectExec("INSERT INTO t (id, name) VALUES (?, ?)").
		WithArgs(int64(1), "a").
		WillReturnResult(sqlmock.NewResult(1, 1))

	if _, err := dbx.NamedExecContext(context.Background(),
		"INSERT INTO t (id, name) VALUES (:id, :name)",
		map[string]any{"id": int64(1), "name": "a"}); err != nil {
		t.Fatalf("NamedExecContext: %v", err)
	}

	if n := ex.count(); n != 1 {
		t.Fatalf("OTel global tracer spans = %d, want 1", n)
	}
	sp, _ := ex.last()
	if sp.Name() != "INSERT" {
		t.Fatalf("span name = %q, want INSERT", sp.Name())
	}
	if names := collectedMetrics(t, reader); !slices.Equal(names, []string{metricDuration}) {
		t.Errorf("OTel global meter provider collected %v, want [%q]", names, metricDuration)
	}
}

func TestNamedExecContext_Success(t *testing.T) {
	h := setupDialect(t, mysqlDriver, "mysql")
	h.mock.ExpectExec("INSERT INTO t (id, name) VALUES (?, ?)").
		WithArgs(int64(1), "a").
		WillReturnResult(sqlmock.NewResult(42, 1))

	res, err := h.db.NamedExecContext(context.Background(),
		"INSERT INTO t (id, name) VALUES (:id, :name)",
		map[string]any{"id": int64(1), "name": "a"})
	if err != nil {
		t.Fatalf("NamedExecContext: %v", err)
	}
	if id, _ := res.LastInsertId(); id != 42 {
		t.Fatalf("LastInsertId = %d, want 42", id)
	}
	if err := h.mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("ExpectationsWereMet: %v", err)
	}

	hist, ok := h.fm.lastHist()
	if !ok {
		t.Fatal("no duration recorded")
	}
	if hist.name != metricDuration {
		t.Fatalf("hist = %+v", hist)
	}
	if hist.attrs["db.system.name"] != "mysql" || hist.attrs["db.operation.name"] != "INSERT" {
		t.Fatalf("hist attrs = %v", hist.attrs)
	}
	if hist.attrs["db.namespace"] != "report_db" {
		t.Fatalf("hist db.namespace = %v, want report_db", hist.attrs["db.namespace"])
	}
	if hist.attrs["server.address"] != "db.example.com" {
		t.Fatalf("hist server.address = %v, want db.example.com", hist.attrs["server.address"])
	}
	if hist.attrs["server.port"] != 3306 {
		t.Fatalf("hist server.port = %v, want 3306", hist.attrs["server.port"])
	}
	if _, ok := hist.attrs["db.query.text"]; ok {
		t.Fatalf("db.query.text must not be on the metric: %v", hist.attrs)
	}
	if _, ok := hist.attrs["db.response.status_code"]; ok {
		t.Fatalf("db.response.status_code must be absent on success: %v", hist.attrs)
	}
	if _, ok := hist.attrs["error.type"]; ok {
		t.Fatalf("error.type must be absent on success: %v", hist.attrs)
	}

	sp, ok := h.ex.last()
	if !ok {
		t.Fatal("no span recorded")
	}
	if sp.Name() != "INSERT" || sp.SpanKind() != trace.SpanKindClient {
		t.Fatalf("span name=%q kind=%v, want INSERT, client", sp.Name(), sp.SpanKind())
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
	if _, ok := attrs["db.response.status_code"]; ok {
		t.Fatalf("span db.response.status_code must be absent on success: %v", attrs)
	}
	if _, ok := attrs["error.type"]; ok {
		t.Fatalf("span error.type must be absent on success: %v", attrs)
	}
	if sp.Status().Code != codes.Unset {
		t.Fatalf("status = %v, want unset", sp.Status().Code)
	}
}

// TestInstrument_MetricAttributes checks the exact duration metric attribute set
// and the span for a successful and a failing statement, using a real manual
// reader so the collected metrics are what an exporter would see.
func TestInstrument_MetricAttributes(t *testing.T) {
	const bound = "INSERT INTO t (id, name) VALUES (?, ?)"
	base := map[string]string{
		"db.system.name":    "mysql",
		"db.namespace":      "report_db",
		"db.operation.name": "INSERT",
		"server.address":    "db.example.com",
		"server.port":       "3306",
	}
	withErr := func(code string) map[string]string {
		m := maps.Clone(base)
		m["error.type"] = code
		m["db.response.status_code"] = code
		return m
	}

	cases := []struct {
		name string
		err  error
		want map[string]string
	}{
		{name: "success", want: base},
		// sql.ErrNoRows is a normal "not found" result, like redis.Nil.
		{name: "no rows", err: sql.ErrNoRows, want: base},
		{name: "mysql error", err: &mysql.MySQLError{Number: 1062, Message: "duplicate entry"}, want: withErr("1062")},
		{name: "unknown error", err: errors.New("boom"), want: func() map[string]string {
			m := maps.Clone(base)
			m["error.type"] = "_OTHER"
			return m
		}()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := setupDialect(t, mysqlDriver, "mysql")
			c := h.db.(*dbConn)
			reader := sdkmetric.NewManualReader()
			mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
			t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
			c.meta.metrics = metrics.Wrap(mp)

			e := h.mock.ExpectExec(bound).WithArgs(int64(1), "a")
			if tc.err != nil {
				e.WillReturnError(tc.err)
			} else {
				e.WillReturnResult(sqlmock.NewResult(1, 1))
			}
			_, _ = h.db.NamedExecContext(context.Background(),
				"INSERT INTO t (id, name) VALUES (:id, :name)",
				map[string]any{"id": int64(1), "name": "a"})

			var rm metricdata.ResourceMetrics
			if err := reader.Collect(context.Background(), &rm); err != nil {
				t.Fatalf("collect: %v", err)
			}
			var names []string
			var points []map[string]string
			for _, sm := range rm.ScopeMetrics {
				for _, m := range sm.Metrics {
					names = append(names, m.Name)
					if m.Name != metricDuration {
						continue
					}
					hist, ok := m.Data.(metricdata.Histogram[float64])
					if !ok || m.Unit != "s" {
						t.Fatalf("%s: data %T unit %q, want histogram in s", m.Name, m.Data, m.Unit)
					}
					for _, dp := range hist.DataPoints {
						points = append(points, attrSetMap(dp.Attributes))
					}
				}
			}
			if !slices.Equal(names, []string{metricDuration}) {
				t.Fatalf("metrics = %v, want only %q", names, metricDuration)
			}
			if len(points) != 1 {
				t.Fatalf("duration data points = %d, want 1", len(points))
			}
			if !maps.Equal(points[0], tc.want) {
				t.Fatalf("metric attributes = %v, want %v", points[0], tc.want)
			}

			sp, ok := h.ex.last()
			if !ok {
				t.Fatal("no span recorded")
			}
			if sp.Name() != "INSERT" || sp.SpanKind() != trace.SpanKindClient {
				t.Fatalf("span name = %q, kind = %v, want INSERT, client", sp.Name(), sp.SpanKind())
			}
			_, failed := tc.want["error.type"]
			if gotErr := sp.Status().Code == codes.Error; gotErr != failed {
				t.Fatalf("span status = %v, want error status %t", sp.Status().Code, failed)
			}
			if got := spanAttrs(sp)["db.query.text"]; got != bound {
				t.Fatalf("span db.query.text = %v, want bound query", got)
			}
		})
	}
}

// attrSetMap converts an OTel attribute set to string values for comparison.
func attrSetMap(set attribute.Set) map[string]string {
	out := make(map[string]string)
	for _, kv := range set.ToSlice() {
		out[string(kv.Key)] = kv.Value.String()
	}
	return out
}

func TestNamedExecContext_StructArg(t *testing.T) {
	h := setupTest(t)
	h.mock.ExpectExec("INSERT INTO t (id, name) VALUES (?, ?)").
		WithArgs(int64(3), "c").
		WillReturnResult(sqlmock.NewResult(1, 1))

	if _, err := h.db.NamedExecContext(context.Background(),
		"INSERT INTO t (id, name) VALUES (:id, :name)",
		report{ID: 3, Name: "c"}); err != nil {
		t.Fatalf("NamedExecContext: %v", err)
	}
	if err := h.mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("ExpectationsWereMet: %v", err)
	}
}

func TestNamedExecContext_MySQLError(t *testing.T) {
	h := setupTest(t)
	h.mock.ExpectExec("UPDATE t SET name = ? WHERE id = ?").
		WithArgs("a", int64(1)).
		WillReturnError(&mysql.MySQLError{Number: 1066, Message: "duplicate entry"})

	_, err := h.db.NamedExecContext(context.Background(),
		"UPDATE t SET name = :name WHERE id = :id",
		map[string]any{"id": int64(1), "name": "a"})
	if err == nil {
		t.Fatal("NamedExecContext() err = nil, want error")
	}

	hist, ok := h.fm.lastHist()
	if !ok {
		t.Fatal("no duration recorded")
	}
	if hist.attrs["error.type"] != "1066" {
		t.Fatalf("error.type = %v, want 1066", hist.attrs["error.type"])
	}
	if hist.attrs["db.response.status_code"] != "1066" {
		t.Fatalf("status_code = %v, want 1066", hist.attrs["db.response.status_code"])
	}

	sp, ok := h.ex.last()
	if !ok {
		t.Fatal("no span recorded")
	}
	if sp.Status().Code != codes.Error {
		t.Fatalf("status = %v, want error", sp.Status().Code)
	}
	attrs := spanAttrs(sp)
	if attrs["error.type"] != "1066" {
		t.Fatalf("span error.type = %v, want 1066", attrs["error.type"])
	}
	if attrs["db.response.status_code"] != "1066" {
		t.Fatalf("span status_code = %v, want 1066", attrs["db.response.status_code"])
	}
}

func TestBindFailure_EmitsNoTelemetry(t *testing.T) {
	// No expectation is set: a statement that reached sqlmock would fail and
	// emit telemetry, so the zero counts below prove binding stopped first.
	h := setupTest(t)

	_, err := h.db.NamedExecContext(context.Background(),
		"UPDATE t SET a = :missing WHERE id = :id", map[string]any{"id": int64(1)})
	if err == nil {
		t.Fatal("NamedExecContext() err = nil, want bind error for missing key")
	}

	if n := h.ex.count(); n != 0 {
		t.Fatalf("spans = %d, want 0", n)
	}
	nc, nh := h.fm.nums()
	if nc != 0 || nh != 0 {
		t.Fatalf("counts = %d, hists = %d, want 0, 0", nc, nh)
	}
}

func TestNamedSelectContext_INExpansion(t *testing.T) {
	h := setupTest(t)
	h.mock.ExpectQuery("SELECT id, name FROM t WHERE id IN (?, ?, ?)").
		WithArgs(int64(1), int64(2), int64(3)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).
			AddRow(int64(1), "a").AddRow(int64(2), "b").AddRow(int64(3), "c"))

	var items []report
	if err := h.db.NamedSelectContext(context.Background(), &items,
		"SELECT id, name FROM t WHERE id IN (:ids)", map[string]any{"ids": []int64{1, 2, 3}}); err != nil {
		t.Fatalf("NamedSelectContext: %v", err)
	}
	if len(items) != 3 || items[0].ID != 1 || items[2].Name != "c" {
		t.Fatalf("items = %+v", items)
	}
	if err := h.mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("ExpectationsWereMet: %v", err)
	}
}

func TestNamedSelectContext_NilArg(t *testing.T) {
	h := setupTest(t)
	h.mock.ExpectQuery("SELECT id, name FROM t").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(int64(1), "a"))

	var items []report
	if err := h.db.NamedSelectContext(context.Background(), &items,
		"SELECT id, name FROM t", nil); err != nil {
		t.Fatalf("NamedSelectContext: %v", err)
	}
	if len(items) != 1 || items[0].ID != 1 {
		t.Fatalf("items = %+v", items)
	}

	if n := h.ex.count(); n != 1 {
		t.Fatalf("spans = %d, want 1", n)
	}
	if _, ok := h.fm.lastHist(); !ok {
		t.Fatal("no duration recorded")
	}
}

func TestNamedGetContext(t *testing.T) {
	h := setupTest(t)
	h.mock.ExpectQuery("SELECT id, name FROM t WHERE id = ?").
		WithArgs(int64(7)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(int64(7), "seven"))

	var row report
	if err := h.db.NamedGetContext(context.Background(), &row,
		"SELECT id, name FROM t WHERE id = :id", map[string]any{"id": int64(7)}); err != nil {
		t.Fatalf("NamedGetContext: %v", err)
	}
	if row.ID != 7 || row.Name != "seven" {
		t.Fatalf("row = %+v", row)
	}
}

func TestNamedGetContext_NoRows(t *testing.T) {
	h := setupTest(t)
	h.mock.ExpectQuery("SELECT id, name FROM t WHERE id = ?").
		WithArgs(int64(9)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name"}))

	var row report
	err := h.db.NamedGetContext(context.Background(), &row,
		"SELECT id, name FROM t WHERE id = :id", map[string]any{"id": int64(9)})
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("NamedGetContext error = %v, want sql.ErrNoRows", err)
	}
	if sp, ok := h.ex.last(); !ok || sp.Status().Code != codes.Unset {
		t.Fatalf("no rows must not set error status: span = %v", sp)
	}
}

func TestBeginTx_Commit(t *testing.T) {
	h := setupTest(t)
	h.mock.ExpectBegin()
	h.mock.ExpectExec("INSERT INTO t (id, name) VALUES (?, ?)").
		WithArgs(int64(2), "b").
		WillReturnResult(sqlmock.NewResult(42, 1))
	h.mock.ExpectCommit()

	tx, err := h.db.Begin(context.Background())
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
	if err := h.mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("ExpectationsWereMet: %v", err)
	}
	if hist, ok := h.fm.lastHist(); !ok || hist.attrs["db.operation.name"] != "INSERT" {
		t.Fatalf("hist = %+v", hist)
	}
}

func TestBeginTx_Rollback(t *testing.T) {
	h := setupTest(t)
	h.mock.ExpectBegin()
	h.mock.ExpectExec("DELETE FROM t WHERE id = ?").
		WithArgs(int64(1)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	h.mock.ExpectRollback()

	tx, err := h.db.Begin(context.Background())
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
	if err := h.mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("ExpectationsWereMet: %v", err)
	}
	if hist, ok := h.fm.lastHist(); !ok || hist.attrs["db.operation.name"] != "DELETE" {
		t.Fatalf("hist = %+v", hist)
	}
}

func TestOperationName(t *testing.T) {
	cases := []struct {
		op, query, want string
	}{
		{"select", "SELECT id FROM t", "SELECT"},
		{"select", "  insert into t (a) values (:a)", "INSERT"},
		// sqlcommenter-style and line comments, and a parenthesised query.
		{"get", "/* app=report */ SELECT id FROM t", "SELECT"},
		{"exec", "-- purge\n  DELETE FROM t", "DELETE"},
		{"select", "(SELECT 1) UNION (SELECT 2)", "SELECT"},
		// No recognised verb: the method name, upper-cased like the verbs.
		{"exec", ":named", "EXEC"},
		{"get", "", "GET"},
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
//
// These run on sqlmock with the postgres spec, so the $n placeholders and the
// postgresql system name are the ones Connect would produce for pgx. Only the
// real pgx driver is absent; the integration tests cover it.

func TestPostgresNamedBind(t *testing.T) {
	h := setupDialect(t, postgresDriver, "postgresql")
	h.mock.ExpectQuery("SELECT id, name FROM t WHERE id = $1").
		WithArgs(int64(1)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(int64(1), "a"))

	var items []report
	if err := h.db.NamedSelectContext(context.Background(), &items,
		"SELECT id, name FROM t WHERE id = :id", map[string]any{"id": int64(1)}); err != nil {
		t.Fatalf("NamedSelectContext: %v", err)
	}
	if len(items) != 1 || items[0].ID != 1 {
		t.Fatalf("items = %+v", items)
	}
}

func TestPostgresINExpansion(t *testing.T) {
	h := setupDialect(t, postgresDriver, "postgresql")
	h.mock.ExpectQuery("SELECT id, name FROM t WHERE id IN ($1, $2, $3)").
		WithArgs(int64(1), int64(2), int64(3)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).
			AddRow(int64(1), "a").AddRow(int64(2), "b").AddRow(int64(3), "c"))

	var items []report
	if err := h.db.NamedSelectContext(context.Background(), &items,
		"SELECT id, name FROM t WHERE id IN (:ids)", map[string]any{"ids": []int64{1, 2, 3}}); err != nil {
		t.Fatalf("NamedSelectContext: %v", err)
	}
	if len(items) != 3 {
		t.Fatalf("items = %+v", items)
	}
}

func TestPostgresLiteralQuestionMarks(t *testing.T) {
	h := setupDialect(t, postgresDriver, "postgresql")
	h.mock.ExpectExec("UPDATE t SET note = $1, body = $$?$$ WHERE note = 'why?'").
		WithArgs("a").
		WillReturnResult(sqlmock.NewResult(0, 1))

	if _, err := h.db.NamedExecContext(context.Background(),
		"UPDATE t SET note = :note, body = $$?$$ WHERE note = 'why?'", map[string]any{"note": "a"}); err != nil {
		t.Fatalf("NamedExecContext: %v", err)
	}
}

func TestPostgresCommentsAndEscapeStrings(t *testing.T) {
	h := setupDialect(t, postgresDriver, "postgresql")
	bound := `UPDATE t SET note = $1 WHERE body = E'it\'s ?' -- really?` + "\n"
	h.mock.ExpectExec(bound).WithArgs("a").WillReturnResult(sqlmock.NewResult(0, 1))

	query := `UPDATE t SET note = :note WHERE body = E'it\'s ?' -- really?` + "\n"
	if _, err := h.db.NamedExecContext(context.Background(), query, map[string]any{"note": "a"}); err != nil {
		t.Fatalf("NamedExecContext: %v", err)
	}
}

func TestPostgresCasts(t *testing.T) {
	h := setupDialect(t, postgresDriver, "postgresql")
	h.mock.ExpectExec("UPDATE t SET ts = $1::timestamptz, id = ($2)::bigint WHERE id = $3").
		WithArgs("2024-01-01", int64(7), int64(7)).
		WillReturnResult(sqlmock.NewResult(0, 1))

	if _, err := h.db.NamedExecContext(context.Background(),
		"UPDATE t SET ts = :ts::timestamptz, id = (:id)::bigint WHERE id = :id",
		map[string]any{"ts": "2024-01-01", "id": int64(7)}); err != nil {
		t.Fatalf("NamedExecContext: %v", err)
	}
}

func TestPostgresErrorTelemetry(t *testing.T) {
	h := setupDialect(t, postgresDriver, "postgresql")
	h.mock.ExpectExec("INSERT INTO t (id) VALUES ($1)").
		WithArgs(int64(1)).
		WillReturnError(&pgconn.PgError{
			Code:    "23505",
			Message: "duplicate key value violates unique constraint",
		})

	_, err := h.db.NamedExecContext(context.Background(),
		"INSERT INTO t (id) VALUES (:id)", map[string]any{"id": int64(1)})
	if err == nil {
		t.Fatal("NamedExecContext() err = nil, want error")
	}

	hist, ok := h.fm.lastHist()
	if !ok {
		t.Fatal("no duration recorded")
	}
	if hist.attrs["db.system.name"] != "postgresql" {
		t.Fatalf("db.system.name = %v, want postgresql", hist.attrs["db.system.name"])
	}
	if hist.attrs["db.response.status_code"] != "23505" {
		t.Fatalf("status_code = %v, want 23505", hist.attrs["db.response.status_code"])
	}
	if hist.attrs["error.type"] != "23505" {
		t.Fatalf("error.type = %v, want 23505", hist.attrs["error.type"])
	}

	sp, ok := h.ex.last()
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
	h := setupDialect(t, postgresDriver, "postgresql")
	h.mock.ExpectBegin()
	h.mock.ExpectExec("DELETE FROM t WHERE id = $1").
		WithArgs(int64(1)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	h.mock.ExpectCommit()

	tx, err := h.db.Begin(context.Background())
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
	if err := h.mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("ExpectationsWereMet: %v", err)
	}
}

func TestMySQLINExpansionWithLiteralQuestionMark(t *testing.T) {
	h := setupDialect(t, mysqlDriver, "mysql")
	h.mock.ExpectQuery("SELECT id, name FROM t WHERE note = 'why?' AND id IN (?, ?)").
		WithArgs(int64(1), int64(2)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(int64(1), "a").AddRow(int64(2), "b"))

	var items []report
	if err := h.db.NamedSelectContext(context.Background(), &items,
		"SELECT id, name FROM t WHERE note = 'why?' AND id IN (:ids)",
		map[string]any{"ids": []int64{1, 2}}); err != nil {
		t.Fatalf("NamedSelectContext: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("items = %+v", items)
	}
}

func TestINExpansionErrors(t *testing.T) {
	h := setupTest(t)

	_, err := h.db.NamedExecContext(context.Background(),
		"DELETE FROM t WHERE id IN (:ids)", map[string]any{"ids": []int64(nil)})
	if err == nil || !strings.Contains(err.Error(), "empty slice") {
		t.Fatalf("error = %v, want empty slice error", err)
	}
	if n := h.ex.count(); n != 0 {
		t.Fatalf("spans = %d, want 0", n)
	}
	if nc, nh := h.fm.nums(); nc != 0 || nh != 0 {
		t.Fatalf("metrics = %d/%d, want 0/0", nc, nh)
	}
}
