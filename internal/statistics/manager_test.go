package statistics

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/karllee830/Xboard-Node/internal/config"
)

func TestManagerUploadsGzipBatchWithAuthAndChecksum(t *testing.T) {
	var mu sync.Mutex
	var received reportEnvelope
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != reportPath {
			t.Errorf("path=%q", request.URL.Path)
		}
		if request.Header.Get("Content-Encoding") != "gzip" {
			t.Errorf("missing gzip encoding")
		}
		if request.Header.Get("X-Xboard-Token") != "secret" || request.Header.Get("X-Xboard-Node-ID") != "9" || request.Header.Get("X-Xboard-Machine-ID") != "3" {
			t.Errorf("invalid auth headers")
		}
		if request.Header.Get("X-Statistics-Pending-Batches") != "1" || request.Header.Get("X-Statistics-Retry-Attempt") != "0" {
			t.Errorf("invalid health headers")
		}
		reader, err := gzip.NewReader(request.Body)
		if err != nil {
			t.Errorf("gzip reader: %v", err)
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		plain, _ := io.ReadAll(reader)
		sum := sha256.Sum256(plain)
		if request.Header.Get("X-Statistics-Checksum") != hex.EncodeToString(sum[:]) {
			t.Errorf("checksum mismatch")
		}
		mu.Lock()
		requests++
		_ = json.Unmarshal(plain, &received)
		mu.Unlock()
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"status":"success","data":{"result":"accepted"}}`))
	}))
	defer server.Close()

	collector := NewCollector(100)
	start := time.Now().UTC().Add(-2 * time.Hour).Truncate(time.Hour)
	connection := collector.Open(testDimensions(), start.Add(time.Minute))
	connection.AddUpload(12)
	connection.AddDownload(34)
	connection.Close(start.Add(2 * time.Minute))

	manager := NewManager(collector, config.PanelConfig{
		URL: server.URL, Token: "secret", NodeID: 9, MachineID: 3,
	}, config.StatisticsConfig{
		MaxHourlyDimensions: 100,
		MaxPendingBatches:   10,
		RequestTimeout:      2,
		SpoolPath:           t.TempDir(),
	}, t.TempDir())
	if err := manager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.Close()

	mu.Lock()
	defer mu.Unlock()
	if requests != 1 {
		t.Fatalf("requests=%d, want 1", requests)
	}
	if received.SchemaVersion != 1 || len(received.Records) != 1 {
		t.Fatalf("envelope=%+v", received)
	}
	if received.Records[0].UploadBytes != 12 || received.Records[0].DownloadBytes != 34 {
		t.Fatalf("record=%+v", received.Records[0])
	}
}

func TestManagerPersistsAndRestoresCurrentHour(t *testing.T) {
	spool := t.TempDir()
	now := time.Now().UTC().Truncate(time.Hour).Add(30 * time.Minute)
	collector := NewCollector(100)
	connection := collector.Open(testDimensions(), now.Add(-time.Minute))
	connection.AddUpload(77)
	connection.Close(now)

	manager := NewManager(collector, config.PanelConfig{URL: "http://127.0.0.1:1", NodeID: 1}, config.StatisticsConfig{
		MaxPendingBatches: 10,
		RequestTimeout:    1,
		SpoolPath:         spool,
	}, spool)
	if err := manager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.Close()

	restored := NewCollector(100)
	restoredManager := NewManager(restored, config.PanelConfig{URL: "http://127.0.0.1:1", NodeID: 1}, config.StatisticsConfig{
		MaxPendingBatches: 10,
		RequestTimeout:    1,
		SpoolPath:         spool,
	}, spool)
	if err := restoredManager.loadCurrent(); err != nil {
		t.Fatal(err)
	}
	hours := restored.CurrentHours(now)
	if len(hours) != 1 || len(hours[0].Records) != 1 || hours[0].Records[0].UploadBytes != 77 {
		t.Fatalf("restored hours=%+v", hours)
	}
	if _, err := filepath.Glob(filepath.Join(spool, "current.json")); err != nil {
		t.Fatal(err)
	}
}

func TestDeterministicBatchIDIsStableUUIDV5(t *testing.T) {
	start := time.Date(2026, 7, 28, 10, 0, 0, 0, time.UTC)
	first := deterministicBatchID(7, start)
	second := deterministicBatchID(7, start)
	if first != second || len(first) != 36 || first[14] != '5' {
		t.Fatalf("batch ids %q %q", first, second)
	}
}

func TestManagerBacksOffAfterTransientFailure(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests++
		if requests == 1 {
			writer.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		writer.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	spool := t.TempDir()
	collector := NewCollector(10)
	manager := NewManager(collector, config.PanelConfig{URL: server.URL, NodeID: 5}, config.StatisticsConfig{
		MaxPendingBatches: 10,
		RequestTimeout:    2,
		SpoolPath:         spool,
	}, spool)
	if err := os.MkdirAll(manager.pendingPath(), 0o700); err != nil {
		t.Fatal(err)
	}
	start := time.Now().UTC().Add(-2 * time.Hour).Truncate(time.Hour)
	if err := manager.enqueue(Hour{Start: start, Records: []Record{{
		UserID: 1, SourceIP: UnknownDimension, DestinationIP: UnknownDimension,
		ExactDomain: UnknownDimension, RegistrableDomain: UnknownDimension,
		Network: "tcp", ApplicationProtocol: UnknownDimension, InboundType: "vless",
		OutboundTag: "direct", OutboundType: "direct", DownloadBytes: 1,
	}}}); err != nil {
		t.Fatal(err)
	}

	now := time.Now()
	if err := manager.uploadPending(context.Background(), now); err == nil {
		t.Fatal("expected transient upload error")
	}
	if requests != 1 {
		t.Fatalf("requests=%d, want 1", requests)
	}
	if err := manager.uploadPending(context.Background(), now.Add(10*time.Second)); err != nil {
		t.Fatal(err)
	}
	if requests != 1 {
		t.Fatalf("backoff made %d requests, want 1", requests)
	}
	if err := manager.uploadPending(context.Background(), now.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if requests != 2 {
		t.Fatalf("requests=%d, want 2", requests)
	}
}

func TestRetryBackoffIsBoundedAndJittered(t *testing.T) {
	for attempt := 1; attempt <= 20; attempt++ {
		delay := retryBackoff(attempt)
		if delay < 30*time.Second || delay > 37*time.Minute+30*time.Second {
			t.Fatalf("attempt %d delay %s outside bounds", attempt, delay)
		}
	}
}

func TestReportStatusClassificationKeepsMissingPluginBatchPending(t *testing.T) {
	if isPermanentReportStatus(http.StatusNotFound) {
		t.Fatal("404 should remain pending until the panel plugin is installed")
	}
	if isPermanentReportStatus(http.StatusTooManyRequests) {
		t.Fatal("429 should be retried")
	}
	if !isPermanentReportStatus(http.StatusUnprocessableEntity) {
		t.Fatal("invalid protocol payload should be rejected permanently")
	}
}
