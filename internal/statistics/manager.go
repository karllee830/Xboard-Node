package statistics

import (
	"bytes"
	"compress/gzip"
	"context"
	cryptorand "crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cedar2025/xboard-node/internal/config"
	"github.com/cedar2025/xboard-node/internal/nlog"
)

const reportPath = "/api/v2/server/statistics/report"

type reportEnvelope struct {
	SchemaVersion    int      `json:"schema_version"`
	BatchID          string   `json:"batch_id"`
	BucketStart      string   `json:"bucket_start"`
	BucketEnd        string   `json:"bucket_end"`
	NodeVersion      string   `json:"node_version,omitempty"`
	CollectorVersion string   `json:"collector_version"`
	Records          []Record `json:"records"`
	Quality          Quality  `json:"quality"`
}

type persistedState struct {
	Version int    `json:"version"`
	Hours   []Hour `json:"hours"`
}

// Manager persists collector snapshots and uploads immutable gzip batches.
type Manager struct {
	collector *Collector
	panel     config.PanelConfig
	cfg       config.StatisticsConfig
	spoolPath string
	client    *http.Client

	mu     sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}

	retryAttempt int
	nextUploadAt time.Time
}

func NewManager(collector *Collector, panel config.PanelConfig, cfg config.StatisticsConfig, kernelConfigDir string) *Manager {
	spoolPath := strings.TrimSpace(cfg.SpoolPath)
	if spoolPath == "" {
		spoolPath = filepath.Join(kernelConfigDir, "statistics")
	}
	return &Manager{
		collector: collector,
		panel:     panel,
		cfg:       cfg,
		spoolPath: spoolPath,
		client: &http.Client{
			Timeout: time.Duration(cfg.RequestTimeout) * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        2,
				MaxIdleConnsPerHost: 2,
				IdleConnTimeout:     90 * time.Second,
			},
		},
		done: make(chan struct{}),
	}
}

func (m *Manager) Start(parent context.Context) error {
	if err := os.MkdirAll(m.pendingPath(), 0o700); err != nil {
		return fmt.Errorf("create statistics pending directory: %w", err)
	}
	if err := os.MkdirAll(m.rejectedPath(), 0o700); err != nil {
		return fmt.Errorf("create statistics rejected directory: %w", err)
	}
	if err := m.loadCurrent(); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(parent)
	m.cancel = cancel
	if err := m.checkpointAndUpload(ctx, time.Now()); err != nil {
		nlog.Core().Warn("statistics initial checkpoint failed", "error", err)
	}
	go m.loop(ctx)
	return nil
}

func (m *Manager) Close() {
	if m.cancel == nil {
		return
	}
	m.cancel()
	<-m.done
}

func (m *Manager) loop(ctx context.Context) {
	defer close(m.done)
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case now := <-ticker.C:
			if err := m.checkpointAndUpload(ctx, now); err != nil {
				nlog.Core().Warn("statistics checkpoint failed", "error", err)
			}
		case <-ctx.Done():
			shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Duration(m.cfg.RequestTimeout)*time.Second)
			_ = m.checkpointAndUpload(shutdownCtx, time.Now())
			cancel()
			return
		}
	}
}

func (m *Manager) Checkpoint(now time.Time) error {
	return m.checkpointAndUpload(context.Background(), now)
}

func (m *Manager) checkpointAndUpload(ctx context.Context, now time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var joined error
	for _, hour := range m.collector.TakeClosed(now) {
		if err := m.enqueue(hour); err != nil {
			m.collector.Restore([]Hour{hour})
			joined = errors.Join(joined, err)
		}
	}
	if err := m.saveCurrent(now); err != nil {
		joined = errors.Join(joined, err)
	}
	if err := m.uploadPending(ctx, now); err != nil {
		joined = errors.Join(joined, err)
	}
	return joined
}

func (m *Manager) enqueue(hour Hour) error {
	batchID := deterministicBatchID(m.panel.NodeID, hour.Start)
	name := hour.Start.UTC().Format("20060102T150405Z") + "_" + batchID + ".json.gz"
	path := filepath.Join(m.pendingPath(), name)
	if _, err := os.Stat(path); err == nil {
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}

	files, err := m.pendingFiles()
	if err != nil {
		return err
	}
	if len(files) >= m.cfg.MaxPendingBatches {
		return fmt.Errorf("statistics pending queue is full (%d)", m.cfg.MaxPendingBatches)
	}

	envelope := reportEnvelope{
		SchemaVersion:    1,
		BatchID:          batchID,
		BucketStart:      hour.Start.UTC().Format(time.RFC3339),
		BucketEnd:        hour.Start.UTC().Add(time.Hour).Format(time.RFC3339),
		CollectorVersion: "0.1.0",
		Records:          hour.Records,
		Quality:          hour.Quality,
	}
	body, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("marshal statistics batch: %w", err)
	}
	compressed, err := gzipBytes(body)
	if err != nil {
		return err
	}
	return atomicWrite(path, compressed, 0o600)
}

func (m *Manager) uploadPending(ctx context.Context, now time.Time) error {
	if !m.nextUploadAt.IsZero() && now.Before(m.nextUploadAt) {
		return nil
	}
	files, err := m.pendingFiles()
	if err != nil {
		return err
	}
	for index, path := range files {
		permanent, err := m.upload(ctx, path, len(files)-index)
		if err == nil {
			if removeErr := os.Remove(path); removeErr != nil {
				return removeErr
			}
			continue
		}
		if permanent {
			target := filepath.Join(m.rejectedPath(), filepath.Base(path))
			if renameErr := os.Rename(path, target); renameErr != nil {
				return errors.Join(err, renameErr)
			}
			nlog.Core().Error("statistics batch permanently rejected", "file", filepath.Base(path), "error", err)
			continue
		}
		m.retryAttempt++
		m.nextUploadAt = now.Add(retryBackoff(m.retryAttempt))
		return err
	}
	m.retryAttempt = 0
	m.nextUploadAt = time.Time{}
	return nil
}

func (m *Manager) upload(ctx context.Context, path string, pendingBatches int) (bool, error) {
	compressed, err := os.ReadFile(path)
	if err != nil {
		return false, err
	}
	plain, err := gunzipBytes(compressed)
	if err != nil {
		return true, fmt.Errorf("decode pending statistics batch: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(m.panel.URL, "/")+reportPath, bytes.NewReader(compressed))
	if err != nil {
		return true, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Encoding", "gzip")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-Statistics-Checksum", sha256Hex(plain))
	req.Header.Set("X-Xboard-Token", m.panel.Token)
	req.Header.Set("X-Xboard-Node-ID", fmt.Sprintf("%d", m.panel.NodeID))
	req.Header.Set("X-Statistics-Pending-Batches", fmt.Sprintf("%d", pendingBatches))
	req.Header.Set("X-Statistics-Retry-Attempt", fmt.Sprintf("%d", m.retryAttempt))
	if m.panel.MachineID > 0 {
		req.Header.Set("X-Xboard-Machine-ID", fmt.Sprintf("%d", m.panel.MachineID))
	}

	resp, err := m.client.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	responseBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return false, nil
	}
	permanent := resp.StatusCode >= 400 && resp.StatusCode < 500 && resp.StatusCode != http.StatusTooManyRequests
	return permanent, fmt.Errorf("statistics report status %d: %s", resp.StatusCode, strings.TrimSpace(string(responseBody)))
}

func (m *Manager) saveCurrent(now time.Time) error {
	state := persistedState{Version: 1, Hours: m.collector.CurrentHours(now)}
	body, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return atomicWrite(filepath.Join(m.spoolPath, "current.json"), body, 0o600)
}

func (m *Manager) loadCurrent() error {
	path := filepath.Join(m.spoolPath, "current.json")
	body, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read statistics current state: %w", err)
	}
	var state persistedState
	if err := json.Unmarshal(body, &state); err != nil {
		return fmt.Errorf("decode statistics current state: %w", err)
	}
	if state.Version != 1 {
		return fmt.Errorf("unsupported statistics state version %d", state.Version)
	}
	m.collector.Restore(state.Hours)
	return nil
}

func (m *Manager) pendingFiles() ([]string, error) {
	entries, err := os.ReadDir(m.pendingPath())
	if err != nil {
		return nil, err
	}
	files := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".json.gz") {
			files = append(files, filepath.Join(m.pendingPath(), entry.Name()))
		}
	}
	sort.Strings(files)
	return files, nil
}

func (m *Manager) pendingPath() string  { return filepath.Join(m.spoolPath, "pending") }
func (m *Manager) rejectedPath() string { return filepath.Join(m.spoolPath, "rejected") }

func deterministicBatchID(nodeID int, start time.Time) string {
	namespace := [16]byte{0x71, 0xe9, 0xe7, 0x48, 0x7f, 0x2e, 0x59, 0xa4, 0xb0, 0x13, 0x37, 0x9f, 0xcd, 0x58, 0x44, 0x1a}
	hash := sha1.New()
	hash.Write(namespace[:])
	fmt.Fprintf(hash, "%d:%s", nodeID, start.UTC().Format(time.RFC3339))
	value := hash.Sum(nil)[:16]
	value[6] = (value[6] & 0x0f) | 0x50
	value[8] = (value[8] & 0x3f) | 0x80
	hexValue := hex.EncodeToString(value)
	return hexValue[0:8] + "-" + hexValue[8:12] + "-" + hexValue[12:16] + "-" + hexValue[16:20] + "-" + hexValue[20:32]
}

func gzipBytes(body []byte) ([]byte, error) {
	var buffer bytes.Buffer
	writer, err := gzip.NewWriterLevel(&buffer, gzip.BestSpeed)
	if err != nil {
		return nil, err
	}
	writer.Header.ModTime = time.Unix(0, 0)
	if _, err := writer.Write(body); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func gunzipBytes(body []byte) ([]byte, error) {
	reader, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return io.ReadAll(reader)
}

func sha256Hex(body []byte) string {
	// Kept here instead of sharing the billing client so the immutable spool
	// payload and checksum always use the same decoded bytes.
	hash := sha256Sum(body)
	return hex.EncodeToString(hash[:])
}

func sha256Sum(body []byte) [32]byte {
	// Small wrapper makes checksum behavior easy to test without exporting it.
	return sha256.Sum256(body)
}

func atomicWrite(path string, body []byte, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), ".tmp-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(mode); err != nil {
		temporary.Close()
		return err
	}
	if _, err := temporary.Write(body); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	return os.Rename(temporaryPath, path)
}

func retryBackoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	if attempt > 7 {
		attempt = 7
	}
	base := 30 * time.Second * time.Duration(1<<(attempt-1))
	if base > 30*time.Minute {
		base = 30 * time.Minute
	}
	// Up to 25% positive jitter prevents a fleet from retrying in lock-step.
	maximumJitter := int64(base / 4)
	if maximumJitter <= 0 {
		return base
	}
	jitter, err := cryptorand.Int(cryptorand.Reader, big.NewInt(maximumJitter+1))
	if err != nil {
		return base
	}
	return base + time.Duration(jitter.Int64())
}
