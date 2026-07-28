package statistics

import (
	"math/bits"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type aggregate struct {
	uploadBytes     uint64
	downloadBytes   uint64
	connectionCount uint64
	activeNanos     uint64
}

type minuteBucket struct {
	records       map[Dimensions]*aggregate
	domainsByUser map[int]map[string]struct{}
	destIPsByUser map[int]map[string]struct{}
	quality       Quality
}

// Collector tracks live connections and folds their counter deltas into UTC
// minute buckets. Byte deltas spanning a boundary are distributed in proportion
// to elapsed time while preserving the exact total.
type Collector struct {
	maxDimensions     int
	maxDomainsPerUser int
	maxDestIPsPerUser int

	mu      sync.Mutex
	buckets map[time.Time]*minuteBucket

	activeMu sync.RWMutex
	active   map[*Connection]struct{}
}

func NewCollector(maxDimensions int, perUserLimits ...int) *Collector {
	if maxDimensions <= 0 {
		maxDimensions = 200000
	}
	maxDomainsPerUser := 5000
	maxDestIPsPerUser := 10000
	if len(perUserLimits) > 0 && perUserLimits[0] > 0 {
		maxDomainsPerUser = perUserLimits[0]
	}
	if len(perUserLimits) > 1 && perUserLimits[1] > 0 {
		maxDestIPsPerUser = perUserLimits[1]
	}
	return &Collector{
		maxDimensions:     maxDimensions,
		maxDomainsPerUser: maxDomainsPerUser,
		maxDestIPsPerUser: maxDestIPsPerUser,
		buckets:           make(map[time.Time]*minuteBucket),
		active:            make(map[*Connection]struct{}),
	}
}

func (c *Collector) Open(dimensions Dimensions, now time.Time) *Connection {
	dimensions = normalizeDimensions(dimensions)
	connection := &Connection{
		collector:  c,
		dimensions: dimensions,
		lastAt:     now,
	}
	c.activeMu.Lock()
	c.active[connection] = struct{}{}
	c.activeMu.Unlock()
	c.add(minuteStart(now), dimensions, 0, 0, 1, 0)
	return connection
}

// Record adds byte deltas that are already associated with a concrete
// dimension, such as an individual UDP packet destination.
func (c *Collector) Record(dimensions Dimensions, upload, download uint64, now time.Time) {
	c.add(minuteStart(now), normalizeDimensions(dimensions), upload, download, 0, 0)
}

func (c *Collector) Snapshot(now time.Time) {
	c.activeMu.RLock()
	connections := make([]*Connection, 0, len(c.active))
	for connection := range c.active {
		connections = append(connections, connection)
	}
	c.activeMu.RUnlock()
	for _, connection := range connections {
		connection.snapshot(now)
	}
}

// TakeClosed snapshots active connections and removes every complete UTC minute.
func (c *Collector) TakeClosed(now time.Time) []Bucket {
	c.Snapshot(now)
	current := minuteStart(now)

	c.mu.Lock()
	starts := make([]time.Time, 0)
	for start := range c.buckets {
		if start.Before(current) {
			starts = append(starts, start)
		}
	}
	sort.Slice(starts, func(i, j int) bool { return starts[i].Before(starts[j]) })
	buckets := make([]Bucket, 0, len(starts))
	for _, start := range starts {
		bucket := c.buckets[start]
		delete(c.buckets, start)
		buckets = append(buckets, exportBucket(start, bucket))
	}
	c.mu.Unlock()
	return buckets
}

// CurrentBuckets returns a persistence snapshot without removing buckets.
func (c *Collector) CurrentBuckets(now time.Time) []Bucket {
	c.Snapshot(now)
	c.mu.Lock()
	starts := make([]time.Time, 0, len(c.buckets))
	for start := range c.buckets {
		starts = append(starts, start)
	}
	sort.Slice(starts, func(i, j int) bool { return starts[i].Before(starts[j]) })
	buckets := make([]Bucket, 0, len(starts))
	for _, start := range starts {
		buckets = append(buckets, exportBucket(start, c.buckets[start]))
	}
	c.mu.Unlock()
	return buckets
}

// Restore merges a previously persisted snapshot. It must be called before
// new connections are accepted.
func (c *Collector) Restore(buckets []Bucket) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, restored := range buckets {
		start := minuteStart(restored.Start)
		bucket := c.buckets[start]
		if bucket == nil {
			bucket = newMinuteBucket()
			c.buckets[start] = bucket
		}
		for _, record := range restored.Records {
			dimensions := normalizeDimensions(Dimensions{
				UserID:              record.UserID,
				SourceIP:            record.SourceIP,
				DestinationIP:       record.DestinationIP,
				ExactDomain:         record.ExactDomain,
				RegistrableDomain:   record.RegistrableDomain,
				Network:             record.Network,
				ApplicationProtocol: record.ApplicationProtocol,
				InboundType:         record.InboundType,
				OutboundTag:         record.OutboundTag,
				OutboundType:        record.OutboundType,
				DestinationPort:     record.DestinationPort,
			})
			bucket.remember(dimensions)
			metric := bucket.records[dimensions]
			if metric == nil {
				metric = &aggregate{}
				bucket.records[dimensions] = metric
			}
			metric.uploadBytes += record.UploadBytes
			metric.downloadBytes += record.DownloadBytes
			metric.connectionCount += record.ConnectionCount
			metric.activeNanos += record.ActiveSeconds * uint64(time.Second)
		}
		bucket.quality.CollapsedDimensions += restored.Quality.CollapsedDimensions
		bucket.quality.CollapsedBytes += restored.Quality.CollapsedBytes
		bucket.quality.UnknownDomainBytes += restored.Quality.UnknownDomainBytes
		bucket.quality.UnknownDestIPBytes += restored.Quality.UnknownDestIPBytes
	}
}

func (c *Collector) ActiveCount() int {
	c.activeMu.RLock()
	count := len(c.active)
	c.activeMu.RUnlock()
	return count
}

func (c *Collector) add(start time.Time, dimensions Dimensions, upload, download, connections, activeNanos uint64) {
	c.mu.Lock()
	bucket := c.buckets[start]
	if bucket == nil {
		bucket = newMinuteBucket()
		c.buckets[start] = bucket
	}
	dimensionCollapsed := false
	if bucket.collapsePerUser(&dimensions, c.maxDomainsPerUser, c.maxDestIPsPerUser) {
		dimensionCollapsed = true
	}
	metric := bucket.records[dimensions]
	collapsed := dimensionCollapsed
	if metric == nil && len(bucket.records) >= c.maxDimensions {
		dimensions = collapseDimensions(dimensions)
		metric = bucket.records[dimensions]
		collapsed = true
	}
	if metric == nil {
		metric = &aggregate{}
		bucket.records[dimensions] = metric
	}
	metric.uploadBytes += upload
	metric.downloadBytes += download
	metric.connectionCount += connections
	metric.activeNanos += activeNanos
	bytes := upload + download
	if collapsed {
		bucket.quality.CollapsedDimensions++
		bucket.quality.CollapsedBytes += bytes
	}
	if dimensions.ExactDomain == UnknownDimension {
		bucket.quality.UnknownDomainBytes += bytes
	}
	if dimensions.DestinationIP == UnknownDimension {
		bucket.quality.UnknownDestIPBytes += bytes
	}
	c.mu.Unlock()
}

func newMinuteBucket() *minuteBucket {
	return &minuteBucket{
		records:       make(map[Dimensions]*aggregate),
		domainsByUser: make(map[int]map[string]struct{}),
		destIPsByUser: make(map[int]map[string]struct{}),
	}
}

func (b *minuteBucket) remember(dimensions Dimensions) {
	if dimensions.ExactDomain != UnknownDimension && dimensions.ExactDomain != OtherDimension {
		values := b.domainsByUser[dimensions.UserID]
		if values == nil {
			values = make(map[string]struct{})
			b.domainsByUser[dimensions.UserID] = values
		}
		values[dimensions.ExactDomain] = struct{}{}
	}
	if dimensions.DestinationIP != UnknownDimension && dimensions.DestinationIP != OtherDimension {
		values := b.destIPsByUser[dimensions.UserID]
		if values == nil {
			values = make(map[string]struct{})
			b.destIPsByUser[dimensions.UserID] = values
		}
		values[dimensions.DestinationIP] = struct{}{}
	}
}

func (b *minuteBucket) collapsePerUser(dimensions *Dimensions, maxDomains, maxDestIPs int) bool {
	collapsed := false
	if dimensions.ExactDomain != UnknownDimension && dimensions.ExactDomain != OtherDimension {
		values := b.domainsByUser[dimensions.UserID]
		_, exists := values[dimensions.ExactDomain]
		if !exists && len(values) >= maxDomains {
			dimensions.ExactDomain = OtherDimension
			dimensions.RegistrableDomain = OtherDimension
			collapsed = true
		} else if !exists {
			if values == nil {
				values = make(map[string]struct{})
				b.domainsByUser[dimensions.UserID] = values
			}
			values[dimensions.ExactDomain] = struct{}{}
		}
	}
	if dimensions.DestinationIP != UnknownDimension && dimensions.DestinationIP != OtherDimension {
		values := b.destIPsByUser[dimensions.UserID]
		_, exists := values[dimensions.DestinationIP]
		if !exists && len(values) >= maxDestIPs {
			dimensions.DestinationIP = OtherDimension
			collapsed = true
		} else if !exists {
			if values == nil {
				values = make(map[string]struct{})
				b.destIPsByUser[dimensions.UserID] = values
			}
			values[dimensions.DestinationIP] = struct{}{}
		}
	}
	return collapsed
}

func (c *Collector) remove(connection *Connection) {
	c.activeMu.Lock()
	delete(c.active, connection)
	c.activeMu.Unlock()
}

type Connection struct {
	collector  *Collector
	dimensions Dimensions
	upload     atomic.Uint64
	download   atomic.Uint64
	closed     atomic.Bool

	mu           sync.Mutex
	lastUpload   uint64
	lastDownload uint64
	lastAt       time.Time
}

func (c *Connection) AddUpload(bytes uint64)   { c.upload.Add(bytes) }
func (c *Connection) AddDownload(bytes uint64) { c.download.Add(bytes) }

// SetDestinationIP upgrades an initially unknown destination after the
// outbound connection has completed its handshake. The lock also keeps this
// safe against a concurrent bucket snapshot.
func (c *Connection) SetDestinationIP(address string) {
	if address == "" {
		return
	}
	c.mu.Lock()
	if c.dimensions.DestinationIP == UnknownDimension {
		c.dimensions.DestinationIP = address
	}
	c.mu.Unlock()
}

func (c *Connection) Close(now time.Time) {
	if !c.closed.CompareAndSwap(false, true) {
		return
	}
	c.snapshot(now)
	c.collector.remove(c)
}

func (c *Connection) snapshot(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if now.Before(c.lastAt) {
		return
	}
	upload := c.upload.Load()
	download := c.download.Load()
	deltaUpload := upload - c.lastUpload
	deltaDownload := download - c.lastDownload
	from := c.lastAt
	c.lastUpload = upload
	c.lastDownload = download
	c.lastAt = now

	if !now.After(from) {
		if deltaUpload > 0 || deltaDownload > 0 {
			c.collector.add(minuteStart(now), c.dimensions, deltaUpload, deltaDownload, 0, 0)
		}
		return
	}

	totalNanos := uint64(now.Sub(from))
	remainingUpload := deltaUpload
	remainingDownload := deltaDownload
	for cursor := from; cursor.Before(now); {
		start := minuteStart(cursor)
		end := start.Add(time.Minute)
		if end.After(now) {
			end = now
		}
		segmentNanos := uint64(end.Sub(cursor))
		segmentUpload := remainingUpload
		segmentDownload := remainingDownload
		if end.Before(now) {
			segmentUpload = mulDiv(deltaUpload, segmentNanos, totalNanos)
			segmentDownload = mulDiv(deltaDownload, segmentNanos, totalNanos)
		}
		remainingUpload -= segmentUpload
		remainingDownload -= segmentDownload
		c.collector.add(start, c.dimensions, segmentUpload, segmentDownload, 0, segmentNanos)
		cursor = end
	}
}

func mulDiv(value, numerator, denominator uint64) uint64 {
	if value == 0 || numerator == 0 {
		return 0
	}
	hi, lo := bits.Mul64(value, numerator)
	quotient, _ := bits.Div64(hi, lo, denominator)
	return quotient
}

func minuteStart(value time.Time) time.Time {
	return value.UTC().Truncate(time.Minute)
}

func normalizeDimensions(dimensions Dimensions) Dimensions {
	if dimensions.SourceIP == "" {
		dimensions.SourceIP = UnknownDimension
	}
	if dimensions.DestinationIP == "" {
		dimensions.DestinationIP = UnknownDimension
	}
	dimensions.ExactDomain, dimensions.RegistrableDomain = NormalizeDomain(dimensions.ExactDomain)
	if dimensions.Network != "udp" {
		dimensions.Network = "tcp"
	}
	if dimensions.ApplicationProtocol == "" {
		dimensions.ApplicationProtocol = UnknownDimension
	}
	if dimensions.InboundType == "" {
		dimensions.InboundType = UnknownDimension
	}
	if dimensions.OutboundTag == "" {
		dimensions.OutboundTag = UnknownDimension
	}
	if dimensions.OutboundType == "" {
		dimensions.OutboundType = UnknownDimension
	}
	dimensions.ApplicationProtocol = strings.ToLower(dimensions.ApplicationProtocol)
	dimensions.InboundType = strings.ToLower(dimensions.InboundType)
	dimensions.OutboundTag = strings.ToLower(dimensions.OutboundTag)
	dimensions.OutboundType = strings.ToLower(dimensions.OutboundType)
	return dimensions
}

func collapseDimensions(dimensions Dimensions) Dimensions {
	dimensions.SourceIP = OtherDimension
	dimensions.DestinationIP = OtherDimension
	dimensions.ExactDomain = OtherDimension
	dimensions.RegistrableDomain = OtherDimension
	dimensions.OutboundTag = OtherDimension
	dimensions.OutboundType = OtherDimension
	dimensions.DestinationPort = 0
	return dimensions
}

func exportBucket(start time.Time, bucket *minuteBucket) Bucket {
	records := make([]Record, 0, len(bucket.records))
	for dimensions, metric := range bucket.records {
		records = append(records, Record{
			UserID:              dimensions.UserID,
			SourceIP:            dimensions.SourceIP,
			DestinationIP:       dimensions.DestinationIP,
			ExactDomain:         dimensions.ExactDomain,
			RegistrableDomain:   dimensions.RegistrableDomain,
			Network:             dimensions.Network,
			ApplicationProtocol: dimensions.ApplicationProtocol,
			InboundType:         dimensions.InboundType,
			OutboundTag:         dimensions.OutboundTag,
			OutboundType:        dimensions.OutboundType,
			DestinationPort:     dimensions.DestinationPort,
			UploadBytes:         metric.uploadBytes,
			DownloadBytes:       metric.downloadBytes,
			ConnectionCount:     metric.connectionCount,
			ActiveSeconds:       metric.activeNanos / uint64(time.Second),
		})
	}
	sort.Slice(records, func(i, j int) bool {
		if records[i].UserID != records[j].UserID {
			return records[i].UserID < records[j].UserID
		}
		if records[i].ExactDomain != records[j].ExactDomain {
			return records[i].ExactDomain < records[j].ExactDomain
		}
		if records[i].SourceIP != records[j].SourceIP {
			return records[i].SourceIP < records[j].SourceIP
		}
		return records[i].DestinationIP < records[j].DestinationIP
	})
	return Bucket{Start: start, Records: records, Quality: bucket.quality}
}
