package statistics

import (
	"testing"
	"time"
)

func TestCollectorAggregatesAndClosesIdempotently(t *testing.T) {
	collector := NewCollector(100)
	start := time.Date(2026, 7, 28, 10, 15, 0, 0, time.UTC)
	dimensions := testDimensions()

	first := collector.Open(dimensions, start)
	first.AddUpload(100)
	first.AddDownload(900)
	first.Close(start.Add(20 * time.Second))
	first.Close(start.Add(30 * time.Second))

	second := collector.Open(dimensions, start.Add(time.Minute))
	second.AddUpload(50)
	second.AddDownload(450)
	second.Close(start.Add(time.Minute + 10*time.Second))

	hours := collector.TakeClosed(start.Add(time.Hour))
	if len(hours) != 1 || len(hours[0].Records) != 1 {
		t.Fatalf("hours=%+v, want one hour with one record", hours)
	}
	record := hours[0].Records[0]
	if record.UploadBytes != 150 || record.DownloadBytes != 1350 {
		t.Fatalf("bytes=(%d,%d), want (150,1350)", record.UploadBytes, record.DownloadBytes)
	}
	if record.ConnectionCount != 2 || record.ActiveSeconds != 30 {
		t.Fatalf("connections=%d active=%d, want 2 and 30", record.ConnectionCount, record.ActiveSeconds)
	}
	if collector.ActiveCount() != 0 {
		t.Fatalf("active=%d, want 0", collector.ActiveCount())
	}
}

func TestCollectorSplitsLongConnectionAcrossHoursWithoutLosingBytes(t *testing.T) {
	collector := NewCollector(100)
	start := time.Date(2026, 7, 28, 10, 59, 30, 0, time.UTC)
	connection := collector.Open(testDimensions(), start)
	connection.AddUpload(101)
	connection.AddDownload(1001)
	connection.Close(start.Add(time.Minute))

	hours := collector.TakeClosed(time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC))
	if len(hours) != 2 {
		t.Fatalf("len(hours)=%d, want 2", len(hours))
	}
	var upload, download, active uint64
	for _, hour := range hours {
		if len(hour.Records) != 1 {
			t.Fatalf("records=%d, want 1", len(hour.Records))
		}
		upload += hour.Records[0].UploadBytes
		download += hour.Records[0].DownloadBytes
		active += hour.Records[0].ActiveSeconds
	}
	if upload != 101 || download != 1001 || active != 60 {
		t.Fatalf("totals=(%d,%d,%d), want (101,1001,60)", upload, download, active)
	}
	if hours[0].Records[0].ConnectionCount != 1 || hours[1].Records[0].ConnectionCount != 0 {
		t.Fatalf("connection count must belong only to opening hour")
	}
}

func TestCollectorCollapsesNewHighCardinalityDimensions(t *testing.T) {
	collector := NewCollector(1)
	start := time.Date(2026, 7, 28, 10, 0, 0, 0, time.UTC)

	first := collector.Open(testDimensions(), start)
	first.AddDownload(100)
	first.Close(start.Add(time.Second))

	otherDimensions := testDimensions()
	otherDimensions.SourceIP = "198.51.100.99"
	otherDimensions.DestinationIP = "203.0.113.99"
	otherDimensions.ExactDomain = "other.example.net"
	second := collector.Open(otherDimensions, start.Add(2*time.Second))
	second.AddDownload(200)
	second.Close(start.Add(3 * time.Second))

	hour := collector.TakeClosed(start.Add(time.Hour))[0]
	if len(hour.Records) != 2 {
		t.Fatalf("records=%d, want original plus collapsed", len(hour.Records))
	}
	var collapsed *Record
	for index := range hour.Records {
		if hour.Records[index].SourceIP == OtherDimension {
			collapsed = &hour.Records[index]
		}
	}
	if collapsed == nil || collapsed.DestinationIP != OtherDimension || collapsed.ExactDomain != OtherDimension || collapsed.DestinationPort != 0 {
		t.Fatalf("collapsed record=%+v", collapsed)
	}
	if hour.Quality.CollapsedBytes != 200 || hour.Quality.CollapsedDimensions == 0 {
		t.Fatalf("quality=%+v", hour.Quality)
	}
}

func TestCollectorCapsDomainsAndDestinationIPsPerUser(t *testing.T) {
	collector := NewCollector(100, 1, 1)
	start := time.Date(2026, 7, 28, 10, 0, 0, 0, time.UTC)
	first := testDimensions()
	collector.Record(first, 10, 0, start)
	second := first
	second.ExactDomain = "second.example.net"
	second.RegistrableDomain = "example.net"
	second.DestinationIP = "203.0.113.81"
	collector.Record(second, 20, 0, start)

	hours := collector.TakeClosed(start.Add(time.Hour))
	if len(hours) != 1 || len(hours[0].Records) != 2 {
		t.Fatalf("hours=%+v", hours)
	}
	var collapsed *Record
	for index := range hours[0].Records {
		if hours[0].Records[index].ExactDomain == OtherDimension {
			collapsed = &hours[0].Records[index]
		}
	}
	if collapsed == nil || collapsed.DestinationIP != OtherDimension || collapsed.UploadBytes != 20 {
		t.Fatalf("collapsed=%+v", collapsed)
	}
	if hours[0].Quality.CollapsedBytes != 20 || hours[0].Quality.CollapsedDimensions != 1 {
		t.Fatalf("quality=%+v", hours[0].Quality)
	}
}

func TestNormalizeDomainUsesRegistrableDomain(t *testing.T) {
	exact, registrable := NormalizeDomain("VIDEO.Example.CO.UK.")
	if exact != "video.example.co.uk" || registrable != "example.co.uk" {
		t.Fatalf("got (%q,%q)", exact, registrable)
	}
}

func testDimensions() Dimensions {
	return Dimensions{
		UserID:              42,
		SourceIP:            "198.51.100.7",
		DestinationIP:       "203.0.113.80",
		ExactDomain:         "video.example.com",
		Network:             "tcp",
		ApplicationProtocol: "tls",
		InboundType:         "vless",
		OutboundTag:         "direct",
		OutboundType:        "direct",
		DestinationPort:     443,
	}
}
