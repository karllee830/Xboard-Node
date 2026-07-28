package singbox

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/karllee830/Xboard-Node/internal/config"
	"github.com/karllee830/Xboard-Node/internal/statistics"
	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing/common/buf"
	singM "github.com/sagernet/sing/common/metadata"
	"golang.org/x/time/rate"
)

func TestSingBoxCapabilities(t *testing.T) {
	s := New(config.KernelConfig{Type: "sing-box"})
	caps := s.Capabilities()
	if !caps.PerUserSpeedLimit || !caps.DeviceLimit || !caps.AliveIPTracking || !caps.ForceCloseUser {
		t.Fatalf("unexpected sing-box capabilities: %+v", caps)
	}
	if caps.BuiltInTrafficStats || caps.ForceCloseConnection {
		t.Fatalf("unexpected sing-box capabilities: %+v", caps)
	}
	protocols := s.Protocols()
	found := false
	for _, protocol := range protocols {
		if protocol == "hysteria" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("Protocols() missing hysteria: %v", protocols)
	}
}

type testConn struct {
	closed bool
	reads  [][]byte
	writes [][]byte
	remote net.Addr
}

type testPacketConn struct {
	closed      bool
	readData    []byte
	readDest    singM.Socksaddr
	writtenData []byte
	writtenDest singM.Socksaddr
}

func (c *testPacketConn) ReadPacket(buffer *buf.Buffer) (singM.Socksaddr, error) {
	_, _ = buffer.Write(c.readData)
	return c.readDest, nil
}
func (c *testPacketConn) WritePacket(buffer *buf.Buffer, destination singM.Socksaddr) error {
	c.writtenData = append([]byte(nil), buffer.Bytes()...)
	c.writtenDest = destination
	return nil
}
func (c *testPacketConn) Close() error                     { c.closed = true; return nil }
func (c *testPacketConn) LocalAddr() net.Addr              { return &net.UDPAddr{} }
func (c *testPacketConn) SetDeadline(time.Time) error      { return nil }
func (c *testPacketConn) SetReadDeadline(time.Time) error  { return nil }
func (c *testPacketConn) SetWriteDeadline(time.Time) error { return nil }

func (c *testConn) Read(b []byte) (int, error) {
	if len(c.reads) == 0 {
		return 0, errors.New("eof")
	}
	chunk := c.reads[0]
	c.reads = c.reads[1:]
	n := copy(b, chunk)
	return n, nil
}

func (c *testConn) Write(b []byte) (int, error) {
	cp := append([]byte(nil), b...)
	c.writes = append(c.writes, cp)
	return len(b), nil
}

func (c *testConn) Close() error        { c.closed = true; return nil }
func (c *testConn) LocalAddr() net.Addr { return &net.TCPAddr{} }
func (c *testConn) RemoteAddr() net.Addr {
	if c.remote != nil {
		return c.remote
	}
	return &net.TCPAddr{}
}
func (c *testConn) SetDeadline(time.Time) error      { return nil }
func (c *testConn) SetReadDeadline(time.Time) error  { return nil }
func (c *testConn) SetWriteDeadline(time.Time) error { return nil }

func testInboundContext(uuid, ip string) adapter.InboundContext {
	return adapter.InboundContext{
		User:   uuid,
		Source: singM.Socksaddr{Addr: netip.MustParseAddr(ip)},
	}
}

func TestConnTrackerRoutedConnectionTracksTrafficAndAliveIPs(t *testing.T) {
	tracker := NewConnTracker(0)
	tracker.SetUserMap(map[string]int{"uuid-1": 1})
	base := &testConn{reads: [][]byte{[]byte("hello")}}

	wrapped := tracker.RoutedConnection(context.Background(), base, testInboundContext("uuid-1", "1.1.1.1"), nil, nil)

	buf := make([]byte, 16)
	n, err := wrapped.Read(buf)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if n != 5 {
		t.Fatalf("Read() bytes = %d, want 5", n)
	}
	if _, err := wrapped.Write([]byte("bye")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	traffic, aliveIPs, connCount := tracker.GetUserTraffic()
	if got := traffic[1]; got != [2]int64{5, 3} {
		t.Fatalf("traffic[1] = %v, want [5 3]", got)
	}
	if !aliveIPs[1]["1.1.1.1"] {
		t.Fatal("expected alive IP to include source address")
	}
	if connCount != 1 {
		t.Fatalf("connCount = %d, want 1", connCount)
	}

	if err := wrapped.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	_, aliveIPs, connCount = tracker.GetUserTraffic()
	if aliveIPs[1] != nil {
		t.Fatalf("aliveIPs after close = %v, want nil", aliveIPs[1])
	}
	if connCount != 0 {
		t.Fatalf("connCount after close = %d, want 0", connCount)
	}
}

func TestConnTrackerCollectsDetailedTCPDimensions(t *testing.T) {
	collector := statistics.NewCollector(100)
	tracker := NewConnTracker(0)
	tracker.SetDetailedCollector(collector)
	tracker.SetUserMap(map[string]int{"uuid-1": 1})
	base := &testConn{reads: [][]byte{[]byte("hello")}}
	metadata := testInboundContext("uuid-1", "198.51.100.7")
	metadata.Destination = singM.Socksaddr{Addr: netip.MustParseAddr("203.0.113.80"), Port: 443}
	metadata.Domain = "video.example.com"
	metadata.Protocol = "tls"
	metadata.InboundType = "vless"

	wrapped := tracker.RoutedConnection(context.Background(), base, metadata, nil, nil)
	buffer := make([]byte, 16)
	if _, err := wrapped.Read(buffer); err != nil {
		t.Fatal(err)
	}
	if _, err := wrapped.Write([]byte("bye")); err != nil {
		t.Fatal(err)
	}
	if err := wrapped.Close(); err != nil {
		t.Fatal(err)
	}

	buckets := collector.TakeClosed(time.Now().UTC().Add(2 * time.Minute))
	if len(buckets) != 1 || len(buckets[0].Records) != 1 {
		t.Fatalf("buckets=%+v", buckets)
	}
	record := buckets[0].Records[0]
	if record.UserID != 1 || record.SourceIP != "198.51.100.7" || record.DestinationIP != "203.0.113.80" {
		t.Fatalf("identity dimensions=%+v", record)
	}
	if record.ExactDomain != "video.example.com" || record.RegistrableDomain != "example.com" || record.ApplicationProtocol != "tls" || record.InboundType != "vless" || record.DestinationPort != 443 {
		t.Fatalf("route dimensions=%+v", record)
	}
	if record.UploadBytes != 5 || record.DownloadBytes != 3 || record.ConnectionCount != 1 {
		t.Fatalf("metrics=%+v", record)
	}
}

func TestTrackedConnUpgradesDirectDestinationAfterHandshake(t *testing.T) {
	collector := statistics.NewCollector(100)
	connection := collector.Open(statistics.Dimensions{
		UserID:        1,
		DestinationIP: statistics.UnknownDimension,
		ExactDomain:   "chatgpt.com",
		Network:       "tcp",
	}, time.Now())
	tracked := &trackedConn{
		detailed: connection,
		direct:   true,
	}

	if err := tracked.ConnHandshakeSuccess(&testConn{
		remote: &net.TCPAddr{IP: net.ParseIP("203.0.113.42"), Port: 443},
	}); err != nil {
		t.Fatal(err)
	}
	connection.AddDownload(10)
	connection.Close(time.Now().Add(2 * time.Minute))

	buckets := collector.TakeClosed(time.Now().Add(3 * time.Minute))
	found := false
	for _, bucket := range buckets {
		for _, record := range bucket.Records {
			if record.DownloadBytes > 0 && record.DestinationIP == "203.0.113.42" {
				found = true
			}
		}
	}
	if !found {
		t.Fatalf("buckets=%+v", buckets)
	}
}

func TestTrackedConnDoesNotExposeProxyDestinationAsTarget(t *testing.T) {
	collector := statistics.NewCollector(100)
	connection := collector.Open(statistics.Dimensions{
		UserID:        1,
		DestinationIP: statistics.UnknownDimension,
		ExactDomain:   "chatgpt.com",
		Network:       "tcp",
	}, time.Now())
	tracked := &trackedConn{detailed: connection}

	if err := tracked.ConnHandshakeSuccess(&testConn{
		remote: &net.TCPAddr{IP: net.ParseIP("192.0.2.10"), Port: 1080},
	}); err != nil {
		t.Fatal(err)
	}
	connection.AddDownload(10)
	connection.Close(time.Now().Add(2 * time.Minute))

	buckets := collector.TakeClosed(time.Now().Add(3 * time.Minute))
	for _, bucket := range buckets {
		for _, record := range bucket.Records {
			if record.DownloadBytes > 0 && record.DestinationIP != statistics.UnknownDimension {
				t.Fatalf("destination_ip=%q, want unknown; buckets=%+v", record.DestinationIP, buckets)
			}
		}
	}
}

func TestConnTrackerCollectsDetailedUDPPerPacketDestination(t *testing.T) {
	collector := statistics.NewCollector(100)
	tracker := NewConnTracker(0)
	tracker.SetDetailedCollector(collector)
	tracker.SetUserMap(map[string]int{"uuid-1": 1})
	destination := singM.Socksaddr{Addr: netip.MustParseAddr("203.0.113.90"), Port: 53}
	base := &testPacketConn{readData: []byte("query"), readDest: destination}
	metadata := testInboundContext("uuid-1", "198.51.100.8")
	metadata.InboundType = "vless"
	metadata.Protocol = "dns"

	wrapped := tracker.RoutedPacketConnection(context.Background(), base, metadata, nil, nil)
	tracked := wrapped.(*trackedPacketConn)
	if tracked.ReaderReplaceable() || tracked.WriterReplaceable() {
		t.Fatal("detailed UDP must keep packet wrappers to retain each destination")
	}
	readBuffer := buf.NewPacket()
	defer readBuffer.Release()
	if _, err := wrapped.ReadPacket(readBuffer); err != nil {
		t.Fatal(err)
	}
	writeBuffer := buf.As([]byte("answer"))
	defer writeBuffer.Release()
	if err := wrapped.WritePacket(writeBuffer, destination); err != nil {
		t.Fatal(err)
	}
	if err := wrapped.Close(); err != nil {
		t.Fatal(err)
	}

	buckets := collector.TakeClosed(time.Now().UTC().Add(2 * time.Minute))
	var packetRecord *statistics.Record
	for bucketIndex := range buckets {
		for recordIndex := range buckets[bucketIndex].Records {
			record := &buckets[bucketIndex].Records[recordIndex]
			if record.DestinationIP == "203.0.113.90" && record.DestinationPort == 53 {
				packetRecord = record
			}
		}
	}
	if packetRecord == nil || packetRecord.UploadBytes != 5 || packetRecord.DownloadBytes != 6 {
		t.Fatalf("packet record=%+v", packetRecord)
	}
	if packetRecord.Network != "udp" || packetRecord.ApplicationProtocol != "dns" {
		t.Fatalf("packet dimensions=%+v", packetRecord)
	}
}

func TestConnTrackerRoutedConnectionRejectsWhenDeviceLimitExceeded(t *testing.T) {
	tracker := NewConnTracker(0)
	tracker.SetUserMap(map[string]int{"uuid-1": 1})
	tracker.SetDeviceLimitFunc(func(uuid string) (int, bool) {
		if uuid != "uuid-1" {
			return 0, false
		}
		return 1, true
	})

	first := &testConn{}
	wrapped1 := tracker.RoutedConnection(context.Background(), first, testInboundContext("uuid-1", "1.1.1.1"), nil, nil)
	if wrapped1 == first {
		t.Fatal("expected first connection to be wrapped")
	}

	second := &testConn{}
	wrapped2 := tracker.RoutedConnection(context.Background(), second, testInboundContext("uuid-1", "2.2.2.2"), nil, nil)
	if wrapped2 != second {
		t.Fatal("expected rejected connection to be returned unwrapped")
	}
	if !second.closed {
		t.Fatal("expected rejected connection to be closed")
	}
}

func TestConnTrackerCheckDeviceGateMergesFreshGlobalDevices(t *testing.T) {
	tracker := NewConnTracker(0)
	tracker.SetUserMap(map[string]int{"uuid-1": 1})
	us := tracker.users[1]
	us.addConn("1.1.1.1")
	tracker.UpdateGlobalDevices(map[int][]string{1: {"9.9.9.9"}})

	if tracker.checkDeviceGate(us, 1, "2.2.2.2", 2) {
		t.Fatal("expected lexicographically earlier candidate to remain allowed")
	}
	if !tracker.checkDeviceGate(us, 1, "99.99.99.99", 2) {
		t.Fatal("expected merged local+global device state to reject lexicographically later third device")
	}
	if tracker.checkDeviceGate(us, 1, "1.1.1.1", 2) {
		t.Fatal("existing local IP should still be allowed")
	}
	if tracker.checkDeviceGate(us, 1, "9.9.9.9", 2) {
		t.Fatal("existing global IP should still be allowed")
	}
}

func TestConnTrackerCloseByIDClosesTrackedConnection(t *testing.T) {
	tracker := NewConnTracker(0)
	tracker.SetUserMap(map[string]int{"uuid-1": 1})
	base := &testConn{}
	wrapped := tracker.RoutedConnection(context.Background(), base, testInboundContext("uuid-1", "1.1.1.1"), nil, nil)
	tracked, ok := wrapped.(*trackedConn)
	if !ok {
		t.Fatalf("wrapped type = %T, want *trackedConn", wrapped)
	}
	if !tracker.CloseByID(tracked.connID) {
		t.Fatal("CloseByID() = false, want true")
	}
	if !base.closed {
		t.Fatal("expected underlying connection to be closed")
	}
}

func TestConnTrackerRateLimitHonorsContextCancellation(t *testing.T) {
	tracker := NewConnTracker(0)
	tracker.SetUserMap(map[string]int{"uuid-1": 1})
	tracker.SetSpeedLimitFunc(func(uuid string) *rate.Limiter {
		if uuid != "uuid-1" {
			return nil
		}
		return rate.NewLimiter(rate.Limit(1), 1)
	})

	readCtx, cancelRead := context.WithCancel(context.Background())
	readBase := &testConn{reads: [][]byte{[]byte("a"), []byte("a")}}
	readTracked := tracker.RoutedConnection(readCtx, readBase, testInboundContext("uuid-1", "1.1.1.1"), nil, nil).(*trackedConn)
	buf := make([]byte, 8)
	if n, err := readTracked.Read(buf); err != nil || n != 1 {
		t.Fatalf("first Read() = (%d, %v), want (1, nil)", n, err)
	}
	cancelRead()
	if n, err := readTracked.Read(buf); !errors.Is(err, context.Canceled) || n != 1 {
		t.Fatalf("second Read() = (%d, %v), want (1, context canceled)", n, err)
	}

	writeCtx, cancelWrite := context.WithCancel(context.Background())
	writeBase := &testConn{}
	writeTracked := tracker.RoutedConnection(writeCtx, writeBase, testInboundContext("uuid-1", "1.1.1.2"), nil, nil).(*trackedConn)
	if n, err := writeTracked.Write([]byte("a")); err != nil || n != 1 {
		t.Fatalf("first Write() = (%d, %v), want (1, nil)", n, err)
	}
	cancelWrite()
	if n, err := writeTracked.Write([]byte("a")); !errors.Is(err, context.Canceled) || n != 0 {
		t.Fatalf("second Write() = (%d, %v), want (0, context canceled)", n, err)
	}
}
