package statistics

import "time"

const (
	UnknownDimension = "unknown"
	OtherDimension   = "__other__"
)

// Dimensions identifies one hourly traffic cube cell.
// It is intentionally comparable so it can be used directly as a map key.
type Dimensions struct {
	UserID              int
	SourceIP            string
	DestinationIP       string
	ExactDomain         string
	RegistrableDomain   string
	Network             string
	ApplicationProtocol string
	InboundType         string
	OutboundTag         string
	OutboundType        string
	DestinationPort     uint16
}

// Record is the report-v1 representation consumed by the Xboard plugin.
type Record struct {
	UserID              int    `json:"user_id"`
	SourceIP            string `json:"source_ip"`
	DestinationIP       string `json:"destination_ip"`
	ExactDomain         string `json:"exact_domain"`
	RegistrableDomain   string `json:"registrable_domain"`
	Network             string `json:"network"`
	ApplicationProtocol string `json:"application_protocol"`
	InboundType         string `json:"inbound_type"`
	OutboundTag         string `json:"outbound_tag"`
	OutboundType        string `json:"outbound_type"`
	DestinationPort     uint16 `json:"destination_port"`
	UploadBytes         uint64 `json:"upload_bytes"`
	DownloadBytes       uint64 `json:"download_bytes"`
	ConnectionCount     uint64 `json:"connection_count"`
	ActiveSeconds       uint64 `json:"active_seconds"`
}

type Quality struct {
	CollapsedDimensions uint64 `json:"collapsed_dimensions"`
	CollapsedBytes      uint64 `json:"collapsed_bytes"`
	UnknownDomainBytes  uint64 `json:"unknown_domain_bytes"`
	UnknownDestIPBytes  uint64 `json:"unknown_destination_ip_bytes"`
}

type Hour struct {
	Start   time.Time `json:"start"`
	Records []Record  `json:"records"`
	Quality Quality   `json:"quality"`
}
