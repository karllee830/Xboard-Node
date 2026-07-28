package kernel

import (
	"strings"

	"github.com/karllee830/Xboard-Node/internal/model"
)

func NeedsGeoIPRules(rules []model.CustomRouteRule) bool {
	for _, r := range rules {
		for _, v := range r.Match.IPCIDRs {
			if strings.HasPrefix(v, "geoip:") {
				return true
			}
		}
		for _, v := range r.Match.SourceCIDRs {
			if strings.HasPrefix(v, "geoip:") {
				return true
			}
		}
	}
	return false
}

func NeedsGeoSiteRules(rules []model.CustomRouteRule) bool {
	for _, r := range rules {
		for _, v := range r.Match.Domains {
			if strings.HasPrefix(v, "geosite:") {
				return true
			}
		}
		for _, v := range r.Match.DomainSuffixes {
			if strings.HasPrefix(v, "geosite:") {
				return true
			}
		}
	}
	return false
}
