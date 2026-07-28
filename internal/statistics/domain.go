package statistics

import (
	"strings"

	"golang.org/x/net/publicsuffix"
)

func NormalizeDomain(domain string) (exact, registrable string) {
	exact = strings.ToLower(strings.TrimSuffix(strings.TrimSpace(domain), "."))
	if exact == "" {
		return UnknownDimension, UnknownDimension
	}
	if exact == OtherDimension {
		return OtherDimension, OtherDimension
	}
	registrable, err := publicsuffix.EffectiveTLDPlusOne(exact)
	if err != nil {
		return exact, exact
	}
	return exact, strings.ToLower(registrable)
}
