package generator

import (
	"strings"
)

func (g *Generator) getBlacklist(datasetName string) *Blacklist {
	blacklist, ok := g.Blacklists[datasetName]
	if !ok {
		blacklist = &Blacklist{}
		blacklist.ignoredPrefixes = make(map[string]map[string]struct{})
	}
	return blacklist
}

func (b *Blacklist) prefixBlacklisted(path string, prefix string) bool {
	for k := range b.ignoredPrefixes[path] {
		if strings.HasPrefix(k, prefix) {
			return true
		}
	}
	return false
}

func (b *Blacklist) blacklistPrefix(path string, prefix string) {
	m, ok := b.ignoredPrefixes[path]
	if !ok {
		m = make(map[string]struct{})
	}
	m[prefix] = struct{}{}
	b.ignoredPrefixes[path] = m
}
