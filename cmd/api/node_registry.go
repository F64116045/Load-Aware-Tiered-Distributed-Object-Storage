package main

import "sort"

func getActiveNodeURLs() []string {
	NodeListLock.RLock()
	out := make([]string, 0, len(ActiveNodeURLs))
	for _, url := range ActiveNodeURLs {
		out = append(out, url)
	}
	NodeListLock.RUnlock()
	sort.Strings(out)
	return out
}

func getActiveNodeCount() int {
	NodeListLock.RLock()
	n := len(ActiveNodeURLs)
	NodeListLock.RUnlock()
	return n
}
