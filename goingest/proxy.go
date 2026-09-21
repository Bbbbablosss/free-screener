package main

import (
	"log"
	"net/http"
	"net/url"
	"os"
)

// INGEST_WS_PROXY, when set, routes ALL websocket connections from this process
// through the given HTTP/HTTPS proxy. Used on a РФ ingest node to egress a
// geo-blocked exchange's WS (e.g. bitget) via a foreign proxy while REST and the
// Redis tunnel stay direct.
//
// http.ProxyURL returns the proxy for EVERY request regardless of scheme — this
// matters because net/http's httpproxy env logic only matches "http"/"https" and
// silently ignores the "wss" scheme gorilla hands it, so relying on HTTPS_PROXY
// env alone would NOT proxy wss:// connections. An explicit ProxyURL avoids that.
func init() {
	p := os.Getenv("INGEST_WS_PROXY")
	if p == "" {
		return
	}
	u, err := url.Parse(p)
	if err != nil {
		log.Printf("[ws] bad INGEST_WS_PROXY %q: %v (ignored)", p, err)
		return
	}
	wsDialer.Proxy = http.ProxyURL(u)
	log.Printf("[ws] routing ALL websocket connections through proxy %s", u.Host)
}
