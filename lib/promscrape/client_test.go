package promscrape

import (
	"io"
	"net"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/promauth"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/proxy"
)

func TestClientUnixSocket(t *testing.T) {
	socketPath := filepath.Join(t.TempDir(), "m.sock")
	ln, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatalf("cannot listen on unix socket %q: %s", socketPath, err)
	}
	const response = "foo 123\n"
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/metrics" {
				http.NotFound(w, r)
				return
			}
			_, _ = w.Write([]byte(response))
		}),
	}
	go func() {
		_ = srv.Serve(ln)
	}()
	defer func() {
		_ = srv.Close()
	}()

	sw := &ScrapeWork{
		ScrapeURL:       "http://node-sock/metrics",
		UnixSocketPath:  socketPath,
		ScrapeTimeout:   5 * time.Second,
		ScrapeInterval:  time.Minute,
		AuthConfig:      &promauth.Config{},
		ProxyAuthConfig: &promauth.Config{},
		ProxyURL:        &proxy.URL{},
	}
	c := newClient(sw)

	data, err := c.ReadData(nil)
	if err != nil {
		t.Fatalf("unexpected error from ReadData: %s", err)
	}
	if string(data) != response {
		t.Fatalf("unexpected response from ReadData; got %q; want %q", data, response)
	}

	sr, err := c.GetStreamReader()
	if err != nil {
		t.Fatalf("unexpected error from GetStreamReader: %s", err)
	}
	data, err = io.ReadAll(sr)
	sr.MustClose()
	if err != nil {
		t.Fatalf("unexpected error when reading the stream: %s", err)
	}
	if string(data) != response {
		t.Fatalf("unexpected response from the stream; got %q; want %q", data, response)
	}
}
