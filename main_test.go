package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestLocalPath(t *testing.T) {
	for _, path := range []string{"disk:/../escape", "disk:/a/../../escape", "disk:/a\\b", "disk:/C:/test", "disk://absolute", "disk:/", "other:/file"} {
		if _, err := localPath(path); err == nil {
			t.Errorf("accepted %q", path)
		}
	}
	if _, err := localPath("disk:/Фото/лето.jpg"); err != nil {
		t.Fatal(err)
	}
}

func TestDownloadPaginationRetryAndSkip(t *testing.T) {
	remotePath := `disk:/folder/file | "quoted".txt`
	localName := `folder/file | "quoted".txt`
	if runtime.GOOS == "windows" {
		remotePath = `disk:/folder/file: schedule | "quoted".txt`
		localName = "folder/file%3A schedule %7C %22quoted%22.txt"
	}
	data := "hello disk"
	sum := sha256.Sum256([]byte(data))
	hash := hex.EncodeToString(sum[:])
	var transfers, pages atomic.Int32
	var server *httptest.Server
	server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/blob" && r.Header.Get("Authorization") != "OAuth test" {
			t.Error("missing API authorization")
		}
		switch r.URL.Path {
		case "/resources/files":
			pages.Add(1)
			if r.URL.Query().Get("offset") == "0" {
				fmt.Fprintf(w, `{"items":[{"path":%q,"size":%d,"sha256":%q}]}`, remotePath, len(data), hash)
			} else {
				fmt.Fprint(w, `{"items":[]}`)
			}
		case "/resources/download":
			if got := r.URL.Query().Get("path"); got != remotePath {
				t.Errorf("download path = %q, want %q", got, remotePath)
			}
			fmt.Fprintf(w, `{"href":%q}`, server.URL+"/blob")
		case "/blob":
			if r.Header.Get("Authorization") != "" {
				t.Error("token leaked to download host")
			}
			if transfers.Add(1) == 1 {
				http.Error(w, "retry", 503)
				return
			}
			fmt.Fprint(w, data)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	root, err := os.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	d := &downloader{token: "test", base: server.URL, root: root, client: server.Client(), attempts: 2, timeout: time.Minute}
	if err := d.all(context.Background(), 3); err != nil {
		t.Fatal(err)
	}
	got, err := root.ReadFile(localName)
	if err != nil || string(got) != data {
		t.Fatalf("content %q: %v", got, err)
	}
	if err := d.all(context.Background(), 3); err != nil {
		t.Fatal(err)
	}
	if transfers.Load() != 2 || pages.Load() != 2 {
		t.Fatalf("transfers=%d pages=%d", transfers.Load(), pages.Load())
	}
	// A corrupted transfer must preserve the previously downloaded file.
	_, err = d.download(context.Background(), resource{Path: remotePath, Size: int64(len(data)), SHA256: strings.Repeat("0", 64)})
	if err == nil {
		t.Fatal("accepted wrong checksum")
	}
	got, _ = root.ReadFile(localName)
	if string(got) != data {
		t.Fatal("existing file damaged")
	}
	entries, _ := root.Open("folder")
	defer entries.Close()
	names, _ := entries.Readdirnames(-1)
	if len(names) != 1 {
		t.Fatalf("temporary files leaked: %v", names)
	}
}

func TestResumeOnlyFailedFiles(t *testing.T) {
	var pages, successful, recovered atomic.Int32
	var server *httptest.Server
	server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/resources/files":
			pages.Add(1)
			if r.URL.Query().Get("offset") == "0" {
				fmt.Fprint(w, `{"items":[{"path":"disk:/ok","size":1},{"path":"disk:/retry","size":1}]}`)
			} else {
				fmt.Fprint(w, `{"items":[]}`)
			}
		case "/resources/download":
			fmt.Fprintf(w, `{"href":%q}`, server.URL+strings.TrimPrefix(r.URL.Query().Get("path"), "disk:"))
		case "/ok":
			successful.Add(1)
			fmt.Fprint(w, "x")
		case "/retry":
			if recovered.Add(1) == 1 {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			fmt.Fprint(w, "y")
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	dest := t.TempDir()
	run := func() error {
		root, err := os.OpenRoot(dest)
		if err != nil {
			t.Fatal(err)
		}
		defer root.Close()
		d := &downloader{base: server.URL, root: root, client: server.Client(), attempts: 1, timeout: time.Minute}
		return d.all(context.Background(), 2)
	}
	if err := run(); err == nil {
		t.Fatal("expected first run to fail")
	}
	for range 2 {
		if err := run(); err != nil {
			t.Fatal(err)
		}
	}
	if pages.Load() != 2 || successful.Load() != 1 || recovered.Load() != 2 {
		t.Fatalf("pages=%d successful=%d recovered=%d", pages.Load(), successful.Load(), recovered.Load())
	}
}
