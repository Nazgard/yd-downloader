package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type resource struct {
	Path   string `json:"path"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

const retryFile = ".yd-downloader-retry.json"

type retryState struct {
	Listed bool       `json:"listed"`
	Files  []resource `json:"files"`
}

type downloader struct {
	token, base string
	client      *http.Client
	root        *os.Root
	attempts    int
	timeout     time.Duration
}

type statusError struct {
	code int
	wait time.Duration
}

func (e *statusError) Error() string { return fmt.Sprintf("HTTP %d", e.code) }

func main() {
	if err := run(); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}

func run() error {
	dest := flag.String("dest", "downloads", "Каталог назначения")
	workers := flag.Int("workers", 4, "Число параллельных загрузок")
	attempts := flag.Int("attempts", 5, "Максимальное число попыток")
	timeout := flag.Duration("timeout", 30*time.Minute, "Таймаут одной попытки загрузки файла")
	flag.Parse()
	if *workers < 1 || *attempts < 1 || *timeout <= 0 {
		return errors.New("workers, attempts и timeout должны быть положительными")
	}
	token := strings.TrimSpace(os.Getenv("YANDEX_DISK_TOKEN"))
	if token == "" {
		return errors.New("задайте OAuth-токен в YANDEX_DISK_TOKEN")
	}
	if err := os.MkdirAll(*dest, 0755); err != nil {
		return err
	}
	root, err := os.OpenRoot(*dest)
	if err != nil {
		return err
	}
	defer root.Close()
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConnsPerHost = *workers
	transport.ResponseHeaderTimeout = 30 * time.Second
	defer transport.CloseIdleConnections()
	d := &downloader{token: token, base: "https://cloud-api.yandex.net/v1/disk", root: root, attempts: *attempts, timeout: *timeout, client: &http.Client{Transport: transport}}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	return d.all(ctx, *workers)
}

func (d *downloader) retry(ctx context.Context, fn func() error) error {
	var err error
	for n := 0; n < d.attempts; n++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		err = fn()
		if err == nil {
			return nil
		}
		var se *statusError
		if errors.As(err, &se) && se.code != 429 && se.code != 408 && se.code < 500 {
			return err
		}
		if n+1 == d.attempts {
			break
		}
		delay := time.Second * time.Duration(1<<min(n, 5))
		if se != nil && se.wait > delay {
			delay = se.wait
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return err
}

func (d *downloader) get(ctx context.Context, address string, auth bool) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, address, nil)
	if err != nil {
		return nil, errors.New("некорректный URL")
	}
	if auth {
		req.Header.Set("Authorization", "OAuth "+d.token)
	}
	resp, err := d.client.Do(req)
	if err != nil {
		// Do not log signed download URLs or tokens from transport errors.
		var ue *url.Error
		if errors.As(err, &ue) {
			return nil, ue.Err
		}
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		wait := time.Duration(0)
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds > 0 {
			wait = time.Duration(min(seconds, 3600)) * time.Second
		} else if t, err := http.ParseTime(resp.Header.Get("Retry-After")); err == nil {
			wait = time.Until(t)
		}
		return nil, &statusError{resp.StatusCode, wait}
	}
	return resp, nil
}

func (d *downloader) api(ctx context.Context, endpoint string, query url.Values, out any) error {
	return d.retry(ctx, func() error {
		requestCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
		defer cancel()
		resp, err := d.get(requestCtx, d.base+endpoint+"?"+query.Encode(), true)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		return json.NewDecoder(io.LimitReader(resp.Body, 16<<20)).Decode(out)
	})
}

func (d *downloader) all(ctx context.Context, workers int) error {
	var state retryState
	data, err := d.root.ReadFile(retryFile)
	if err == nil {
		if err := json.Unmarshal(data, &state); err != nil {
			return fmt.Errorf("чтение %s: %w", retryFile, err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("чтение %s: %w", retryFile, err)
	}
	remaining := make(map[string]resource, len(state.Files))
	for _, item := range state.Files {
		remaining[item.Path] = item
	}
	var remainingMu sync.Mutex
	jobs := make(chan resource, workers)
	var wg sync.WaitGroup
	var done, skipped, failed atomic.Int64
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for item := range jobs {
				if ctx.Err() != nil {
					return
				}
				skip, err := d.download(ctx, item)
				remainingMu.Lock()
				if err != nil {
					remaining[item.Path] = item
				} else {
					delete(remaining, item.Path)
				}
				remainingMu.Unlock()
				if err != nil {
					failed.Add(1)
					log.Printf("Ошибка %q: %v", item.Path, err)
				} else if skip {
					skipped.Add(1)
				} else {
					done.Add(1)
					log.Printf("Скачан %q", item.Path)
				}
			}
		}()
	}
	queue := func(items []resource) error {
		for _, item := range items {
			select {
			case jobs <- item:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}
	listErr := func() error {
		if state.Listed {
			log.Printf("Файлов для повторной загрузки: %d", len(state.Files))
			return queue(state.Files)
		}
		for offset := 0; ; {
			var page struct {
				Items []resource `json:"items"`
			}
			err := d.api(ctx, "/resources/files", url.Values{"limit": {"1000"}, "offset": {strconv.Itoa(offset)}}, &page)
			if err != nil {
				return fmt.Errorf("список файлов: %w", err)
			}
			if len(page.Items) == 0 {
				return nil
			}
			if err := queue(page.Items); err != nil {
				return err
			}
			offset += len(page.Items)
		}
	}()
	close(jobs)
	wg.Wait()
	state.Listed = state.Listed || (listErr == nil && ctx.Err() == nil)
	state.Files = make([]resource, 0, len(remaining))
	for _, item := range remaining {
		state.Files = append(state.Files, item)
	}
	if err := d.saveRetryState(state); err != nil {
		return errors.Join(listErr, ctx.Err(), fmt.Errorf("запись %s: %w", retryFile, err))
	}
	log.Printf("Скачано: %d; пропущено: %d; ошибок: %d", done.Load(), skipped.Load(), failed.Load())
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if listErr != nil {
		return listErr
	}
	if failed.Load() > 0 {
		return fmt.Errorf("не удалось скачать %d файлов", failed.Load())
	}
	return nil
}

func (d *downloader) saveRetryState(state retryState) error {
	tmp := fmt.Sprintf(".yd-%d-%d.part", os.Getpid(), time.Now().UnixNano())
	f, err := d.root.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer d.root.Remove(tmp)
	encodeErr := json.NewEncoder(f).Encode(state)
	if err := errors.Join(encodeErr, f.Close()); err != nil {
		return err
	}
	return d.root.Rename(tmp, retryFile)
}

func localPath(remote string) (string, error) {
	if !strings.HasPrefix(remote, "disk:/") {
		return "", fmt.Errorf("неожиданный путь %q", remote)
	}
	rel := strings.TrimPrefix(remote, "disk:/")
	parts := strings.Split(rel, "/")
	if strings.EqualFold(parts[0], retryFile) {
		return "", fmt.Errorf("путь %q зарезервирован для списка повторной загрузки", remote)
	}
	for i, part := range parts {
		if part == "" || part == "." || part == ".." || strings.ContainsAny(part, "\\\x00") {
			return "", fmt.Errorf("небезопасный путь %q", remote)
		}
		if runtime.GOOS == "windows" {
			if len(part) == 2 && filepath.VolumeName(part) != "" {
				return "", fmt.Errorf("небезопасный путь %q", remote)
			}
			parts[i] = windowsName(part)
		} else if strings.ContainsRune(part, ':') {
			return "", fmt.Errorf("небезопасный путь %q", remote)
		}
	}
	path := filepath.FromSlash(strings.Join(parts, "/"))
	if !filepath.IsLocal(path) {
		return "", fmt.Errorf("недопустимый путь %q", remote)
	}
	return path, nil
}

func windowsName(name string) string {
	var encoded strings.Builder
	trailing := len(strings.TrimRight(name, ". "))
	for i, r := range name {
		if r < 32 || strings.ContainsRune(`%<>:"|?*`, r) || i >= trailing {
			fmt.Fprintf(&encoded, "%%%02X", r)
		} else {
			encoded.WriteRune(r)
		}
	}
	result := encoded.String()
	if !filepath.IsLocal(result) {
		// Reserved device names begin with an ASCII letter.
		result = fmt.Sprintf("%%%02X%s", result[0], result[1:])
	}
	if len(result) > 255 {
		units := 0
		for _, r := range result {
			units++
			if r > 0xFFFF {
				units++
			}
		}
		if units > 255 {
			return fmt.Sprintf("%%!%x", sha256.Sum256([]byte(name)))
		}
	}
	return result
}

func (d *downloader) download(ctx context.Context, item resource) (bool, error) {
	path, err := localPath(item.Path)
	if err != nil {
		return false, err
	}
	if item.SHA256 != "" {
		if f, err := d.root.Open(path); err == nil {
			info, statErr := f.Stat()
			if statErr == nil && info.Mode().IsRegular() && info.Size() == item.Size {
				h := sha256.New()
				_, hashErr := io.Copy(h, f)
				f.Close()
				if hashErr == nil && strings.EqualFold(hex.EncodeToString(h.Sum(nil)), item.SHA256) {
					return true, nil
				}
			} else {
				f.Close()
			}
		}
	}
	if err := d.root.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return false, err
	}
	err = d.retry(ctx, func() error {
		attemptCtx, cancel := context.WithTimeout(ctx, d.timeout)
		defer cancel()
		var link struct {
			Href string `json:"href"`
		}
		if err := d.api(attemptCtx, "/resources/download", url.Values{"path": {item.Path}}, &link); err != nil {
			return err
		}
		u, err := url.Parse(link.Href)
		if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil {
			return errors.New("API вернул некорректную HTTPS-ссылку")
		}
		resp, err := d.get(attemptCtx, link.Href, false)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		// Unique temporary names avoid collisions with actual files on Disk.
		tmp := filepath.Join(filepath.Dir(path), fmt.Sprintf(".yd-%d-%d.part", os.Getpid(), time.Now().UnixNano()))
		f, err := d.root.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			return err
		}
		defer d.root.Remove(tmp)
		h := sha256.New()
		n, copyErr := io.Copy(io.MultiWriter(f, h), resp.Body)
		closeErr := f.Close()
		if copyErr != nil {
			return copyErr
		}
		if closeErr != nil {
			return closeErr
		}
		if n != item.Size {
			return fmt.Errorf("размер: получено %d, ожидалось %d", n, item.Size)
		}
		if item.SHA256 != "" && !strings.EqualFold(hex.EncodeToString(h.Sum(nil)), item.SHA256) {
			return errors.New("SHA-256 не совпадает")
		}
		return d.root.Rename(tmp, path)
	})
	return false, err
}
