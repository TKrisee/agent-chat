// Package updater downloads and stages a release without touching the running installation.
package updater

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"time"
)

const maxArchive = 256 << 20
const maxExpanded = 512 << 20

var versionPattern = regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+$`)
var digestPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

// Client fetches a plain text manifest and assets from a fixed release origin.
// BaseURL is the repository release URL (without /latest/download).
type Client struct {
	BaseURL  string
	HTTP     *http.Client
	OS, Arch string
}
type Release struct{ Version, Asset, SHA256 string }

func (c Client) platform() (string, error) {
	osname, arch := c.OS, c.Arch
	if osname == "" {
		osname = runtime.GOOS
	}
	if arch == "" {
		arch = runtime.GOARCH
	}
	p := osname + "-" + arch
	switch p {
	case "linux-amd64", "darwin-arm64", "windows-amd64":
		return p, nil
	}
	return "", fmt.Errorf("unsupported update platform %s", p)
}
func (c Client) client() *http.Client {
	if c.HTTP != nil {
		return c.HTTP
	}
	return &http.Client{Timeout: 2 * time.Minute}
}
func (c Client) get(ctx context.Context, url string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.client().Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("update server returned %s", resp.Status)
	}
	return resp, nil
}

// Check accepts exactly one version line and one checksum line for each supported asset.
func (c Client) Check(ctx context.Context) (Release, error) {
	platform, err := c.platform()
	if err != nil {
		return Release{}, err
	}
	resp, err := c.get(ctx, strings.TrimRight(c.BaseURL, "/")+"/latest/download/agentchat-update.txt")
	if err != nil {
		return Release{}, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 8193))
	if err != nil {
		return Release{}, err
	}
	if len(b) > 8192 {
		return Release{}, errors.New("update manifest too large")
	}
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	if len(lines) != 4 {
		return Release{}, errors.New("invalid update manifest")
	}
	version := strings.TrimPrefix(strings.TrimSpace(lines[0]), "version ")
	if !strings.HasPrefix(lines[0], "version ") || !versionPattern.MatchString(version) {
		return Release{}, errors.New("invalid release version")
	}
	ext := ".tar.gz"
	if strings.HasPrefix(platform, "windows-") {
		ext = ".zip"
	}
	asset := "agentchat-" + platform + ext
	checks := map[string]string{}
	for _, line := range lines[1:] {
		parts := strings.Fields(line)
		if len(parts) != 2 || !digestPattern.MatchString(parts[0]) || checks[parts[1]] != "" {
			return Release{}, errors.New("invalid update checksum")
		}
		checks[parts[1]] = parts[0]
	}
	for _, p := range []string{"linux-amd64.tar.gz", "darwin-arm64.tar.gz", "windows-amd64.zip"} {
		if checks["agentchat-"+p] == "" {
			return Release{}, errors.New("missing update checksum")
		}
	}
	return Release{Version: version, Asset: asset, SHA256: checks[asset]}, nil
}

// Download verifies the release archive before extracting it into a new staging directory.
// The caller must arrange shutdown, installation and rollback separately.
func (c Client) Download(ctx context.Context, r Release, parent string) (stage string, err error) {
	platform, e := c.platform()
	if e != nil {
		return "", e
	}
	ext := ".tar.gz"
	if strings.HasPrefix(platform, "windows-") {
		ext = ".zip"
	}
	if !versionPattern.MatchString(r.Version) || !digestPattern.MatchString(r.SHA256) || r.Asset != "agentchat-"+platform+ext {
		return "", errors.New("invalid release")
	}
	stage, err = os.MkdirTemp(parent, ".agentchat-update-")
	if err != nil {
		return "", err
	}
	createdStage := stage
	defer func() {
		if err != nil {
			os.RemoveAll(createdStage)
			stage = ""
		}
	}()
	resp, err := c.get(ctx, strings.TrimRight(c.BaseURL, "/")+"/download/v"+r.Version+"/"+r.Asset)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	file, err := os.CreateTemp(stage, "archive-")
	if err != nil {
		return "", err
	}
	archive := file.Name()
	h := sha256.New()
	n, copyErr := io.Copy(io.MultiWriter(file, h), io.LimitReader(resp.Body, maxArchive+1))
	closeErr := file.Close()
	if copyErr != nil {
		return "", copyErr
	}
	if closeErr != nil {
		return "", closeErr
	}
	if n > maxArchive {
		return "", errors.New("update archive too large")
	}
	if hex.EncodeToString(h.Sum(nil)) != r.SHA256 {
		return "", errors.New("update checksum mismatch")
	}
	payload := filepath.Join(stage, "payload")
	if err = os.Mkdir(payload, 0700); err != nil {
		return "", err
	}
	if ext == ".zip" {
		err = extractZip(archive, payload)
	} else {
		err = extractTar(archive, payload)
	}
	if err != nil {
		return "", err
	}
	if err = os.Remove(archive); err != nil {
		return "", err
	}
	exe := "agentchat"
	if ext == ".zip" {
		exe += ".exe"
	}
	if info, e := os.Stat(filepath.Join(payload, exe)); e != nil || !info.Mode().IsRegular() {
		return "", errors.New("update executable missing")
	}
	if info, e := os.Stat(filepath.Join(payload, "web", "index.html")); e != nil || !info.Mode().IsRegular() {
		return "", errors.New("update UI missing")
	}
	return stage, nil
}
func target(root, name string) (string, error) {
	name = strings.TrimPrefix(name, "./")
	if name == "" || name == "." {
		return root, nil
	}
	if strings.Contains(name, "\\") || strings.HasPrefix(name, "/") || path.Clean(name) != name || name == ".." || strings.HasPrefix(name, "../") {
		return "", errors.New("unsafe archive path")
	}
	return filepath.Join(root, filepath.FromSlash(name)), nil
}
func writeFile(root, name string, mode os.FileMode, src io.Reader, size int64, total *int64) error {
	if size < 0 || size > maxExpanded-*total {
		return errors.New("expanded update too large")
	}
	dst, err := target(root, name)
	if err != nil {
		return err
	}
	if err = os.MkdirAll(filepath.Dir(dst), 0700); err != nil {
		return err
	}
	f, err := os.OpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_WRONLY, mode&0755)
	if err != nil {
		return err
	}
	n, e := io.CopyN(f, src, size)
	ce := f.Close()
	*total += n
	if e != nil {
		return e
	}
	return ce
}
func extractTar(filename, root string) error {
	f, e := os.Open(filename)
	if e != nil {
		return e
	}
	defer f.Close()
	gz, e := gzip.NewReader(f)
	if e != nil {
		return e
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	var total int64
	for {
		h, e := tr.Next()
		if e == io.EOF {
			return nil
		}
		if e != nil {
			return e
		}
		dst, e := target(root, strings.TrimSuffix(h.Name, "/"))
		if e != nil {
			return e
		}
		switch h.Typeflag {
		case tar.TypeDir:
			if e = os.MkdirAll(dst, 0700); e != nil {
				return e
			}
		case tar.TypeReg, tar.TypeRegA:
			if e = writeFile(root, h.Name, os.FileMode(h.Mode), tr, h.Size, &total); e != nil {
				return e
			}
		default:
			return errors.New("unsupported archive entry")
		}
	}
}
func extractZip(filename, root string) error {
	z, e := zip.OpenReader(filename)
	if e != nil {
		return e
	}
	defer z.Close()
	var total int64
	for _, f := range z.File {
		dst, e := target(root, strings.TrimSuffix(f.Name, "/"))
		if e != nil {
			return e
		}
		if f.FileInfo().IsDir() {
			if e = os.MkdirAll(dst, 0700); e != nil {
				return e
			}
			continue
		}
		if !f.Mode().IsRegular() || f.UncompressedSize64 > maxExpanded {
			return errors.New("unsupported archive entry")
		}
		r, e := f.Open()
		if e != nil {
			return e
		}
		e = writeFile(root, f.Name, 0600, r, int64(f.UncompressedSize64), &total)
		ce := r.Close()
		if e != nil {
			return e
		}
		if ce != nil {
			return ce
		}
	}
	return nil
}
