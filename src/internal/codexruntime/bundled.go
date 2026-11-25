package codexruntime

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"time"

	"agentchat/src/internal/platformlock"
)

const maxBundledExpanded = 1 << 30
const maxBundleArchive = 256 << 20

var bundleVersion = regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+$`)
var bundleDigest = regexp.MustCompile(`^[0-9a-f]{64}$`)

//go:embed codex-lock.tsv
var codexLock string

type bundleSpec struct {
	asset, digest, version string
	client                 *http.Client
}

func lockedBundle(platform string) (*bundleSpec, error) {
	return parseLockedBundle(codexLock, platform)
}

func parseLockedBundle(lock, platform string) (*bundleSpec, error) {
	lock = strings.ReplaceAll(lock, "\r\n", "\n")
	lines := strings.Split(strings.TrimSuffix(lock, "\n"), "\n")
	if len(lines) != 4 {
		return nil, errors.New("invalid Codex version lock")
	}
	first := strings.Split(lines[0], "\t")
	if len(first) != 2 || first[0] != "version" || !bundleVersion.MatchString(first[1]) {
		return nil, errors.New("invalid Codex locked version")
	}
	expected := map[string]string{"darwin-arm64": "codex-package-aarch64-apple-darwin.tar.gz", "linux-amd64": "codex-package-x86_64-unknown-linux-musl.tar.gz", "windows-amd64": "codex-package-x86_64-pc-windows-msvc.tar.gz"}
	seen := map[string]bool{}
	var found *bundleSpec
	for _, line := range lines[1:] {
		parts := strings.Split(line, "\t")
		if len(parts) != 3 || expected[parts[0]] == "" || parts[1] != expected[parts[0]] || !bundleDigest.MatchString(parts[2]) || seen[parts[0]] {
			return nil, errors.New("invalid Codex lock entry")
		}
		seen[parts[0]] = true
		if parts[0] == platform {
			found = &bundleSpec{asset: parts[1], digest: parts[2], version: first[1]}
		}
	}
	if found == nil {
		return nil, errors.New("unsupported Codex platform")
	}
	return found, nil
}

func packageExecutable(root, platform string) (string, error) {
	suffix := ""
	if platform == "windows" {
		suffix = ".exe"
	}
	files := []string{filepath.Join("bin", "codex"+suffix), filepath.Join("bin", "codex-code-mode-host"+suffix), filepath.Join("bin", "codex-exec-server"+suffix), filepath.Join("codex-path", "rg"+suffix)}
	switch platform {
	case "linux":
		files = append(files, filepath.Join("codex-resources", "bwrap"))
	case "windows":
		files = append(files, filepath.Join("codex-resources", "codex-command-runner.exe"), filepath.Join("codex-resources", "codex-windows-sandbox-setup.exe"))
	}
	for _, name := range append([]string{"codex-package.json"}, files...) {
		info, err := os.Stat(filepath.Join(root, name))
		if err != nil || !info.Mode().IsRegular() {
			return "", fmt.Errorf("bundled Codex package incomplete: %s", name)
		}
		if name != "codex-package.json" && platform != "windows" && info.Mode().Perm()&0111 == 0 {
			return "", fmt.Errorf("bundled Codex entry is not executable: %s", name)
		}
	}
	return filepath.Join(root, "bin", "codex"+suffix), nil
}
func (b *bundleSpec) prepare(ctx context.Context, stateDir string) (string, error) {
	cache := filepath.Join(stateDir, "bundles")
	if err := os.MkdirAll(cache, 0700); err != nil {
		return "", err
	}
	info, err := os.Lstat(cache)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", errors.New("invalid Codex bundle cache")
	}
	lock, err := os.OpenFile(filepath.Join(cache, "."+b.digest+".lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return "", err
	}
	defer lock.Close()
	for {
		err = platformlock.Lock(lock, false)
		if err == nil {
			break
		}
		if !platformlock.IsContended(err) {
			return "", err
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
	defer platformlock.Unlock(lock)
	dest := filepath.Join(cache, b.digest)
	if _, err = os.Lstat(dest); err == nil {
		marker, e := os.ReadFile(filepath.Join(dest, ".agentchat-complete"))
		if e != nil || string(marker) != b.digest+"\n"+b.version+"\n" {
			return "", errors.New("incomplete Codex cache entry")
		}
		return b.verifyPackage(ctx, dest)
	} else if !os.IsNotExist(err) {
		return "", err
	}
	url := "https://github.com/openai/codex/releases/download/rust-v" + b.version + "/" + b.asset
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	client := b.client
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Minute}
	}
	resp, err := client.Do(request)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("Codex download returned %s", resp.Status)
	}
	archive, err := os.CreateTemp(cache, ".download-")
	if err != nil {
		return "", err
	}
	archivePath := archive.Name()
	defer func() { archive.Close(); os.Remove(archivePath) }()
	hash := sha256.New()
	n, err := io.Copy(io.MultiWriter(archive, hash), io.LimitReader(resp.Body, maxBundleArchive+1))
	if err != nil {
		return "", err
	}
	if n > maxBundleArchive {
		return "", errors.New("Codex download exceeds size limit")
	}
	if hex.EncodeToString(hash.Sum(nil)) != b.digest {
		return "", errors.New("Codex download digest mismatch")
	}

	stage, err := os.MkdirTemp(cache, ".extract-")
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(stage)
	if _, err = archive.Seek(0, io.SeekStart); err != nil {
		return "", err
	}
	if err = extractBundle(ctx, archive, stage); err != nil {
		return "", err
	}
	if _, err = b.verifyPackage(ctx, stage); err != nil {
		return "", err
	}
	if err = os.WriteFile(filepath.Join(stage, ".agentchat-complete"), []byte(b.digest+"\n"+b.version+"\n"), 0600); err != nil {
		return "", err
	}
	if err = os.Rename(stage, dest); err != nil {
		return "", err
	}
	return packageExecutable(dest, runtime.GOOS)
}
func (b *bundleSpec) verifyPackage(ctx context.Context, root string) (string, error) {
	info, err := os.Lstat(root)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", errors.New("invalid Codex bundle cache entry")
	}
	exe, err := packageExecutable(root, runtime.GOOS)
	if err != nil {
		return "", err
	}
	cmd := exec.CommandContext(ctx, exe, "--version")
	out, err := cmd.Output()
	if err != nil {
		return "", errors.New("cannot run bundled Codex version check")
	}
	if strings.TrimSpace(string(out)) != "codex-cli "+b.version {
		return "", errors.New("bundled Codex version mismatch")
	}
	return exe, nil
}
func extractBundle(ctx context.Context, source io.Reader, root string) error {
	gz, err := gzip.NewReader(source)
	if err != nil {
		return err
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	var expanded int64
	entries := 0
	for {
		if err = ctx.Err(); err != nil {
			return err
		}
		h, e := tr.Next()
		if e == io.EOF {
			return nil
		}
		if e != nil {
			return e
		}
		entries++
		if entries > 10000 {
			return errors.New("too many Codex bundle entries")
		}
		name := strings.TrimPrefix(h.Name, "./")
		name = strings.TrimSuffix(name, "/")
		if name == "" || name == "." {
			if h.Typeflag == tar.TypeDir {
				continue
			}
			return errors.New("invalid archive entry")
		}
		if strings.ContainsAny(name, "\\:") || strings.HasPrefix(name, "/") || path.Clean(name) != name || name == ".." || strings.HasPrefix(name, "../") {
			return errors.New("unsafe Codex bundle path")
		}
		dst := filepath.Join(root, filepath.FromSlash(name))
		switch h.Typeflag {
		case tar.TypeDir:
			if e = os.MkdirAll(dst, 0700); e != nil {
				return e
			}
		case tar.TypeReg, tar.TypeRegA:
			if h.Size < 0 || h.Size > maxBundledExpanded-expanded {
				return errors.New("Codex bundle exceeds expansion limit")
			}
			if e = os.MkdirAll(filepath.Dir(dst), 0700); e != nil {
				return e
			}
			mode := os.FileMode(0600)
			if h.Mode&0111 != 0 {
				mode |= 0100
			}
			f, e := os.OpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_WRONLY, mode)
			if e != nil {
				return e
			}
			n, e := io.CopyN(f, tr, h.Size)
			ce := f.Close()
			expanded += n
			if e != nil {
				return e
			}
			if ce != nil {
				return ce
			}
		default:
			return errors.New("unsupported Codex bundle entry")
		}
	}
}
