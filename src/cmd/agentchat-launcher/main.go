// agentchat-launcher supervises a managed foreground installation.
package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"syscall"
	"time"

	"agentchat/src/internal/platformlock"
)

var version = "dev" // set by release builds
var validVersion = regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+$`)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

// effectiveListen follows the standard Go flag parser: both dash forms are
// accepted, later values win, and parsing stops at a positional argument or --.
func effectiveListen(args []string) string {
	listen := "127.0.0.1:8877"
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--" || !strings.HasPrefix(arg, "-") || arg == "-" {
			break
		}
		name := strings.TrimPrefix(arg, "-")
		name = strings.TrimPrefix(name, "-")
		key, value, hasValue := strings.Cut(name, "=")
		if !hasValue {
			if i+1 >= len(args) {
				break
			}
			i++
			value = args[i]
		}
		if key == "listen" {
			listen = value
		}
	}
	return listen
}
func run() error {
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	root := filepath.Dir(exe)
	lock, err := os.OpenFile(filepath.Join(root, ".launcher.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer lock.Close()
	if err = platformlock.Lock(lock, false); err != nil {
		return fmt.Errorf("installation is already running: %w", err)
	}
	defer platformlock.Unlock(lock)
	active := version
	if b, e := os.ReadFile(filepath.Join(root, "active-version")); e == nil {
		active = strings.TrimSpace(string(b))
		if !validVersion.MatchString(active) {
			return fmt.Errorf("invalid active version")
		}
	} else if !os.IsNotExist(e) {
		return e
	}
	// An interrupted activation leaves the last committed pointer authoritative.
	// Preserve the uncommitted bundle for inspection and permit a later retry.
	requestPath := filepath.Join(root, "update-request")
	if b, e := os.ReadFile(requestPath); e == nil {
		pending := strings.TrimSpace(string(b))
		if !validVersion.MatchString(pending) {
			return fmt.Errorf("invalid pending update request")
		}
		if pending != active {
			src := filepath.Join(root, "versions", pending)
			if _, e = os.Stat(src); e == nil {
				dst := filepath.Join(root, "versions", fmt.Sprintf(".interrupted-%s-%d", pending, time.Now().UnixNano()))
				if e = os.Rename(src, dst); e != nil {
					return e
				}
			} else if !os.IsNotExist(e) {
				return e
			}
		}
		if e = os.Remove(requestPath); e != nil {
			return e
		}
	} else if !os.IsNotExist(e) {
		return e
	}
	args := os.Args[1:]
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)
	skipStart := false
	for {
		dir := root
		if active != version {
			dir = filepath.Join(root, "versions", active)
		}
		binary := "agentchat"
		if runtime.GOOS == "windows" {
			binary += ".exe"
		}
		if !skipStart {
			childArgs := append(append([]string{}, args...), "-static", filepath.Join(dir, "web"))
			child := exec.Command(filepath.Join(dir, binary), childArgs...)
			child.Stdin = os.Stdin
			child.Stdout = os.Stdout
			child.Stderr = os.Stderr
			child.Env = append(os.Environ(), "AGENTCHAT_MANAGED_ROOT="+root, "AGENTCHAT_MANAGED_VERSION="+active)
			if err = child.Start(); err != nil {
				return err
			}
			childDone := make(chan error, 1)
			go func() { childDone <- child.Wait() }()
			select {
			case err = <-childDone:
				if err != nil {
					return err
				}
			case sig := <-signals:
				forwardSignal(child.Process, sig)
				<-childDone
				return nil
			}
		}
		skipStart = false
		// A request is only written after an archive has been verified and staged.
		b, e := os.ReadFile(filepath.Join(root, "update-request"))
		if os.IsNotExist(e) {
			return nil
		}
		if e != nil {
			return e
		}
		next := strings.TrimSpace(string(b))
		if !validVersion.MatchString(next) || next == active {
			return fmt.Errorf("invalid update request")
		}
		staged := filepath.Join(root, "versions", next)
		if _, e = os.Stat(filepath.Join(staged, binary)); e != nil {
			return fmt.Errorf("staged executable unavailable: %w", e)
		}
		// Persist selection only after a trial process has remained alive. If it exits,
		// the previous selection remains active and can be launched again.
		trial := exec.Command(filepath.Join(staged, binary), append(append([]string{}, args...), "-static", filepath.Join(staged, "web"))...)
		trial.Stdin = os.Stdin
		trial.Stdout = os.Stdout
		trial.Stderr = os.Stderr
		tokenBytes := make([]byte, 32)
		if _, e = rand.Read(tokenBytes); e != nil {
			return e
		}
		token := hex.EncodeToString(tokenBytes)
		trial.Env = append(os.Environ(), "AGENTCHAT_MANAGED_ROOT="+root, "AGENTCHAT_MANAGED_VERSION="+next, "AGENTCHAT_MANAGED_TOKEN="+token)
		if e = trial.Start(); e != nil {
			return e
		}
		done := make(chan error, 1)
		go func() { done <- trial.Wait() }()
		listen := effectiveListen(args)
		ready := false
		client := &http.Client{Timeout: time.Second}
		deadline := time.After(30 * time.Second)
		ticker := time.NewTicker(200 * time.Millisecond)
	probe:
		for {
			select {
			case e = <-done:
				break probe
			case sig := <-signals:
				forwardSignal(trial.Process, sig)
				<-done
				ticker.Stop()
				return nil
			case <-deadline:
				trial.Process.Kill()
				<-done
				break probe
			case <-ticker.C:
				resp, requestErr := client.Get("http://" + listen + "/api/update/identity")
				if requestErr == nil {
					b, _ := io.ReadAll(io.LimitReader(resp.Body, 128))
					resp.Body.Close()
					if resp.StatusCode == 200 && string(b) == token {
						ready = true
						break probe
					}
				}
			}
		}
		ticker.Stop()
		if !ready {
			os.Remove(filepath.Join(root, "update-request"))
			failed := filepath.Join(root, "versions", fmt.Sprintf(".failed-%s-%d", next, time.Now().UnixNano()))
			if moveErr := os.Rename(staged, failed); moveErr != nil {
				return fmt.Errorf("preserve failed update: %w", moveErr)
			}
			log.Printf("updated process did not become ready; restarting previous version")
			continue
		}

		tmp := filepath.Join(root, "active-version.tmp")
		if e = os.WriteFile(tmp, []byte(next+"\n"), 0600); e != nil {
			trial.Process.Kill()
			<-done
			return e
		}
		if e = os.Rename(tmp, filepath.Join(root, "active-version")); e != nil {
			trial.Process.Kill()
			<-done
			return e
		}
		if e = os.Remove(filepath.Join(root, "update-request")); e != nil {
			trial.Process.Kill()
			<-done
			return e
		}
		select {
		case e = <-done:
		case sig := <-signals:
			forwardSignal(trial.Process, sig)
			<-done
			return nil
		}
		if e != nil {
			return e
		}
		active = next
		skipStart = true
	}
}
