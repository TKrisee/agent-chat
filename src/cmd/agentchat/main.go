package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"agentchat/src/internal/fixture"
	"agentchat/src/internal/runtimeapi"
	"agentchat/src/internal/service"
	"agentchat/src/internal/updater"
)

var releaseBaseURL = "https://github.com/TKrisee/agent-chat/releases"

var makeCodexRuntime func(stateDir, codexPath, jqPath string) (runtimeapi.Runner, error)

func main() {
	if err := entry(); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context, stop context.CancelFunc, ready func()) error {
	var opts service.Options
	flag.StringVar(&opts.Directory, "data", "./data", "isolated service data directory")
	flag.StringVar(&opts.RootModel, "root-model", "", "Root model for a new database")
	flag.StringVar(&opts.POModel, "po-model", "", "model for newly created project POs")
	flag.StringVar(&opts.WorkerModel, "worker-model", "", "model for newly created workers")
	flag.StringVar(&opts.Effort, "effort", "high", "reasoning effort for newly created agents")
	flag.IntVar(&opts.Concurrency, "workers", 4, "maximum concurrent runtime attempts")
	listen := flag.String("listen", "127.0.0.1:8877", "loopback HTTP listen address")
	static := flag.String("static", "", "optional built UI directory")
	mode := flag.String("runtime", "fixture", "runtime adapter (fixture or codex)")
	codexPath := flag.String("codex-path", "", "Codex executable for -runtime codex")
	jqPath := flag.String("jq-path", "", "jq executable for -runtime codex")
	flag.Parse()

	host, _, err := net.SplitHostPort(*listen)
	if err != nil || !loopback(host) {
		return fmt.Errorf("listen must be a loopback address")
	}
	s, err := service.Open(opts)
	if err != nil {
		return err
	}
	defer s.Close()
	var runner runtimeapi.Runner
	switch *mode {
	case "fixture":
		runner = fixture.Runner{Dir: filepath.Join(opts.Directory, "fixture")}
	case "codex":
		if makeCodexRuntime == nil {
			return fmt.Errorf("codex runtime requires the adapter package and build tag codexruntime")
		}
		stateDir, err := filepath.Abs(filepath.Join(opts.Directory, "codex"))
		if err != nil {
			return err
		}
		runner, err = makeCodexRuntime(stateDir, *codexPath, *jqPath)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown runtime %q", *mode)
	}
	var ui http.Handler
	if *static != "" {
		info, err := os.Stat(*static)
		if err != nil || !info.IsDir() {
			return fmt.Errorf("static directory unavailable")
		}
		ui = http.FileServer(http.Dir(*static))
	}
	handler := s.Handler(ui)
	update := &updater.Handler{Client: updater.Client{BaseURL: releaseBaseURL}, Root: os.Getenv("AGENTCHAT_MANAGED_ROOT"), Version: os.Getenv("AGENTCHAT_MANAGED_VERSION"), Stop: stop}
	server := &http.Server{
		Addr: *listen, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second,
		WriteTimeout: 25 * time.Second, IdleTimeout: 45 * time.Second,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requestHost, _, err := net.SplitHostPort(r.Host)
			if err != nil {
				requestHost = r.Host
			}
			if !loopback(requestHost) {
				http.Error(w, "loopback Host required", http.StatusForbidden)
				return
			}
			if r.Method == http.MethodGet && r.URL.Path == "/api/update/identity" && os.Getenv("AGENTCHAT_MANAGED_TOKEN") != "" {
				w.Header().Set("Cache-Control", "no-store")
				_, _ = w.Write([]byte(os.Getenv("AGENTCHAT_MANAGED_TOKEN")))
				return
			}
			if strings.HasPrefix(r.URL.Path, "/api/update") {
				if origin := r.Header.Get("Origin"); origin != "" {
					u, err := url.Parse(origin)
					if err != nil || u.Host != r.Host || (u.Scheme != "http" && u.Scheme != "https") {
						http.Error(w, "cross-origin request refused", http.StatusForbidden)
						return
					}
				}
				update.ServeHTTP(w, r)
				return
			}
			handler.ServeHTTP(w, r)
		}),
	}
	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		return err
	}
	if ready != nil {
		ready()
	}
	serviceDone := make(chan error, 1)
	httpDone := make(chan error, 1)
	go func() { serviceDone <- s.Run(ctx, runner) }()
	go func() { httpDone <- server.Serve(listener) }()
	log.Printf("Agent Chat %s runtime on http://%s", *mode, listener.Addr())

	select {
	case <-ctx.Done():
		return errors.Join(shutdown(server), <-serviceDone, trimHTTPError(<-httpDone))
	case err := <-httpDone:
		stop()
		return errors.Join(trimHTTPError(err), <-serviceDone)
	case err := <-serviceDone:
		stop()
		return errors.Join(err, shutdown(server), trimHTTPError(<-httpDone))
	}
}

func loopback(host string) bool {
	return host == "localhost" || net.ParseIP(host).IsLoopback()
}

func shutdown(server *http.Server) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := server.Shutdown(ctx)
	if err != nil {
		_ = server.Close()
	}
	return err
}

func trimHTTPError(err error) error {
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
