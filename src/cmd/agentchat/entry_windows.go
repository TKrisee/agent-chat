//go:build windows

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"unsafe"
)

const defaultServiceName = "AgentChat"
const (
	serviceWin32OwnProcess = 0x10
	serviceStopped         = 1
	serviceStartPending    = 2
	serviceStopPending     = 3
	serviceRunning         = 4
	serviceAcceptStop      = 1
	serviceAcceptShutdown  = 4
)

type serviceStatus struct{ serviceType, currentState, controlsAccepted, win32ExitCode, serviceSpecificExitCode, checkPoint, waitHint uint32 }
type serviceTableEntry struct {
	name *uint16
	proc uintptr
}

var (
	advapi          = syscall.NewLazyDLL("advapi32.dll")
	startDispatcher = advapi.NewProc("StartServiceCtrlDispatcherW")
	registerHandler = advapi.NewProc("RegisterServiceCtrlHandlerExW")
	setStatus       = advapi.NewProc("SetServiceStatus")
	statusHandle    uintptr
	serviceCancel   context.CancelFunc
	serviceMu       sync.Mutex
	serviceErr      error
	serviceName     = defaultServiceName
)

func entry() error {
	if len(os.Args) < 2 || os.Args[1] != "service" {
		return foreground()
	}
	if len(os.Args) < 3 {
		return errors.New("usage: agentchat service install|uninstall|start|stop|run")
	}
	action := os.Args[2]
	if action == "run" {
		if len(os.Args) < 4 || !strings.HasPrefix(os.Args[3], "-name=") {
			return errors.New("service run requires -name=<service name>")
		}
		serviceName = strings.TrimPrefix(os.Args[3], "-name=")
		if err := validateServiceName(serviceName); err != nil {
			return err
		}
		os.Args = append([]string{os.Args[0]}, os.Args[4:]...)
		name, _ := syscall.UTF16PtrFromString(serviceName)
		table := []serviceTableEntry{{name: name, proc: syscall.NewCallback(serviceMain)}, {}}
		ok, _, err := startDispatcher.Call(uintptr(unsafe.Pointer(&table[0])))
		if ok == 0 {
			return fmt.Errorf("start service dispatcher: %w", err)
		}
		return serviceErr
	}
	if action == "install" {
		return installService(os.Args[3:])
	}
	if action != "uninstall" && action != "start" && action != "stop" {
		return fmt.Errorf("unknown service action %q", action)
	}
	fs := flag.NewFlagSet("service "+action, flag.ContinueOnError)
	name := fs.String("name", defaultServiceName, "service name")
	if err := fs.Parse(os.Args[3:]); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("unexpected service %s arguments", action)
	}
	if err := validateServiceName(*name); err != nil {
		return err
	}
	command := action
	if action == "uninstall" {
		command = "delete"
	}
	return runSC(command, *name)
}

func installService(args []string) error {
	fs := flag.NewFlagSet("service install", flag.ContinueOnError)
	name := fs.String("name", defaultServiceName, "service name")
	data := fs.String("data", "", "absolute data directory")
	static := fs.String("static", "", "absolute UI directory")
	codex := fs.String("codex-path", "", "absolute Codex executable path")
	jq := fs.String("jq-path", "", "absolute jq executable path")
	runtimeMode := fs.String("runtime", "fixture", "runtime adapter")
	listen := fs.String("listen", "127.0.0.1:8877", "loopback listen address")
	rootModel := fs.String("root-model", "", "Root model")
	poModel := fs.String("po-model", "", "PO model")
	workerModel := fs.String("worker-model", "", "worker model")
	effort := fs.String("effort", "high", "reasoning effort")
	workers := fs.Int("workers", 4, "maximum concurrent runtime attempts")
	start := fs.String("start", "demand", "service startup: demand or auto")
	account := fs.String("account", "LocalSystem", "service account (LocalSystem, NT AUTHORITY\\LocalService or NT AUTHORITY\\NetworkService)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return errors.New("unexpected service install arguments")
	}
	if err := validateServiceName(*name); err != nil {
		return err
	}
	if *start != "auto" && *start != "demand" {
		return errors.New("start must be auto or demand")
	}
	if *account != "LocalSystem" && *account != `NT AUTHORITY\LocalService` && *account != `NT AUTHORITY\NetworkService` {
		return errors.New("account must be LocalSystem, NT AUTHORITY\\LocalService, or NT AUTHORITY\\NetworkService")
	}
	for _, item := range []struct {
		name, value string
		required    bool
	}{{"data", *data, true}, {"static", *static, false}, {"codex-path", *codex, false}, {"jq-path", *jq, *runtimeMode == "codex"}} {
		if item.value == "" && item.required {
			return fmt.Errorf("-%s is required", item.name)
		}
		if item.value != "" && !filepath.IsAbs(item.value) {
			return fmt.Errorf("-%s must be absolute", item.name)
		}
	}
	if *runtimeMode != "fixture" && *runtimeMode != "codex" {
		return errors.New("runtime must be fixture or codex")
	}
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	argv := []string{exe, "service", "run", "-name=" + *name, "-data", *data, "-runtime", *runtimeMode, "-listen", *listen, "-effort", *effort, "-workers", fmt.Sprint(*workers)}
	for _, item := range []struct{ name, value string }{{"static", *static}, {"codex-path", *codex}, {"jq-path", *jq}, {"root-model", *rootModel}, {"po-model", *poModel}, {"worker-model", *workerModel}} {
		if item.value != "" {
			argv = append(argv, "-"+item.name, item.value)
		}
	}
	for i := range argv {
		argv[i] = syscall.EscapeArg(argv[i])
	}
	return runSC("create", *name, "binPath=", strings.Join(argv, " "), "start=", *start, "obj=", *account, "DisplayName=", "Agent Chat")
}

// Restrict names to portable, unambiguous SCM identifiers.
func validateServiceName(name string) error {
	if len(name) == 0 || len(name) > 128 {
		return errors.New("service name must contain 1 to 128 ASCII letters, digits, '.', '_' or '-'")
	}
	for _, c := range name {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '.' || c == '_' || c == '-') {
			return errors.New("service name must contain 1 to 128 ASCII letters, digits, '.', '_' or '-'")
		}
	}
	return nil
}

func runSC(args ...string) error {
	cmd := exec.Command("sc.exe", args...)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	return cmd.Run()
}

func reportStatus(state, exitCode, hint uint32) {
	status := serviceStatus{serviceType: serviceWin32OwnProcess, currentState: state, win32ExitCode: exitCode, waitHint: hint}
	if state == serviceRunning {
		status.controlsAccepted = serviceAcceptStop | serviceAcceptShutdown
	}
	setStatus.Call(statusHandle, uintptr(unsafe.Pointer(&status)))
}

func serviceMain(_ uint32, _ **uint16) uintptr {
	name, _ := syscall.UTF16PtrFromString(serviceName)
	handle, _, err := registerHandler.Call(uintptr(unsafe.Pointer(name)), syscall.NewCallback(serviceControl), 0)
	if handle == 0 {
		serviceErr = fmt.Errorf("register service handler: %w", err)
		return 0
	}
	statusHandle = handle
	reportStatus(serviceStartPending, 0, 30000)
	ctx, cancel := context.WithCancel(context.Background())
	serviceMu.Lock()
	serviceCancel = cancel
	serviceMu.Unlock()
	serviceErr = run(ctx, cancel, func() { reportStatus(serviceRunning, 0, 0) })
	cancel()
	code := uint32(0)
	if serviceErr != nil {
		code = 1
	}
	reportStatus(serviceStopped, code, 0)
	return 0
}

func serviceControl(control, _ uint32, _ uintptr, _ uintptr) uintptr {
	if control == 1 || control == 5 {
		serviceMu.Lock()
		cancel := serviceCancel
		serviceMu.Unlock()
		if cancel != nil {
			reportStatus(serviceStopPending, 0, 30000)
			cancel()
		}
	}
	return 0
}
