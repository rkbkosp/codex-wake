package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/rkbkosp/codex-wake/internal/config"
	"github.com/rkbkosp/codex-wake/internal/ipc"
	"github.com/rkbkosp/codex-wake/internal/model"
	"github.com/rkbkosp/codex-wake/internal/protocol"
	"github.com/rkbkosp/codex-wake/internal/version"
)

func main() {
	if err := run(context.Background(), os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "codex-wait:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return usageError()
	}
	if args[0] == "--version" || args[0] == "version" {
		fmt.Println("codex-wait " + version.Current)
		return nil
	}
	command := args[0]
	parsed, err := parseArgs(args[1:])
	if err != nil {
		return err
	}
	dataDir, err := config.DefaultDataDir()
	if err != nil {
		return err
	}
	socket := os.Getenv("CODEX_WAIT_SOCKET")
	if socket == "" {
		socket = config.DefaultSocket(dataDir)
	}
	if parsed.socket != "" {
		socket = parsed.socket
	}
	client := ipc.Client{SocketPath: socket}
	switch command {
	case "pr-merge":
		if len(parsed.positionals) != 1 {
			return errors.New("usage: codex-wait pr-merge <PR_URL> [--after <note>] [--json]")
		}
		threadID, err := currentThreadID()
		if err != nil {
			return err
		}
		ref, err := model.ParseGitHubPRURL(parsed.positionals[0])
		if err != nil {
			return err
		}
		params := protocol.RegisterParams{Event: model.EventPRMerged, RepositoryKey: ref.RepositoryKey, RepositoryDisplay: ref.RepositoryDisplay, PRNumber: ref.Number, PRURL: ref.URL, ThreadID: threadID, Continuation: parsed.after}
		var result protocol.RegisterResult
		if err := client.Call(ctx, "wait.register", params, &result); err != nil {
			return err
		}
		if parsed.json {
			return printJSON(registerJSON(result))
		}
		fmt.Printf("armed %s for %s#%d (relay: %s)\n", result.Wait.ID, result.Wait.RepositoryDisplay, result.Wait.PRNumber, result.Relay)
		return nil
	case "list":
		if len(parsed.positionals) != 0 {
			return errors.New("usage: codex-wait list [--json]")
		}
		var waits []model.Wait
		if err := client.Call(ctx, "wait.list", struct{}{}, &waits); err != nil {
			return err
		}
		if parsed.json {
			return printJSON(waits)
		}
		if len(waits) == 0 {
			fmt.Println("no waits")
			return nil
		}
		for _, w := range waits {
			fmt.Printf("%s\t%s\t%s#%d\n", w.ID, strings.ToLower(string(w.State)), w.RepositoryDisplay, w.PRNumber)
		}
		return nil
	case "status":
		return oneWaitCommand(ctx, client, "wait.status", parsed)
	case "cancel":
		return oneWaitCommand(ctx, client, "wait.cancel", parsed)
	case "debug-fire":
		return oneWaitCommand(ctx, client, "wait.debug_fire", parsed)
	case "doctor":
		if len(parsed.positionals) != 0 {
			return errors.New("usage: codex-wait doctor [--json]")
		}
		var result protocol.DoctorResult
		if err := client.Call(ctx, "daemon.doctor", struct{}{}, &result); err != nil {
			return err
		}
		if parsed.json {
			return printJSON(result)
		}
		for _, key := range []string{"daemon_socket", "daemon_version", "relay_connection", "codex_queue", "local_database"} {
			fmt.Printf("%-20s %s\n", key+":", result.Checks[key])
		}
		return nil
	case "help", "--help", "-h":
		fmt.Print(usage())
		return nil
	default:
		return fmt.Errorf("unknown command %q\n%s", command, usage())
	}
}

type parsedArgs struct {
	json          bool
	after, socket string
	positionals   []string
}

func parseArgs(args []string) (parsedArgs, error) {
	var out parsedArgs
	for i := 0; i < len(args); i++ {
		a := args[i]
		switch {
		case a == "--json":
			out.json = true
		case a == "--after" || a == "--socket":
			if i+1 >= len(args) {
				return out, fmt.Errorf("%s requires a value", a)
			}
			i++
			if a == "--after" {
				out.after = args[i]
			} else {
				out.socket = args[i]
			}
		case strings.HasPrefix(a, "--after="):
			out.after = strings.TrimPrefix(a, "--after=")
		case strings.HasPrefix(a, "--socket="):
			out.socket = strings.TrimPrefix(a, "--socket=")
		case strings.HasPrefix(a, "-"):
			return out, fmt.Errorf("unknown option %q", a)
		default:
			out.positionals = append(out.positionals, a)
		}
	}
	return out, nil
}

func currentThreadID() (string, error) {
	thread := strings.TrimSpace(os.Getenv("CODEX_THREAD_ID"))
	session := strings.TrimSpace(os.Getenv("CODEX_SESSION_ID"))
	if thread != "" && session != "" && thread != session {
		return "", errors.New("CODEX_THREAD_ID and CODEX_SESSION_ID differ; refusing to register")
	}
	if thread != "" {
		return thread, nil
	}
	if session != "" {
		return session, nil
	}
	return "", errors.New("neither CODEX_THREAD_ID nor CODEX_SESSION_ID is set")
}

func oneWaitCommand(ctx context.Context, client ipc.Client, method string, args parsedArgs) error {
	if len(args.positionals) != 1 {
		return fmt.Errorf("usage: codex-wait %s <WAIT_ID> [--json]", strings.TrimPrefix(method, "wait."))
	}
	var w model.Wait
	if err := client.Call(ctx, method, protocol.IDParams{ID: args.positionals[0]}, &w); err != nil {
		return err
	}
	if args.json {
		return printJSON(w)
	}
	fmt.Printf("%s %s\n", w.ID, strings.ToLower(string(w.State)))
	return nil
}

type registrationOutput struct {
	WaitID     string `json:"wait_id"`
	State      string `json:"state"`
	Event      string `json:"event"`
	Repository string `json:"repository"`
	PRNumber   int64  `json:"pr_number"`
	PRURL      string `json:"pr_url"`
	ThreadID   string `json:"thread_id"`
	Relay      string `json:"relay"`
}

func registerJSON(result protocol.RegisterResult) registrationOutput {
	w := result.Wait
	return registrationOutput{WaitID: w.ID, State: strings.ToLower(string(w.State)), Event: w.EventType, Repository: w.RepositoryKey, PRNumber: w.PRNumber, PRURL: w.PRURL, ThreadID: w.CodexThreadID, Relay: result.Relay}
}

func printJSON(value any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(value)
}

func usageError() error { return errors.New(usage()) }
func usage() string {
	return `usage:
  codex-wait pr-merge <PR_URL> [--after <note>] [--json]
  codex-wait list [--json]
  codex-wait status <WAIT_ID> [--json]
  codex-wait cancel <WAIT_ID> [--json]
  codex-wait debug-fire <WAIT_ID> [--json]
  codex-wait doctor [--json]
`
}
