# Agent Chat

Agent Chat is a local chat workspace for coordinating Codex agents across projects. It runs a service on your computer and opens in a browser.

## Install

1. Download the latest stable archive for your computer from [Agent Chat releases](https://github.com/TKrisee/agent-chat/releases/latest): `darwin-arm64` for Apple silicon, `linux-amd64` for 64-bit Linux, or `windows-amd64` for 64-bit Windows.
2. Extract the archive to a directory your account can write. Keep the launcher, service executable, and `web` directory together. Node.js and npm are not required to run the release.
3. Install [jq](https://jqlang.org/download/) separately and make sure it is available on your PATH. Agent Chat uses it to validate Codex results.

## Start

On macOS or Linux, from the extracted directory:

```sh
./agentchat-launcher -runtime codex \
  -data "$HOME/.local/share/agent-chat" \
  -listen 127.0.0.1:8877 \
  -jq-path "$(command -v jq)"
```

In PowerShell on Windows:

```powershell
.\agentchat-launcher.exe -runtime codex `
  -data "$env:LOCALAPPDATA\AgentChat\data" `
  -listen 127.0.0.1:8877 `
  -jq-path (Get-Command jq).Source
```

Open [http://127.0.0.1:8877](http://127.0.0.1:8877) on the same computer. On first launch, Settings shows download progress while Agent Chat prepares its pinned official Codex version; chat is available once setup is ready. Keep the launcher running while you use Agent Chat. Store the data directory separately from the extracted release and keep it private to the serving account. The service uses a fixture rather than Codex when `-runtime codex` is omitted.

Setup prepares the executable but does not sign you in. In another terminal, from the extracted directory and as the same OS account that runs the service, authenticate the managed CLI with the same data directory:

```sh
./agentchat codex login -data "$HOME/.local/share/agent-chat"
```

On Windows, use `.\agentchat.exe codex login -data "$env:LOCALAPPDATA\AgentChat\data"` in PowerShell. For a configured external CLI, run its own login command, or add `-codex-path <absolute-path>` to the `agentchat codex login` command. Follow the Codex sign-in instructions. Credentials and configuration remain in that account and are not part of the app archive.

Agent Chat downloads the exact Codex version pinned by the release into its private data directory. To use a different executable, pass `-codex-path` followed by its absolute path to the launcher. Settings shows the selected CLI and lets you switch between the managed version and a configured external executable. Your selection is retained across restarts.

## Use

Start in the Root conversation to describe your project. Select a project to open its group conversation, or select a project agent for a direct message. Type `@` in a group conversation to choose an agent. Use the attachment control or drop files into the conversation. Enter sends; Shift+Enter adds a line.

The right panel shows the team, work and project activity. Open Settings from the cog in the sidebar to check for app updates and configure the optional weekly allowance guard. When enabled, the guard holds new work at the configured reserve or while allowance information is unavailable; it does not interrupt work already running.

## Updates and recovery

In Settings, use **Check for updates**, then **Download** and **Restart to update** when a release is available. The launcher checks the replacement before activating it and retains the previous version if the trial does not become ready. Keep the installation directory writable for updates. The page reloads after a confirmed update when there are no unsent drafts or attachments; finish or clear them first. Pages loaded from older versions may need one manual refresh after updating.

Your data is stored separately from the release files. Keep a backup of the data directory before moving installations or manually reverting versions; reverting binaries does not automatically revert database changes. More service options are documented in [src/README.md](src/README.md).

The previous Python implementation is preserved on the `archive/python-parity` branch.
