param([Parameter(Mandatory=$true)][ValidateSet('darwin-arm64','linux-amd64','windows-amd64')][string]$Platform)
$ErrorActionPreference = 'Stop'
$lines = @(Get-Content (Join-Path $PSScriptRoot '../internal/codexruntime/codex-lock.tsv'))
if ($lines.Count -ne 4) { throw 'Invalid Codex lock row count' }
$header = $lines[0].Split("`t")
if ($header.Count -ne 2 -or $header[0] -ne 'version' -or $header[1] -cnotmatch '^\d+\.\d+\.\d+$') { throw 'Invalid Codex version' }
$version = $header[1]
$entries = @($lines[1..3] | ForEach-Object { ,$_.Split("`t") })
$entry = @($entries | Where-Object { $_[0] -ceq $Platform })
if ($entry.Count -ne 1 -or $entry[0].Count -ne 3 -or $entry[0][1] -cnotmatch '^codex-package-[a-z0-9_-]+[.]tar[.]gz$' -or $entry[0][2] -cnotmatch '^[0-9a-f]{64}$') { throw 'Invalid Codex target lock' }
$sha = $entry[0][2]
$temp = Join-Path ([IO.Path]::GetTempPath()) ("agentchat-codex-check-" + [guid]::NewGuid())
New-Item -ItemType Directory $temp | Out-Null
try {
  $exe = if ($IsWindows) { '.exe' } else { '' }
  $app = Join-Path $PWD "dist/agentchat$exe"
  $observed = & $app codex version -data $temp
  if ($LASTEXITCODE -ne 0 -or "$observed" -ne "codex-cli $version") { throw 'Managed Codex version smoke failed' }
  $resolved = & $app codex path -data $temp
  if ($LASTEXITCODE -ne 0 -or @($resolved).Count -ne 1) { throw 'Managed Codex path unavailable' }
  $tree = Split-Path (Split-Path "$resolved" -Parent) -Parent
  if ((Split-Path $tree -Leaf) -cne $sha -or !(Test-Path -PathType Leaf "$resolved")) { throw 'Managed Codex cache path mismatch' }
  $required = @('codex-package.json', "bin/codex$exe", "bin/codex-code-mode-host$exe", "bin/codex-exec-server$exe", "codex-path/rg$exe")
  if ($IsLinux) { $required += 'codex-resources/bwrap' }
  if ($IsWindows) { $required += @('codex-resources/codex-command-runner.exe','codex-resources/codex-windows-sandbox-setup.exe') }
  foreach ($item in $required) { if (!(Test-Path -PathType Leaf (Join-Path $tree $item))) { throw "Codex package missing $item" } }
  $help = (& "$resolved" exec --help | Out-String)
  if ($LASTEXITCODE -ne 0 -or !$help.Contains('--ephemeral') -or !$help.Contains('--output-schema') -or !$help.Contains('--output-last-message')) { throw 'Codex exec contract unavailable' }
  & "$resolved" app-server --help | Out-Null
  if ($LASTEXITCODE -ne 0) { throw 'Codex app-server unavailable' }
} finally { if (Test-Path $temp) { Remove-Item -LiteralPath $temp -Recurse -Force } }
