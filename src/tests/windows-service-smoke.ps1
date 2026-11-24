$ErrorActionPreference = 'Stop'
$name = 'AgentChat-CI-' + [guid]::NewGuid().ToString('N')
$root = Join-Path $env:RUNNER_TEMP ("Agent Chat service smoke " + [guid]::NewGuid().ToString('N'))
$exe = Join-Path $root 'agentchat.exe'
$data = Join-Path $root 'service data'
$ui = Join-Path $root 'web UI'
$installed = $false
New-Item -ItemType Directory -Path $root | Out-Null
Copy-Item 'dist/agentchat.exe' $exe
Copy-Item -Recurse 'dist/web' $ui
$listener = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Loopback, 0)
$listener.Start()
$port = $listener.LocalEndpoint.Port
$listener.Stop()
try {
    & $exe service install -name $name -data $data -static $ui -runtime fixture -listen "127.0.0.1:$port" -start demand -account LocalSystem
    if ($LASTEXITCODE -ne 0) { throw "Service install failed: $LASTEXITCODE" }
    $installed = $true
    & $exe service start -name $name
    if ($LASTEXITCODE -ne 0) { throw "Service start failed: $LASTEXITCODE" }
    (Get-Service -Name $name).WaitForStatus('Running', [TimeSpan]::FromSeconds(30))
    $response = $null
    for ($i = 0; $i -lt 30; $i++) {
        try { $response = Invoke-WebRequest -UseBasicParsing "http://127.0.0.1:$port/"; break }
        catch { Start-Sleep -Seconds 1 }
    }
    if (!$response -or $response.StatusCode -ne 200 -or $response.Content -notmatch '<html') { throw 'Service UI smoke failed' }
    $state = Invoke-WebRequest -UseBasicParsing "http://127.0.0.1:$port/api/state"
    if ($state.StatusCode -ne 200) { throw 'Service API smoke failed' }
    & $exe service stop -name $name
    if ($LASTEXITCODE -ne 0) { throw "Service stop failed: $LASTEXITCODE" }
    (Get-Service -Name $name).WaitForStatus('Stopped', [TimeSpan]::FromSeconds(30))
} finally {
    if ($installed) {
        try {
            $service = Get-Service -Name $name -ErrorAction SilentlyContinue
            if ($service -and $service.Status -ne 'Stopped') {
                & $exe service stop -name $name
                if ($LASTEXITCODE -ne 0) { throw "Service cleanup stop failed: $LASTEXITCODE" }
                (Get-Service -Name $name).WaitForStatus('Stopped', [TimeSpan]::FromSeconds(30))
            }
        } finally {
            & $exe service uninstall -name $name
            if ($LASTEXITCODE -ne 0) { throw "Service cleanup uninstall failed: $LASTEXITCODE" }
            for ($i = 0; $i -lt 30 -and (Get-Service -Name $name -ErrorAction SilentlyContinue); $i++) { Start-Sleep -Seconds 1 }
            if (Get-Service -Name $name -ErrorAction SilentlyContinue) { throw 'Service was not removed' }
        }
    }
    Remove-Item -Recurse -Force $root -ErrorAction Stop
}
