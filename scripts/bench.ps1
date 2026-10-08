# goHarness unified benchmark entry (plan 2.11): runs the Smoke / Full /
# Stress groups with fixed parameters, saves raw output and emits a
# manifest. Compare baselines with benchstat on the same scenario names.
#
#   ./scripts/bench.ps1 -Profile Full -Count 10 -Benchtime 1s -Cpu 1 `
#       -OutputDirectory .bench/results/before
#   benchstat .bench/results/before/core-cpu1.txt .bench/results/after/core-cpu1.txt
[CmdletBinding()]
param(
    [ValidateSet('Smoke', 'Full', 'Stress')]
    [string]$Profile = 'Smoke',
    [int]$Count = 1,
    [string]$Benchtime = '100ms',
    [int]$Cpu = 1,
    [string]$OutputDirectory = '.bench/results/run',
    # Opt-in real Redis group; requires GOHARNESS_BENCH_REDIS_ADDR.
    [switch]$Redis
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path

$benchPattern = switch ($Profile) {
    'Smoke'  { '^BenchmarkGHSmoke$' }
    'Stress' { '^BenchmarkGHStress$' }
    default  { '^Benchmark(GHFull|MessageTransport)$' }
}

# 预期场景清单与完整性校验在 bench-validate.ps1，便于单独测试。

if ($Redis) {
    if (-not $env:GOHARNESS_BENCH_REDIS_ADDR) {
        throw "Redis benchmarks require GOHARNESS_BENCH_REDIS_ADDR"
    }
    # The extensions module is separate: run inside extensions/.
    $workDir = Join-Path $repoRoot 'extensions'
    $goArgs = @('test', '-p=1', './state/redis',
        '-run', '^$', '-bench', '^BenchmarkGHRedis$', '-benchmem',
        "-benchtime=$Benchtime", "-count=$Count", "-cpu=$Cpu")
} else {
    $workDir = $repoRoot
    $goArgs = @('test', '-p=1',
        './pkg/schema', './pkg/contextengine', './pkg/tool',
        './pkg/graph', './pkg/agent', './pkg/state',
        '-run', '^$', '-bench', $benchPattern, '-benchmem',
        "-benchtime=$Benchtime", "-count=$Count", "-cpu=$Cpu")
}

New-Item -ItemType Directory -Force -Path $OutputDirectory | Out-Null
# Resolve once relative to the caller's directory; reuse for every artifact.
$outDir = (Resolve-Path $OutputDirectory).Path
$outFile = Join-Path $outDir "core-cpu$Cpu.txt"

Push-Location $workDir
try {
    Write-Host "go $($goArgs -join ' ')" -ForegroundColor DarkGray
    # Collect then write UTF-8 without BOM: benchstat cannot parse UTF-16.
    $raw = & { $ErrorActionPreference = 'Continue'; & go @goArgs 2>&1 } | Out-String -Stream
    $exit = $LASTEXITCODE
} finally {
    Pop-Location
}
[IO.File]::WriteAllText($outFile, ($raw -join "`n") + "`n", (New-Object Text.UTF8Encoding $false))
$raw
if ($exit -ne 0) {
    Write-Error "benchmark run failed with exit code $exit (see $outFile)"
    exit $exit
}

# Verify every expected scenario ran with exactly -Count result lines:
# a missing package, missing scenario or undersampled run must not slip
# through as a PASS. The scenario list lives in bench-validate.ps1.
& (Join-Path $PSScriptRoot 'bench-validate.ps1') -Profile $Profile `
    -Count $Count -Cpu $Cpu -OutputFile $outFile -Redis:$Redis
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

# Manifest: environment, build and run parameters for baseline review.
# Read effective values via `go env` (environment variables are usually
# unset even though the toolchain has real defaults).
$goVersion = (& { $ErrorActionPreference = 'Continue'; & go version 2>$null }) -join ' '
$goEnvKeys = @('GOOS', 'GOARCH', 'CGO_ENABLED')
$goEnvVals = & { $ErrorActionPreference = 'Continue'; & go env @goEnvKeys 2>$null }
$goEnv = @{}
for ($i = 0; $i -lt $goEnvKeys.Count; $i++) { $goEnv[$goEnvKeys[$i]] = "$($goEnvVals[$i])" }
# GOMAXPROCS is fixed by -cpu below; GOGC/GOMEMLIMIT are unset unless the
# caller exported them (go env reports empty for unset runtime knobs).
$gogc = if ($env:GOGC) { $env:GOGC } else { 'unset' }
$gomemlimit = if ($env:GOMEMLIMIT) { $env:GOMEMLIMIT } else { 'unset' }

# 采样代码身份：fixture 版本取自 benchfixture.Version 单一来源；
# HEAD、工作区脏标记随 manifest 记录，脏工作区把补丁与未跟踪源码
# 指纹一并归档，保证采样结果能还原到当时的实现。
$fixtureVersion = 'unknown'
$versionMatch = Select-String -Path (Join-Path $repoRoot 'internal/benchfixture/messages.go') `
    -Pattern 'const Version = "([^"]+)"'
if ($versionMatch) { $fixtureVersion = $versionMatch.Matches[0].Groups[1].Value }

# git 的 stderr 警告（如 CRLF 提示）在 EAP=Stop 下会成为终止错误；
# 在函数作用域内放宽并丢弃 stderr。
function Invoke-Git {
    param([Parameter(ValueFromRemainingArguments = $true)][string[]]$GitArgs)
    $ErrorActionPreference = 'Continue'
    & git -C $repoRoot @GitArgs 2>$null
}
$gitHead = 'unavailable'
$gitDirty = 'unknown'
$workdirArchive = ''
if (Get-Command git -ErrorAction SilentlyContinue) {
    $head = Invoke-Git rev-parse HEAD
    if ($head) { $gitHead = "$head".Trim() }
    # core.quotepath=false 让中文路径直接输出 UTF-8；同时把控制台
    # 输出解码切到 UTF-8，否则中文文件名在管道里变乱码。
    $prevOutEnc = [Console]::OutputEncoding
    [Console]::OutputEncoding = [Text.Encoding]::UTF8
    try {
        $porcelain = @(Invoke-Git -c core.quotepath=false status --porcelain)
        $untracked = @(Invoke-Git -c core.quotepath=false ls-files --others --exclude-standard)
    } finally {
        [Console]::OutputEncoding = $prevOutEnc
    }
    $gitDirty = [bool]$porcelain
    if ($gitDirty) {
        # 补丁由 git 直接写出：PowerShell 文本往返会损坏字节序列，
        # 产生 git apply 无法解析的补丁。
        $patchPath = Join-Path $outDir 'workdir.patch'
        Invoke-Git diff --binary "--output=$patchPath" HEAD
        [IO.File]::WriteAllText((Join-Path $outDir 'workdir-status.txt'),
            ($porcelain -join "`n") + "`n", (New-Object Text.UTF8Encoding $false))
        # 未跟踪文件不进 diff：按相对路径归档构建与基准相关源码，
        # 并对全部未跟踪文件记录 SHA256 指纹用于还原核对。
        $srcRoot = Join-Path $outDir 'workdir-src'
        # .tmp/ 是本地构建缓存、.bench/ 是结果产物，两者非源码，
        # 既不进归档也不计指纹。
        $fingerprints = foreach ($rel in $untracked) {
            if ($rel -match '^(\.tmp|\.bench)[\\/]') { continue }
            $full = Join-Path $repoRoot $rel
            if (-not (Test-Path -LiteralPath $full -PathType Leaf)) { continue }
            $hash = (Get-FileHash -LiteralPath $full -Algorithm SHA256).Hash
            if ($rel -match '\.(go|md|ps1)$' -or $rel -match '(^|[\\/])go\.(mod|sum)$') {
                $dst = Join-Path $srcRoot $rel
                New-Item -ItemType Directory -Force -Path (Split-Path $dst) | Out-Null
                Copy-Item -LiteralPath $full -Destination $dst
            }
            "$hash  $rel"
        }
        if ($fingerprints) {
            [IO.File]::WriteAllText((Join-Path $outDir 'workdir-fingerprints.txt'),
                ($fingerprints -join "`n") + "`n", (New-Object Text.UTF8Encoding $false))
        }
        $workdirArchive = 'workdir.patch + workdir-status.txt + workdir-src/ + workdir-fingerprints.txt'
    }
}
$manifest = [ordered]@{
    profile          = $Profile
    count            = $Count
    benchtime        = $Benchtime
    cpu              = $Cpu
    redis            = [bool]$Redis
    fixture_version  = $fixtureVersion
    bench_version    = 'bench-v2'
    git_head         = $gitHead
    git_dirty        = $gitDirty
    workdir_archive  = $workdirArchive
    command          = "go $($goArgs -join ' ')"
    date             = (Get-Date).ToString('o')
    go_version       = $goVersion
    goos             = $goEnv['GOOS']
    goarch           = $goEnv['GOARCH']
    gomaxprocs       = $Cpu
    gogc             = $gogc
    gomemlimit       = $gomemlimit
    cgo_enabled      = $goEnv['CGO_ENABLED']
    cpu_model        = $env:PROCESSOR_IDENTIFIER
    machine          = "$env:OS $env:PROCESSOR_ARCHITECTURE"
    redis_addr       = $env:GOHARNESS_BENCH_REDIS_ADDR
}
# UTF-8 without BOM, consistent with the benchmark output file.
$manifestFile = Join-Path $outDir 'manifest.json'
[IO.File]::WriteAllText($manifestFile, ($manifest | ConvertTo-Json) + "`n", (New-Object Text.UTF8Encoding $false))

Write-Host "results -> $outFile" -ForegroundColor Green
Write-Host "manifest -> $manifestFile" -ForegroundColor Green
