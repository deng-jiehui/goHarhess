# Benchmark output completeness validator (plan 2.11).
# Owns the expected scenario list per profile; bench.ps1 calls this after
# collecting raw output. Exits non-zero when any expected scenario is
# missing, undersampled, or the output contains FAIL.
#
#   ./scripts/bench-validate.ps1 -Profile Full -Count 1 -OutputFile out.txt
[CmdletBinding()]
param(
    [ValidateSet('Smoke', 'Full', 'Stress')]
    [string]$Profile = 'Smoke',
    [int]$Count = 1,
    [int]$Cpu = 1,
    [Parameter(Mandatory = $true)][string]$OutputFile,
    [switch]$Redis
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# 每个 Profile 的完整预期场景清单（叶子子基准名）。验收规则：每个
# 场景必须出现且恰好有 Count 条含 ns/op 的结果行——缺包、缺场景、
# 少采样或输出被替换都必须失败。新增场景同步加入此表。
$expectedScenarios = @{
    Smoke = @(
        'BenchmarkGHSmoke/MessageClone/messages=100/kind=text',
        'BenchmarkGHSmoke/MessageClone/messages=100/kind=toolmeta',
        'BenchmarkGHSmoke/ContextTrim/DropOld/turns=100/kind=tool',
        'BenchmarkGHSmoke/ToolBatch/calls=8/turns=100/history=off',
        'BenchmarkGHSmoke/SequentialGraph/nodes=16',
        'BenchmarkGHSmoke/ParallelGraph/entry=forkjoin/branches=8/state_bytes=1024',
        'BenchmarkGHSmoke/PersistedGraph/nodes=16/state_bytes=4096/checkpoint=async',
        'BenchmarkGHSmoke/ReAct/model_calls=10/checkpoint=off',
        'BenchmarkGHSmoke/ResumeAgent/tool_suspend/checkpoint=sync',
        'BenchmarkGHSmoke/ResumeAgent/tool_suspend/checkpoint=async',
        'BenchmarkGHSmoke/CheckpointCodec/payload_bytes=65536/clone',
        'BenchmarkGHSmoke/CheckpointCodec/payload_bytes=65536/json',
        'BenchmarkGHSmoke/CheckpointCodec/payload_bytes=65536/decode',
        'BenchmarkGHSmoke/MemoryStore/load_history=100'
    )
    Full = @(
        'BenchmarkGHFull/MessageClone/messages=10/kind=text',
        'BenchmarkGHFull/MessageClone/messages=10/kind=toolmeta',
        'BenchmarkGHFull/MessageClone/messages=100/kind=text',
        'BenchmarkGHFull/MessageClone/messages=100/kind=toolmeta',
        'BenchmarkGHFull/MessageClone/messages=1000/kind=text',
        'BenchmarkGHFull/MessageClone/messages=1000/kind=toolmeta',
        'BenchmarkMessageTransport/delta',
        'BenchmarkMessageTransport/snapshot',
        'BenchmarkGHFull/ContextTrim/NoTrim/turns=10',
        'BenchmarkGHFull/ContextTrim/NoTrim/turns=100',
        'BenchmarkGHFull/ContextTrim/NoTrim/turns=1000',
        'BenchmarkGHFull/ContextTrim/DropOld/turns=100/kind=text',
        'BenchmarkGHFull/ContextTrim/DropOld/turns=100/kind=tool',
        'BenchmarkGHFull/ContextTrim/DropOld/turns=1000/kind=text',
        'BenchmarkGHFull/ContextTrim/DropOld/turns=1000/kind=tool',
        'BenchmarkGHFull/ContextTrim/DropOld/turns=100/kind=cjk',
        'BenchmarkGHFull/ContextTrim/OversizedLatest/input_bytes=65536',
        'BenchmarkGHFull/ToolBatch/calls=1/turns=100/history=off',
        'BenchmarkGHFull/ToolBatch/calls=1/turns=100/history=on',
        'BenchmarkGHFull/ToolBatch/calls=8/turns=100/history=off',
        'BenchmarkGHFull/ToolBatch/calls=8/turns=100/history=on',
        'BenchmarkGHFull/ToolBatch/calls=32/turns=100/history=off',
        'BenchmarkGHFull/ToolBatch/calls=32/turns=100/history=on',
        'BenchmarkGHFull/ToolBatch/calls=8/turns=100/history=mixed',
        'BenchmarkGHFull/ToolBatch/calls=8/turns=10/history=off',
        'BenchmarkGHFull/ToolBatch/calls=8/turns=1000/history=off',
        'BenchmarkGHFull/ToolCallbacks/calls=8/callbacks=0/history=off',
        'BenchmarkGHFull/ToolCallbacks/calls=8/callbacks=0/history=on',
        'BenchmarkGHFull/ToolCallbacks/calls=8/callbacks=1/history=off',
        'BenchmarkGHFull/ToolCallbacks/calls=8/callbacks=1/history=on',
        'BenchmarkGHFull/ToolCallbacks/calls=8/callbacks=4/history=off',
        'BenchmarkGHFull/ToolCallbacks/calls=8/callbacks=4/history=on',
        'BenchmarkGHFull/SequentialGraph/nodes=1',
        'BenchmarkGHFull/SequentialGraph/nodes=10',
        'BenchmarkGHFull/SequentialGraph/nodes=100',
        'BenchmarkGHFull/SequentialGraph/build_compile/nodes=10',
        'BenchmarkGHFull/SequentialGraph/build_compile/nodes=100',
        'BenchmarkGHFull/SequentialGraph/compile_only/nodes=100',
        'BenchmarkGHFull/ParallelGraph/entry=forkjoin/branches=2/state_bytes=1024',
        'BenchmarkGHFull/ParallelGraph/entry=forkjoin/branches=8/state_bytes=1024',
        'BenchmarkGHFull/ParallelGraph/entry=forkjoin/branches=32/state_bytes=1024',
        'BenchmarkGHFull/ParallelGraph/entry=parallelnode/branches=2/state_bytes=1024',
        'BenchmarkGHFull/ParallelGraph/entry=parallelnode/branches=8/state_bytes=1024',
        'BenchmarkGHFull/ParallelGraph/entry=parallelnode/branches=32/state_bytes=1024',
        'BenchmarkGHFull/ParallelGraph/entry=forkjoin/branches=8/state_bytes=65536',
        'BenchmarkGHFull/ParallelGraph/entry=parallelnode/branches=8/state_bytes=65536',
        'BenchmarkGHFull/ParallelGraph/entry=forkjoin/branches=8/max_concurrency=1',
        'BenchmarkGHFull/ParallelGraph/entry=forkjoin/branches=8/max_concurrency=8',
        'BenchmarkGHFull/ParallelGraph/entry=parallelnode/branches=8/max_concurrency=1',
        'BenchmarkGHFull/ParallelGraph/entry=parallelnode/branches=8/max_concurrency=8',
        'BenchmarkGHFull/PersistedGraph/nodes=16/state_bytes=4096/checkpoint=off',
        'BenchmarkGHFull/PersistedGraph/nodes=16/state_bytes=4096/checkpoint=sync',
        'BenchmarkGHFull/PersistedGraph/nodes=16/state_bytes=4096/checkpoint=async',
        'BenchmarkGHFull/PersistedGraph/nodes=16/state_bytes=65536/checkpoint=off',
        'BenchmarkGHFull/PersistedGraph/nodes=16/state_bytes=65536/checkpoint=sync',
        'BenchmarkGHFull/PersistedGraph/nodes=16/state_bytes=65536/checkpoint=async',
        'BenchmarkGHFull/ResumeGraph/sequential_suspend/checkpoint=sync',
        'BenchmarkGHFull/ResumeGraph/sequential_suspend/checkpoint=async',
        'BenchmarkGHFull/ResumeGraph/parallel_suspend/checkpoint=sync',
        'BenchmarkGHFull/ResumeGraph/parallel_suspend/checkpoint=async',
        'BenchmarkGHFull/ResumeGraph/completed_result/checkpoint=sync',
        'BenchmarkGHFull/ResumeGraph/completed_result/checkpoint=async',
        'BenchmarkGHFull/ReAct/model_calls=10/checkpoint=off',
        'BenchmarkGHFull/ReAct/model_calls=50/checkpoint=off',
        'BenchmarkGHFull/ReAct/model_calls=100/checkpoint=off',
        'BenchmarkGHFull/ReAct/model_calls=50/history=off',
        'BenchmarkGHFull/ReAct/model_calls=50/history=on',
        'BenchmarkGHFull/AgentPersist/model_calls=10/checkpoint=off',
        'BenchmarkGHFull/AgentPersist/model_calls=10/checkpoint=sync',
        'BenchmarkGHFull/AgentPersist/model_calls=10/checkpoint=async',
        'BenchmarkGHFull/AgentPersist/model_calls=50/checkpoint=off',
        'BenchmarkGHFull/AgentPersist/model_calls=50/checkpoint=sync',
        'BenchmarkGHFull/AgentPersist/model_calls=50/checkpoint=async',
        'BenchmarkGHFull/SessionGrowth/requests=10',
        'BenchmarkGHFull/SessionGrowth/requests=100',
        'BenchmarkGHFull/ResumeAgent/tool_suspend/checkpoint=sync',
        'BenchmarkGHFull/ResumeAgent/tool_suspend/checkpoint=async',
        'BenchmarkGHFull/AgentStream/mode=invoke/text_bytes=16384',
        'BenchmarkGHFull/AgentStream/mode=stream/text_bytes=16384',
        'BenchmarkGHFull/AgentStream/mode=events/text_bytes=16384',
        'BenchmarkGHFull/AgentStream/mode=events_lifecycle/text_bytes=16384',
        'BenchmarkGHFull/CheckpointCodec/payload_bytes=4096/clone',
        'BenchmarkGHFull/CheckpointCodec/payload_bytes=4096/json',
        'BenchmarkGHFull/CheckpointCodec/payload_bytes=4096/decode',
        'BenchmarkGHFull/CheckpointCodec/payload_bytes=65536/clone',
        'BenchmarkGHFull/CheckpointCodec/payload_bytes=65536/json',
        'BenchmarkGHFull/CheckpointCodec/payload_bytes=65536/decode',
        'BenchmarkGHFull/CheckpointCodec/payload_bytes=524288/clone',
        'BenchmarkGHFull/CheckpointCodec/payload_bytes=524288/json',
        'BenchmarkGHFull/CheckpointCodec/payload_bytes=524288/decode',
        'BenchmarkGHFull/MemoryStore/save_empty',
        'BenchmarkGHFull/MemoryStore/save_history=100',
        'BenchmarkGHFull/MemoryStore/load_history=100',
        'BenchmarkGHFull/MemoryStore/get_oldest/history=100',
        'BenchmarkGHFull/MemoryStore/get_oldest/history=1000',
        'BenchmarkGHFull/MemoryStore/list_latest/history=100/limit=10',
        'BenchmarkGHFull/MemoryStore/list_latest/history=100/limit=100',
        'BenchmarkGHFull/MemoryStore/list_latest/history=1000/limit=10',
        'BenchmarkGHFull/MemoryStore/list_latest/history=1000/limit=100'
    )
    Stress = @(
        'BenchmarkGHStress/MessageClone/messages=1/media_bytes=1048576',
        'BenchmarkGHStress/ContextTrim/DropOld/turns=1000/kind=tool',
        'BenchmarkGHStress/ToolBatch/calls=32/turns=1000/history=on',
        'BenchmarkGHStress/ParallelGraph/entry=parallelnode/branches=8/state_bytes=1048576',
        'BenchmarkGHStress/SessionGrowth/requests=1000',
        'BenchmarkGHStress/AgentStream/mode=events/text_bytes=65536'
    )
    Redis = @(
        'BenchmarkGHRedis/RedisStore/save_payload_bytes=4096',
        'BenchmarkGHRedis/RedisStore/save_payload_bytes=65536',
        'BenchmarkGHRedis/RedisStore/save_payload_bytes=524288',
        'BenchmarkGHRedis/RedisStore/load_payload_bytes=4096',
        'BenchmarkGHRedis/RedisStore/load_payload_bytes=65536',
        'BenchmarkGHRedis/RedisStore/load_payload_bytes=524288',
        'BenchmarkGHRedis/RedisStore/list/history=100/limit=10',
        'BenchmarkGHRedis/RedisStore/list/history=100/limit=100',
        'BenchmarkGHRedis/RedisStore/list/history=1000/limit=10',
        'BenchmarkGHRedis/RedisStore/list/history=1000/limit=100',
        'BenchmarkGHRedis/RedisGraphRun/nodes=16/checkpoint=sync',
        'BenchmarkGHRedis/RedisGraphRun/nodes=16/checkpoint=async'
    )
}
$wantScenarios = if ($Redis) { $expectedScenarios.Redis } else { $expectedScenarios[$Profile] }

# Go 在 -cpu>1 时给完整场景名追加 -N 后缀（cpu=1 无后缀），
# 校验时必须带上与采样相同的后缀，否则误判缺失。
$cpuSuffix = if ($Cpu -gt 1) { '-' + $Cpu } else { '' }
$lines = Get-Content $OutputFile
$bad = [System.Collections.Generic.List[string]]::new()
foreach ($name in $wantScenarios) {
    $pattern = '^' + [regex]::Escape($name) + [regex]::Escape($cpuSuffix) + '\s'
    $samples = @($lines | Where-Object { $_ -match $pattern -and $_ -match 'ns/op' })
    if ($samples.Count -ne $Count) {
        $bad.Add("$name : samples=$($samples.Count), want=$Count")
    }
}
if ($bad.Count -gt 0) {
    [Console]::Error.WriteLine("incomplete benchmark coverage in ${OutputFile}:`n" + ($bad -join "`n"))
    exit 1
}
if (@($lines | Where-Object { $_ -match 'FAIL' }).Count -gt 0) {
    [Console]::Error.WriteLine("benchmark output contains FAIL (see $OutputFile)")
    exit 1
}
Write-Output "coverage OK: $($wantScenarios.Count) scenarios x $Count sample(s) in $OutputFile"
