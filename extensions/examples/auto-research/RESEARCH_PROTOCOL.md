# 研究协议

## 研究目标

本项目回答一个可控制的问题：

> 在 2021 至 2026 年公开的软件工程论文中，LLM Agent 用于自动化代码缺陷修复时采用了哪些工具调用方式、数据集和评测指标？不同方法的主要局限是什么？

这个问题比“自动研究任何主题”更适合学生项目：领域边界清楚，公开论文足够多，结果可以人工抽查，也能展示检索、证据抽取、并行处理、检查点恢复和引用校验。对其中有公开代码和可用环境的论文，项目还可以在用户授权的实验室服务器上执行受控复现，并把实验结果与论文指标进行比较。

## 用户、Agent 能力与执行工具

| 组成 | 任务 |
| --- | --- |
| 学生用户 | 输入问题、年份和论文数量，确认候选论文，检查最终报告 |
| AutoResearch：规划能力 | 生成检索式、关键词、排除规则和字段模板 |
| Source Adapter | 调用 OpenAlex / arXiv，保存原始来源和时间 |
| AutoResearch：证据分析能力 | 从单篇论文中抽取结构化证据和原文片段 |
| AutoResearch：综合能力 | 根据证据矩阵形成主题、比较和局限 |
| AutoResearch：复核能力 | 检查结论是否有引用、是否超出证据、是否存在冲突 |
| Runner | 在用户授权的服务器上执行已确认的复现计划并收集产物 |

以上规划、分析、综合和复核均由同一个研究 Agent 在会话中完成；阶段模板描述可复用的任务步骤，不要求用户只能按固定流水线使用产品。

## 用户故事

学生运行：

```powershell
# 计划中的 CLI 离线示例，在本项目根目录运行。
go run . fixture --question "LLM Agent 如何用于软件工程缺陷修复？" --year-from 2021 --year-to 2026 --analyze-limit 12
```

系统先展示：

```text
研究问题：LLM Agent 如何用于软件工程缺陷修复？
来源：OpenAlex, arXiv
年份：2021-2026
建议查询式：
  1. "LLM agent" AND "automated program repair"
  2. "language model" AND "software bug fixing" AND tool
  3. "agentic" AND "code repair" AND benchmark
候选论文：30 篇，建议分析 12 篇
是否继续？[yes/no]
```

用户确认后，系统逐篇分析并生成：

```text
报告摘要
1. 工具调用方式：编辑器补丁、测试执行、仓库搜索是最常见组合。
2. 数据集：多数研究使用 Defects4J、SWE-bench 或自建仓库。
3. 评测：修复成功率之外，常报告测试通过率、补丁正确性和成本。
4. 局限：数据集泄漏、测试不足和真实工程成本报告不充分。

每条结论后附 [P03][P08] 等论文引用。
```

## 数据契约

每篇论文至少保存：

- `paper_id`：OpenAlex ID、arXiv ID 或 DOI；
- 标题、作者、年份、摘要、来源 URL；
- `retrieved_at` 和来源查询；
- 开放全文的可用状态；
- 证据字段：`problem`、`method`、`tools`、`dataset`、`metrics`、`result`、`limitation`；
- 原文 `quote`、段落或页码定位、抽取置信度。
- 若执行复现，额外保存服务器 ID、代码 commit、数据版本、环境指纹、命令、资源、远程 job ID、指标和日志产物。

没有 `paper_id` 的文字不能成为最终引用，没有 `quote` 的模型推断只能列在“待核验”中。

## 流程状态

```text
planned
  → searched
  → candidates_ready
  → waiting_user_review
  → selected
  → fetched
  → evidence_extracted
  → synthesized
  → critiqued
  → completed
```

如果用户启用论文复现，`selected` 之后增加：

```text
selected → plan_ready → awaiting_approval → running → comparing → completed
```

任一阶段失败都保存 `failed_stage` 和错误列表。可恢复阶段从最近的成功检查点开始；同一 `run_id` 下已保存的来源和证据不重复写入。

## 研究范围限制

- 只收公开元数据、摘要和明确开放的全文；不处理付费墙；
- 第一版只支持英文论文和文本内容；
- 第一版最多分析 12 篇论文，避免成本和上下文失控；
- 第一版输出研究辅助报告，不自动声称达到系统综述的完整性标准；
- 结果必须注明搜索日期、来源范围和未访问的数据库。
- 复现只能使用用户登记的服务器和项目隔离目录；没有用户确认的计划不能提交远程任务。

## 成功标准

当用户可以从报告中的任意一条结论回到 `paper_id → evidence_id → quote → source URL`，并且一次网络失败可以恢复而不重复整条流程时，论文研究部分达到演示目标。启用复现后，还应能从一次比较结论回到 `paper_id → ExperimentRun → commit / command / metric / artifact`。
