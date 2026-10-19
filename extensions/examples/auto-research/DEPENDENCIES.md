# 阶段 01 依赖清单

核对日期：2026-10-17。Go 版本和校验和由 go.mod / go.sum 固定；前端由 web/package-lock.json 固定。以下记录当前 Go 运行依赖，以及前端直接依赖的许可；Go 许可已读取下载模块根目录 LICENSE，前端许可按锁文件声明记录。

## Go

| 模块 | 版本 | 许可 |
| --- | --- | --- |
| modernc.org/sqlite | v1.36.3 | BSD-3-Clause |
| modernc.org/libc | v1.61.13 | BSD-3-Clause；模块内第三方代码另有声明 |
| modernc.org/mathutil | v1.7.1 | BSD-3-Clause |
| modernc.org/memory | v1.8.2 | BSD-3-Clause |
| github.com/dustin/go-humanize | v1.0.1 | MIT |
| github.com/google/uuid | v1.6.0 | BSD-3-Clause |
| github.com/mattn/go-isatty | v0.0.20 | MIT |
| github.com/ncruces/go-strftime | v0.1.9 | MIT |
| github.com/remyoudompheng/bigfft | v0.0.0-20230129092748-24d4a6f8daec | BSD-3-Clause |
| golang.org/x/exp | v0.0.0-20230315142452-642cacee5cc0 | BSD-3-Clause |
| golang.org/x/sys | v0.31.0 | BSD-3-Clause |

goHarness 与 extensions 目前只保留本地 replace，尚未导入或链接；后续阶段引入时同步清单。Go 工具和驱动自身测试所需的额外模块由 `go list -m all` 列出，不全都链接进应用。

## 前端直接依赖

| 包 | 版本 | 许可 | 用途 |
| --- | --- | --- | --- |
| react | 19.2.9 | MIT | 运行 |
| react-dom | 19.2.9 | MIT | 运行 |
| react-router-dom | 7.18.4 | MIT | 运行 |
| @types/node | 22.19.15 | MIT | 开发 |
| @types/react | 19.2.14 | MIT | 开发 |
| @types/react-dom | 19.2.3 | MIT | 开发 |
| typescript | 5.9.3 | Apache-2.0 | 构建 |
| vite | 7.3.7 | MIT | 开发、构建 |
| vitest | 4.1.11 | MIT | 测试 |
| @playwright/test | 1.64.1 | Apache-2.0 | 浏览器测试 |

前端所有传递依赖及可选平台包的版本和 license 字段见 [package-lock.json](web/package-lock.json)。本次核对锁文件没有缺失 license 声明的依赖；声明包含 MIT、Apache-2.0、ISC、BSD-3-Clause。实际分发时保留各依赖原始许可及第三方声明，本清单不代替许可证原文。
