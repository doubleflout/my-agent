# Agent

心有灵犀 Agent 是一个面向长期陪伴和自主执行场景的个人 Agent 系统。它不仅能处理被动对话，还支持长期记忆、插件扩展、工具按需暴露、定时任务、主动消息、后台 Drift 任务和多 Subagent 协作。

项目重点不只是“调用一次大模型”，而是把 Agent 运行时拆成可观察、可扩展的后端系统。

## 核心能力

- **被动消息处理**：基于生命周期 Phase Pipeline 编排 BeforeTurn、BeforeReasoning、Reasoning、AfterReasoning、AfterTurn 等阶段，支持 ReAct 工具循环、上下文裁剪重试和会话持久化。
- **长期记忆系统**：同时维护 Markdown 人类可读记忆和 SQLite + embedding 语义记忆库，支持 consolidation、向量检索、重复强化、规则 supersede 和 prompt 注入。
- **插件系统**：插件可注册 PhaseModule、工具、事件监听器和工具拦截器，在不改主循环的情况下扩展 Agent 行为。
- **工具按需暴露**：通过 tool_search 动态暴露工具 schema，降低大模型上下文成本。
- **定时任务**：支持 at / after / every 三类触发方式，every 支持 interval 和 cron 表达式；任务可持久化、恢复、取消。
- **主动消息**：通过 proactive loop 聚合 alert、content、context 多源数据，结合 presence、去重 ACK 和长期记忆判断是否推送。
- **后台 Drift 任务**：空闲时基于 Skill 定义的目标和流程执行后台任务，支持文件读写、记忆检索、网页抓取、Shell 和 MCP 工具。
- **Subagent**：主 Agent 可将复杂子任务委托给同步或后台 Subagent，按 profile 隔离工具权限，降低主上下文膨胀。
- **评测与 Trace**：LongMemEval / PersonaMem 评测链路覆盖 ingest、QA、评分与 trace 记录，便于定位长期记忆任务中的错误来源。

## Quickstart

需要 Python 3.12。

```bash
git clone <this-repo>
cd akashic-agent
uv venv
uv pip install -r requirements.txt
```

没有 `uv` 的话，可以先安装：

```bash
pip install uv
```

初始化工作区：

```bash
uv run python main.py setup
```

也可以使用非交互模式：

```bash
uv run python main.py init
```

复制并填写配置：

```bash
cp config.example.toml config.toml
```

配置至少需要包含：

- 主模型 API 配置
- embedding 模型配置
- 一个消息渠道，例如 Telegram、QQ 或 QQBot
- 可选的 MCP 数据源和主动消息配置

启动：

```bash
uv run python main.py
```

## 系统架构

```text
用户消息
  -> MessageBus 入站队列
  -> AgentLoop
  -> Phase Pipeline
  -> Prompt 组装 / Memory 注入 / ReAct 工具循环
  -> Turn 持久化
  -> MessageBus 出站队列

后台能力
  -> SchedulerService 定时任务
  -> ProactiveLoop 主动消息
  -> DriftRunner 后台 Skill 任务
  -> Memory consolidation / optimizer
```

## 被动对话链路

被动回复以一次 Turn 为单位运行。生命周期模块会围绕同一个 Frame 修改状态：

```text
BeforeTurn
  -> BeforeReasoning
  -> PromptRender
  -> Reasoning
  -> AfterReasoning
  -> AfterTurn
```

这套机制类似后端中的 pipeline / interceptor chain，也类似 LangGraph 中共享 state 的节点编排。每个模块通过 slot、requires、produces 声明依赖，运行前进行拓扑排序。

相关代码：

- `agent/lifecycle/phase.py`
- `agent/lifecycle/phases/`
- `agent/core/passive_turn.py`

## 记忆系统

记忆系统分成两层：

1. **Markdown 记忆层**
   
   - `MEMORY.md`：长期画像和规则
   - `HISTORY.md`：时间线事件
   - `PENDING.md`：待归档候选记忆
   - `RECENT_CONTEXT.md`：近期上下文摘要
2. **memory2 语义记忆层**
   
   - `event`：具体事件
   - `profile`：用户事实和状态
   - `preference`：服务偏好
   - `procedure`：长期执行规则

写入时会结合 `content_hash`、向量相似度、`memory_type`、`extra_json.category`、`tool_requirement` 等信息做重复强化、合并或 supersede。

相关代码：

- `core/memory/markdown.py`
- `memory2/store.py`
- `memory2/memorizer.py`
- `plugins/default_memory/engine.py`

## 插件系统

插件继承基础 Plugin 类，通过约定方法向主系统注册能力：

- PhaseModule：插入生命周期阶段
- Tool：注册可被 LLM 调用的工具
- EventBus handler：监听系统事件
- Tool hook：在工具执行前后做拦截和增强

插件管理器负责扫描插件目录、导入 `plugin.py`、实例化插件并收集模块。主流程只依赖统一接口，不依赖具体插件实现。

相关代码：

- `agent/plugins/base.py`
- `agent/plugins/manager.py`
- `plugins/`
- `_handbook/plugins-tutorial.md`

## 定时任务

定时任务由 `schedule` 工具创建，底层由 `SchedulerService` 每秒 tick 检查到期任务。

支持三种 trigger：

- `at`：指定绝对时间
- `after`：相对延迟
- `every`：周期任务，支持 `1h`、`30m` 和 cron，例如 `0 9 * * *`

支持两种 tier：

- `instant`：到点直接推送固定消息
- `soft`：到点调用 Agent 生成内容，再推送给用户

任务持久化到工作区的 `schedules.json`，服务重启后会自动恢复。适合“每天早上 9 点根据昨天聊天情况做复盘总结”这类任务。

相关代码：

- `agent/scheduler.py`
- `agent/tools/schedule.py`
- `bootstrap/toolsets/schedule.py`

## 主动消息与 Drift

主动消息系统会周期性收集 alert、content、context，根据用户状态、长期记忆、去重记录和模型判断决定是否推送。

当没有合适内容推送时，DriftRunner 可以进入后台任务模式，根据 Skill 中定义的目标、工作文件和流程推进长期任务。

相关代码：

- `proactive_v2/loop.py`
- `proactive_v2/agent_tick.py`
- `proactive_v2/drift_runner.py`
- `_handbook/proactive-guide.md`
- `_handbook/drift-guide.md`

## Subagent

Subagent 用于处理复杂任务中的子问题，避免主 Agent 长工具链阻塞和上下文膨胀。系统支持同步执行、后台执行、任务取消和 profile 级工具权限隔离。

相关代码：

- `agent/subagent.py`
- `agent/background/subagent_manager.py`
- `agent/background/subagent_profiles.py`
- `agent/tools/spawn.py`

## 评测

项目包含 LongMemEval 和 PersonaMem 评测框架，用于验证长期记忆 Agent 的端到端表现。

LongMemEval 链路覆盖：

```text
dataset
  -> ingest
  -> memory consolidation
  -> QA
  -> scoring
  -> trace / result
```

相关代码：

- `eval/longmemeval/`
- `eval/personamem/`
- `plugins/observe/`
- `core/common/strategy_trace.py`

## 常用命令

```bash
uv run python main.py
uv run python main.py cli
uv run python main.py dashboard
uv run python main.py setup
uv run python main.py --help
```

运行测试：

```bash
pytest tests/
```

## 文档

- [_handbook/proactive-guide.md](./_handbook/proactive-guide.md)
- [_handbook/drift-guide.md](./_handbook/drift-guide.md)
- [_handbook/memory-markdown.md](./_handbook/memory-markdown.md)
- [_handbook/plugins-tutorial.md](./_handbook/plugins-tutorial.md)

