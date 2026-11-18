# WorldQuant Alpha 挖掘系统

基于 LLM 的 WorldQuant Brain Alpha 因子挖掘工具：自动生成表达式、提交模拟、筛选结果，并通过 SQLite、飞书机器人和可选的自动任务完成日常运维。

> 当前推荐运行方式：本地 Python 直接运行，不依赖 Docker。
> 项目内保留了 `requirements.txt` 和部分历史脚本，实际运行入口以本文命令为准。

## 功能概览

- **双 LLM 模式**：支持 OpenAI 兼容接口和本地 Ollama。
- **延迟 0 / 延迟 1 双字段池**：按概率混合挖掘 `delay=0` 与 `delay=1` 因子。
- **动态字段采样**：使用 Log + MinMax + Softmax 平衡数据集和字段选择。
- **多种生成策略**：新因子生成、共享池基因重组、边界因子挽救。
- **严格筛选与参数调优**：达到阈值后继续检查 checks，失败时尝试多组参数组合。
- **反向因子检测**：Sharpe 明显为负时自动翻转表达式重测。
- **SQLite 持久化**：保存因子结果、状态、checks 和挽救池。
- **飞书通知**：发现 Alpha、相关性检查、异常时推送消息。
- **飞书机器人**：支持查询、启停、检查和提交，带发送者白名单。
- **可选自动任务**：自动相关性检查和自动提交，由飞书机器人进程启动。

## 处理流程

```text
fetch_fields.py
      │
      ▼
data/fields_delay0, data/fields_delay1
      │
      ▼
run_alpha_miner.py ──► LLM 生成表达式
      │                       │
      │                       ▼
      │              WorldQuant Brain 模拟
      │                       │
      ▼                       ▼
  动态字段/权重 ◄──── 结果筛选、参数调优、挽救
                              │
                              ▼
                       data/alphas.db
                              │
              ┌───────────────┴───────────────┐
              ▼                               ▼
    check_correlation.py               submit_alpha.py
       相关性检查                          手动提交
              │                               │
              └───────────────┬───────────────┘
                              ▼
                     飞书通知 / 飞书机器人
```

## 系统要求

- Python **3.11+**
- WorldQuant Brain 账号
- 以下任一种 LLM 服务：
  - OpenAI 兼容 API（配置 `LLM_API_KEY` / `LLM_BASE_URL` / `LLM_MODEL`）
  - 本地 Ollama（配置 `OLLAMA_URL` / `OLLAMA_MODEL`）
- 可选：飞书自定义机器人 Webhook、飞书应用（用于机器人指令控制）

## 安装

### 方式一：使用已有 Python 环境

```bash
pip install requests openai python-dotenv pandas numpy schedule lark-oapi
```

如果只运行挖掘器、不运行飞书机器人，可以暂不安装 `lark-oapi`。

### 方式二：使用 uv

仓库包含 `pyproject.toml`。如果你使用 uv：

```bash
uv sync
```

`uv sync` 会根据 `pyproject.toml` 解析依赖，并生成 `uv.lock`。`uv.lock` 是依赖锁定文件，用于固定版本；一旦生成，**不建议随意删除**。当前目录中尚未保留 `uv.lock`，所以首次运行 uv 时会重新生成。

> `requirements.txt` 是历史文件，当前依赖声明以 `pyproject.toml` 和实际环境为准。

## 配置

复制示例文件：

```bash
cp .env.example .env
```

### WorldQuant Brain

```env
WQ_USERNAME=your_email@example.com
WQ_PASSWORD=your_password
```

### LLM

代码实际读取的是 `LLM_*`，不是旧版 README 中的 `DEEPSEEK_API_KEY`。

```env
LLM_API_KEY=your_api_key
LLM_BASE_URL=https://your-llm-endpoint/v1
LLM_MODEL=your_model_name
```

本地 Ollama：

```env
OLLAMA_URL=http://localhost:11434
OLLAMA_MODEL=qwen3:8b
```

`--llm auto` 的规则是：配置了 `LLM_API_KEY` 时使用远程接口，否则使用 Ollama。

### 飞书通知（可选）

```env
FEISHU_WEBHOOK=https://open.feishu.cn/open-apis/bot/v2/hook/xxx
```

不配置则关闭通知。

### 飞书机器人（可选）

```env
FEISHU_APP_ID=cli_xxx
FEISHU_APP_SECRET=xxx
FEISHU_ALLOWED_OPEN_IDS=ou_xxxxxxxxxxxxxxxxx
```

多个用户用英文逗号分隔：

```env
FEISHU_ALLOWED_OPEN_IDS=ou_aaa,ou_bbb
```

**未配置白名单时，机器人会拒绝所有指令。** 白名单是安全边界，不要把整个群或无关人员加入。

### 自动任务（可选）

自动任务由 `feishu_bot.py` 启动，配置后可按计划检查或提交：

```env
# 每天 4 次：0/6/12/18 点
AUTO_CHECK_SCHEDULE=4
AUTO_CHECK_BATCH_SIZE=20

# 每天 14:30 提交一次
AUTO_SUBMIT_SCHEDULE=14:30
AUTO_SUBMIT_MAX_PER_DAY=4
AUTO_SUBMIT_MIN_PER_DAY=1
AUTO_SUBMIT_MIN_FITNESS=1.2
```

`AUTO_CHECK_SCHEDULE` / `AUTO_SUBMIT_SCHEDULE` 支持两种写法：

- 整数 `1` 到 `24`：按一天平均分配，例如 `4` 表示每天 4 次。
- `HH:MM` 或逗号分隔的时间列表：例如 `09:00,15:00`。

自动提交会真实调用平台提交接口，请确认配额和策略后再开启。

## 准备字段数据

首次运行前，先拉取字段和运算符：

```bash
# 延迟 1 字段，默认
python fetch_fields.py --delay 1

# 延迟 0 字段
python fetch_fields.py --delay 0
```

输出位置：

```text
data/
├── fields_delay1/        # delay=1 字段 CSV
├── fields_delay0/        # delay=0 字段 CSV
└── operators/
    └── operators.csv     # 运算符
```

程序会加载目录中所有包含 `Field` 和 `Description` 列的 CSV。当前仓库实际已有 14 个 delay1 数据集和 11 个 delay0 数据集；如果目录为空，挖掘器没有有效字段可用。

## 启动挖掘

```bash
# 推荐入口
python run_alpha_miner.py --llm auto --workers 2 --member-id default

# 使用 Ollama
python run_alpha_miner.py --llm ollama --workers 2

# 使用远程 OpenAI 兼容接口
python run_alpha_miner.py --llm deepseek --workers 2
```

参数说明：

| 参数 | 默认值 | 说明 |
|---|---:|---|
| `--llm` | `auto` | `auto` / `ollama` / `deepseek` |
| `--workers` | `2` | 并发模拟数，建议不超过 2 |
| `--member-id` | `default` | 共享池成员标识，团队协作时使用 |
| `--delay0-prob` | `0.4` | 使用 delay=0 字段的概率 |
| `--username` | 空 | 临时覆盖 `.env` 中的 WQ 用户名 |
| `--password` | 空 | 临时覆盖 `.env` 中的 WQ 密码 |

`--llm deepseek` 会映射到远程 `LLM_*` 配置；是否真的是 DeepSeek 取决于 `LLM_BASE_URL` 和 `LLM_MODEL`。

## 结果处理规则

### 生成策略

生产者线程大致按以下比例选择任务：

- **60%** 新因子生成
- **20%** 从共享池基因重组
- **20%** 从挽救池修复边界因子

### 筛选条件

| 条件 | 动作 |
|---|---|
| Sharpe ≥ 1.25 且 Fitness ≥ 1.0 且 checks 全部通过 | 写入数据库，状态 `pending` |
| 达到阈值但 checks 失败 | 参数调优；可修复的失败进入挽救池 |
| Sharpe > 1.0 且 Fitness > 0.8 | 加入共享池 |
| Sharpe < -0.8 | 翻转表达式后重测 |
| `abs(Sharpe) + abs(Fitness) > 1.7` | 进入挽救池，最多尝试 3 次 |

挽救池主要处理换手率、回撤、参数设置等问题；`SELF_CORRELATION` 等核心逻辑问题通常不适合挽救。

## 相关性检查

```bash
# 检查 pending 因子；默认删除 SELF_CORRELATION FAIL
python check_correlation.py

# 只检查，不修改数据库
python check_correlation.py --dry-run

# 保留失败因子
python check_correlation.py --keep-fail

# 只检查前 20 个
python check_correlation.py --limit 20

# 指定 alpha ID
python check_correlation.py --alpha-ids id1,id2

# 不发送飞书通知
python check_correlation.py --no-notify

# 输出 JSON，供外部脚本聚合
python check_correlation.py --batch-mode
```

也可以通过飞书机器人发送 `/check` 或 `/auto_check on`。

## 提交因子

```bash
# 提交单个或多个因子
python submit_alpha.py <alpha_id>
python submit_alpha.py id1 id2

# 保留平台检查失败的因子，不从本地数据库删除
python submit_alpha.py --keep-failed id1

# 指定提交类型
python submit_alpha.py --type REGULAR id1
```

默认行为是：平台明确返回 checks 失败时，从本地数据库删除该记录。`--keep-failed` 可以关闭这个行为。

## 辅助工具

从 WorldQuant Brain 补录已有因子到本地数据库：

```bash
python add_alpha.py <alpha_id>
python add_alpha.py id1 id2
```

## 飞书机器人

### 启动

```bash
python feishu_bot.py
```

机器人使用飞书 **EventStream 长连接**，不开放 HTTP 端口，因此不需要配置 Webhook 回调地址，也不需要 `--port` 参数。

在飞书开放平台：

1. 应用 → 事件与回调 → 接收方式选择“长连接”
2. 订阅事件 `im.message.receive_v1`
3. 配置 `FEISHU_APP_ID`、`FEISHU_APP_SECRET`
4. 配置 `FEISHU_ALLOWED_OPEN_IDS`

### 命令

| 命令 | 说明 | 示例 |
|---|---|---|
| `/summary` | 查看因子库统计和矿机状态 | `/summary` |
| `/start [workers]` | 启动挖掘器 | `/start 2` |
| `/stop` | 停止挖掘器 | `/stop` |
| `/check` | 执行相关性检查 | `/check` |
| `/submit <id> [...]` | 提交指定因子 | `/submit id1 id2` |
| `/list [数量] [状态/grade]` | 查看列表或单个因子详情 | `/list 50 submitted` |
| `/auto_check [on/off]` | 查看或切换自动检查 | `/auto_check on` |
| `/auto_submit [on/off]` | 查看或切换自动提交 | `/auto_submit off` |
| `/help` | 显示帮助 | `/help` |

机器人只接受 `FEISHU_ALLOWED_OPEN_IDS` 中的发送者；私聊和群聊都适用。未授权指令会记录发送者 `open_id` 到本地日志，但不会执行。

## 项目结构

```text
WorldQuant/
├── core/                       # 核心模块
│   ├── api_session.py          # WQ API 会话
│   ├── config.py               # 凭据和通用配置
│   ├── alpha_db.py             # SQLite 数据库
│   ├── llm_client.py           # LLM 客户端
│   ├── data_fetcher.py         # 字段和运算符拉取
│   ├── notifier.py             # 飞书通知
│   ├── feishu_client.py        # 飞书 API 客户端
│   ├── submission_quota.py     # 提交配额
│   └── log_manager.py          # 日志管理
├── data/
│   ├── fields_delay1/          # delay=1 字段
│   ├── fields_delay0/          # delay=0 字段
│   ├── operators/              # 运算符
│   ├── shared_pool/            # 共享因子池
│   ├── failed_patterns.json    # 失败模式黑名单
│   └── alphas.db               # SQLite 主库
├── log/                        # 运行日志
├── backup/                     # 历史备份和手工测试脚本，非运行入口
├── run_alpha_miner.py          # 主挖掘程序
├── check_correlation.py        # 相关性检查
├── submit_alpha.py             # 手动提交
├── add_alpha.py                # 从平台补录因子
├── fetch_fields.py             # 拉取字段和运算符
├── feishu_bot.py               # 飞书机器人及自动任务入口
├── pyproject.toml              # 项目依赖声明
└── .env.example                # 环境变量示例
```

`backup/` 中的脚本是历史版本或手工测试工具，不建议作为日常入口；`mine.sh`、`deploy.sh`、`docker-compose.yml` 等部署脚本也属于辅助文件，当前以本文的 Python 直接运行方式为准。

## 数据库

### `alphas`

保存通过筛选的因子，主键为 `alpha_id`。主要状态：

- `pending`：已入库，等待相关性检查
- `unsubmitted`：通过检查，可提交
- `submitted`：已成功提交
- `tested` / `error`：历史或异常记录，可能不会被自动流程处理

### `rescue_pool`

保存边界因子：

- `attempt_count`：已尝试次数，达到 3 次后由清理逻辑删除
- `failed_checks`：失败检查项，用于生成针对性变种
- `modules_used`：生成时使用的模块
- `created_at`：首次进入挽救池的时间

## 日志

日志位于 `log/`：

- `YYYY-MM-DD.log`：通用运行日志
- `miner-YYYY-MM-DD.log`：挖掘器日志

日志不会自动按 30 天清理；历史日志可以手动归档或删除，建议保留最近一段时间的日志用于排查。

## 常见问题

### 认证失败

确认 `.env` 中 `WQ_USERNAME` / `WQ_PASSWORD` 正确，并检查网络和代理设置。系统会自动重新认证。

### LLM 报错或一直返回空结果

检查 `LLM_API_KEY`、`LLM_BASE_URL`、`LLM_MODEL` 是否完整；如果使用 Ollama，先确认：

```bash
ollama serve
ollama list
```

### 字段为空或启动后没有生成因子

检查：

```bash
ls data/fields_delay1/*.csv
ls data/fields_delay0/*.csv
```

至少需要一套有效的字段 CSV。需要重新拉取时执行：

```bash
python fetch_fields.py --delay 1
python fetch_fields.py --delay 0
```

### 飞书机器人回复“无权限”

说明发送者 `open_id` 不在白名单中。查看本地日志中的 `sender_open_id`，然后加入：

```env
FEISHU_ALLOWED_OPEN_IDS=ou_xxxxxxxxxxxxxxxxx
```

修改后重启 `feishu_bot.py`。

### 相关性检查与定时任务冲突

当前建议统一使用飞书机器人的 `/auto_check`，或只保留一种外部定时方式。`setup_cron.sh` 是历史脚本，其中使用了旧参数，直接使用前需要自行核对 `check_correlation.py` 的实际参数。

## 安全注意事项

- `.env` 包含账号和 API 凭据，不要提交到版本库或发给他人。
- 飞书机器人必须配置 `FEISHU_ALLOWED_OPEN_IDS`，否则所有指令都会被拒绝。
- 程序默认校验证书；不要通过关闭 TLS 校验来绕过网络问题。
- 提交是不可逆操作，建议先使用 `--dry-run` 或手动检查，再执行真实提交。

## License

MIT License
