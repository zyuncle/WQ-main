"""
飞书机器人指令控制系统（EventStream 长连接版）

通过飞书消息远程控制挖掘器、查询因子库、执行相关性检查、提交因子。

核心约束：
  1. 事件处理器必须在 3 秒内返回，SDK 会自动 ACK；不返回或抛异常会触发飞书重推
  2. event_id 去重必须保留（至少一次投递）
  3. 只能单进程运行（集群模式随机分发）

命令（9 个）：
  /summary                        查看因子库统计
  /start [workers]                启动挖掘器
  /stop                           停止挖掘器
  /check                          手动相关性检查（全量）
  /submit <id> [id2...]           手动提交指定因子
  /list [数量] [状态/grade|alpha_id]  查列表或详情
  /auto_check [on|off]            自动检查：无参数查状态，on 启动，off 停止
  /auto_submit [on|off]           自动提交：无参数查状态，on 启动，off 停止
  /help                           帮助

Usage:
    pip install lark-oapi -U
    python feishu_bot.py

飞书开放平台：
    - 事件与回调 → 清空回调 URL，接收方式选“长连接”
    - 保留事件 im.message.receive_v1
    - 保留权限 im:message（私聊和群聊分别申请 p2p_msg / group_at_msg）
"""

import os
import sys
import re
import time
import signal
import threading
import queue
from datetime import datetime
from collections import OrderedDict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv
load_dotenv()

import lark_oapi as lark
from lark_oapi.api.im.v1 import P2ImMessageReceiveV1
from core.alpha_db import get_alpha_db
from core.feishu_client import get_feishu_client
from core.log_manager import setup_logger

from auto_tasks import (
    init_auto_tasks,
    shutdown_auto_tasks,
    get_scheduler,
    get_check_service,
)

logger = setup_logger(__name__)

# ==================== 配置 ====================
APP_ID = os.getenv("FEISHU_APP_ID")
APP_SECRET = os.getenv("FEISHU_APP_SECRET")
# 指令白名单：未配置时 fail-closed，拒绝包括查询在内的所有机器人指令。
ALLOWED_OPEN_IDS = frozenset(
    open_id.strip()
    for open_id in os.getenv("FEISHU_ALLOWED_OPEN_IDS", "").split(",")
    if open_id.strip()
)

PID_FILE = ".miner.pid"
PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
MINER_SCRIPT_NAME = "run_alpha_miner.py"

# ==================== 任务队列与命令分组 ====================
task_queue = queue.Queue(maxsize=50)

ASYNC_COMMANDS = {"/check", "/submit"}
FAST_COMMANDS = {"/summary", "/list", "/help", "/auto_check", "/auto_submit"}
CONTROL_COMMANDS = {"/start", "/stop"}

TOTAL_CHECK_TIMEOUT = 1800
SUBMIT_TIMEOUT = 3600

# ==================== 消息去重缓存 ====================
_processed_messages = OrderedDict()
_processed_messages_max_size = 1000
_processed_messages_ttl_seconds = 300
_processed_lock = threading.Lock()

# 进程操作锁：防止 /start 与 /stop 并发冲突
_process_op_lock = threading.Lock()


# ── 去重工具 ─────────────────────────────────────────

def _cleanup_expired_messages(now: float):
    expired_keys = [
        k for k, v in _processed_messages.items()
        if now - v > _processed_messages_ttl_seconds
    ]
    for k in expired_keys:
        del _processed_messages[k]


def _limit_processed_cache():
    while len(_processed_messages) > _processed_messages_max_size:
        _processed_messages.popitem(last=False)


def _try_mark_processed(event_id: str) -> bool:
    now = time.time()
    _cleanup_expired_messages(now)
    if event_id in _processed_messages:
        return False
    _processed_messages[event_id] = now
    _limit_processed_cache()
    return True


# ── 群聊 @ 占位符剥离 ───────────────────────────────

def _strip_mentions(text: str) -> str:
    """
    剥离飞书群聊消息中的 @_user_N 占位符。
    """
    return re.sub(r"@_user_\d+\s*", "", text).strip()


def _get_sender_open_id(event: P2ImMessageReceiveV1) -> str:
    """从飞书事件中提取发送者 open_id；结构不完整时按未授权处理。"""
    try:
        return event.event.sender.sender_id.open_id or ""
    except (AttributeError, TypeError):
        return ""


def _is_authorized_sender(sender_open_id: str) -> bool:
    """仅白名单成员可执行指令；空白名单不代表放行所有人。"""
    return bool(ALLOWED_OPEN_IDS) and sender_open_id in ALLOWED_OPEN_IDS


# ── subprocess 封装 ─────────────────────────────────

def _run_subprocess(cmd: list, timeout: int = 15) -> str:
    import subprocess
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout,
        )
        return result.stdout or ""
    except Exception as e:
        logger.error(f"subprocess 执行失败: {cmd[0]}, error={type(e).__name__}")
        return ""


def subprocess_run_powershell(script: str, timeout: int = 10) -> str:
    return _run_subprocess(
        ["powershell", "-NoProfile", "-Command", script], timeout=timeout
    )


# ── 进程管理 ─────────────────────────────────────────

def _safe_remove(path: str):
    try:
        os.remove(path)
    except Exception:
        pass


def _get_process_commandline(pid: int) -> str:
    try:
        if sys.platform == "win32":
            out = subprocess_run_powershell(
                f"(Get-CimInstance Win32_Process -Filter \"ProcessId={pid}\").CommandLine"
            )
            return (out or "").strip()
        else:
            try:
                with open(f"/proc/{pid}/cmdline", "rb") as f:
                    return f.read().replace(b"\x00", b" ").decode(errors="ignore").strip()
            except Exception:
                return ""
    except Exception:
        return ""


def _find_miner_pid_by_cmdline(exclude_pid: int = 0) -> int:
    try:
        # 必须限定 Name 为 python*，否则执行本查询的 powershell 自身命令行里
        # 就含有 run_alpha_miner.py，会被误当成挖掘器返回。
        out = subprocess_run_powershell(
            "Get-CimInstance Win32_Process | "
            "Where-Object { $_.Name -like 'python*' -and $_.CommandLine -like '*run_alpha_miner.py*' } | "
            "Sort-Object CreationDate -Descending | "
            "Select-Object ProcessId"
        )
        for line in (out or "").splitlines():
            x = line.strip()
            if x.isdigit():
                p = int(x)
                if p != os.getpid() and p != exclude_pid:
                    return p
    except Exception:
        pass
    return 0


def _pid_alive(pid: int) -> bool:
    """判断 PID 是否仍存在（不校验进程身份）。"""
    if pid <= 0:
        return False

    if sys.platform == "win32":
        out = _run_subprocess(
            ["tasklist", "/fo", "csv", "/nh", "/fi", f"PID eq {pid}"]
        )
        for line in out.splitlines():
            line = line.strip()
            if not line:
                continue
            parts = [p.strip('"') for p in line.split('","')]
            if len(parts) >= 2 and parts[1] == str(pid):
                return True
        return False

    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _write_pid_file(pid: int):
    try:
        with open(PID_FILE, "w") as f:
            f.write(str(pid))
    except Exception as e:
        logger.warning(f"写入 PID 文件失败: {type(e).__name__}")


def is_miner_running() -> tuple:
    """
    判断挖掘器是否真的在运行。

    PID 文件只是缓存：失效、指向别的进程、或用户手动启动未写文件时，
    都以实际进程扫描结果为准，并回写 PID 文件做校正。
    """
    pid = 0
    if os.path.exists(PID_FILE):
        try:
            with open(PID_FILE, "r") as f:
                pid = int(f.read().strip())
        except Exception:
            logger.warning("PID 文件内容无法解析，已清理")
            _safe_remove(PID_FILE)
            pid = 0

    if pid and pid == os.getpid():
        logger.error(f"PID 文件指向 bot 自身 (PID={pid})，判定失效并清理")
        _safe_remove(PID_FILE)
        pid = 0

    if pid and not _pid_alive(pid):
        logger.info(f"PID 文件已过期 (PID={pid} 不存在)，已清理")
        _safe_remove(PID_FILE)
        pid = 0

    if pid:
        cmdline = _get_process_commandline(pid)
        if not cmdline or MINER_SCRIPT_NAME not in cmdline:
            logger.warning(
                f"PID {pid} 存在但命令行不匹配挖掘器（cmdline={cmdline[:80]!r}），清理 PID 文件"
            )
            _safe_remove(PID_FILE)
            pid = 0

    if pid:
        return True, pid

    # PID 文件缺失或已被否决时，按命令行扫描真实进程（手动启动的情况）
    real_pid = _find_miner_pid_by_cmdline()
    if real_pid:
        logger.info(f"发现未登记的挖掘器进程 (PID={real_pid})，回写 PID 文件")
        _write_pid_file(real_pid)
        return True, real_pid

    return False, 0


def start_miner(workers: int = 2) -> tuple:
    import subprocess

    running, pid = is_miner_running()
    if running:
        return False, f"挖掘已在运行中 (PID={pid})"

    try:
        log_dir = os.path.join(PROJECT_DIR, "log")
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, f"miner-{datetime.now().strftime('%Y-%m-%d')}.log")

        child_env = {
            **os.environ,
            "MINER_LOG_STREAM": "0",
            "PYTHONIOENCODING": "utf-8",
        }

        with open(log_file, "a") as lf:
            proc = subprocess.Popen(
                [sys.executable, MINER_SCRIPT_NAME, "--workers", str(workers)],
                cwd=PROJECT_DIR,
                stdout=lf,
                stderr=subprocess.STDOUT,
                start_new_session=True,
                env=child_env,
            )

        time.sleep(2.0)

        if proc.poll() is None:
            real_pid = proc.pid
        else:
            real_pid = _find_miner_pid_by_cmdline(exclude_pid=proc.pid)
            if not real_pid:
                _safe_remove(PID_FILE)
                return False, (
                    f"启动后未找到 run_alpha_miner.py 进程"
                    f"（stub exit={proc.returncode}），请检查 log 目录"
                )

        with open(PID_FILE, "w") as f:
            f.write(str(real_pid))

        logger.info(f"挖掘进程已启动: PID={real_pid}, workers={workers} (stub={proc.pid})")
        return True, f"挖掘已启动 (PID={real_pid}, workers={workers})"

    except Exception as e:
        logger.error(f"启动挖掘失败: {e}", exc_info=True)
        return False, "启动失败，详细原因请查看服务器日志"


def stop_miner() -> tuple:
    running, pid = is_miner_running()
    if not running:
        return False, "挖掘未在运行"

    if pid == os.getpid():
        _safe_remove(PID_FILE)
        return False, "拒绝执行：目标 PID 指向 bot 自身"

    cmdline = _get_process_commandline(pid)
    if not cmdline or MINER_SCRIPT_NAME not in cmdline:
        _safe_remove(PID_FILE)
        return False, f"拒绝执行：PID {pid} 命令行不匹配挖掘器（cmdline={cmdline[:80]!r}）"

    try:
        _run_subprocess(["taskkill", "/PID", str(pid), "/T", "/F"], timeout=15)
        logger.info(f"taskkill /T /F PID={pid}")

        _safe_remove(PID_FILE)
        return True, "挖掘已停止"

    except Exception as e:
        logger.error(f"停止挖掘失败: {e}", exc_info=True)
        return False, "停止失败，详细原因请查看服务器日志"


# ── 命令执行器 ────────────────────────────────────────

def cmd_summary(args: list) -> tuple:
    """执行 /summary 命令。返回 (title, content)。"""
    db = get_alpha_db()
    summary = db.get_alpha_summary()

    lines = [
        "## 因子库汇总",
        f"- 因子总数: **{summary['total']}**",
        f"- 已提交: **{summary['submitted']}**",
        f"- 待检查: **{summary['pending']}**",
        f"- 当前可提交: **{summary['unsubmitted']}**",
        "",
        "## 累计统计",
        f"- 全部因子: **{summary['new_all_time']}**",
        f"- 累计可提交: **{summary['submittable_all_time']}**",
    ]

    running, pid = is_miner_running()
    lines.append("")
    lines.append("## 挖掘状态")
    if running:
        lines.append(f"- 状态: **运行中** (PID={pid})")
    else:
        lines.append("- 状态: **未运行**")

    return "因子库汇总", "\n".join(lines)


def cmd_start(args: list) -> tuple:
    """执行 /start 命令。返回 (title, content)。"""
    workers = 2
    if args:
        try:
            workers = int(args[0])
            if workers < 1 or workers > 10:
                return "启动失败", "workers 数量应在 1-10 之间"
        except ValueError:
            return "启动失败", f"无效的 workers 参数: {args[0]}"

    success, message = start_miner(workers)
    return "启动挖掘" if success else "启动失败", message


def cmd_stop(args: list) -> tuple:
    """执行 /stop 命令。返回 (title, content)。"""
    success, message = stop_miner()
    return "停止挖掘" if success else "停止失败", message


def cmd_check(args: list) -> tuple:
    """执行 /check 命令。通过 AutoCheckService 全量检查。"""
    try:
        db = get_alpha_db()
        service = get_check_service()

        total_pending = db.get_alpha_summary()["pending"]
        if total_pending == 0:
            return "相关性检查", "没有 pending 的 alpha"

        results = service.run_all_pending(batch_size=5)

        if len(results) == 1 and results[0].error_detail == "busy":
            return "检查失败", "已有检查任务在执行（可能是自动检查），请稍后再试"

        num_batches = len([r for r in results if r.total_checked > 0])
        lines = [
            "## 相关性检查结果", "",
            f"共 {total_pending} 个 pending，分 {num_batches} 批检查（每批 5 个）", "",
        ]

        aborted = False
        abort_reason = ""

        for idx, r in enumerate(results, 1):
            if r.total_checked == 0 and not r.error_detail:
                continue
            lines.append(f"**批次 {idx}/{num_batches}**（{r.total_checked} 个）")
            if r.error_detail == "总超时":
                lines.append("  - ⏰ 总超时（>1800s），已中止后续批次")
                aborted, abort_reason = True, "总超时"
                break
            if r.error_detail.startswith("批次超时"):
                lines.append(f"  - ⏰ {r.error_detail}")
                aborted, abort_reason = True, r.error_detail
                break
            if r.error_detail:
                lines.append(f"  - ❌ {r.error_detail}")
            else:
                lines.append(
                    f"  - PASS: {r.passed}, FAIL: {r.failed}, PENDING: {r.pending}"
                )
            lines.append("")

        if aborted:
            lines.append(f"### ⚠️ 已中止后续批次：{abort_reason}")
            lines.append("请过段时间再 /check")

        summary = db.get_alpha_summary()
        lines.append("### 汇总")
        lines.append(f"- 因子总数: {summary['total']}")
        lines.append(f"- 已提交: {summary['submitted']}")
        lines.append(f"- 待检查: {summary['pending']}")
        lines.append(f"- 当前可提交: {summary['unsubmitted']}")
        lines.append(f"- 累计因子: {summary['new_all_time']}")

        return "相关性检查", "\n".join(lines)

    except Exception as e:
        logger.error(f"[cmd_check] 异常: {e}", exc_info=True)
        return "检查失败", "检查命令执行异常，详细原因请查看服务器日志。"


def cmd_submit(args: list) -> tuple:
    """执行 /submit 命令。返回 (title, content)。"""
    import subprocess

    if not args:
        return "提交失败", "请指定 alpha ID，例如: /submit akornja9 QPnwOqKr"

    try:
        result = subprocess.run(
            [sys.executable, "submit_alpha.py"] + args,
            cwd=PROJECT_DIR,
            capture_output=True,
            text=True,
            timeout=SUBMIT_TIMEOUT,
        )
        all_output = result.stdout + "\n" + result.stderr

        lines = ["## 提交结果", ""]
        current_alpha_id = None
        detail_buf = []

        for line in all_output.splitlines():
            s = line.strip()
            if not s:
                continue

            if "Processing Alpha:" in line:
                parts = line.split("Processing Alpha:")
                if len(parts) >= 2:
                    current_alpha_id = parts[-1].strip()
                detail_buf.clear()
                continue

            if s.startswith("- "):
                detail_buf.append(s)

            if "[OK] 提交成功并已通过平台核验" in line:
                if current_alpha_id:
                    lines.append(f"- {current_alpha_id}: ✅ 提交成功，已通过核验")
                current_alpha_id = None
                detail_buf.clear()

            elif "[OK] 平台显示已提交" in line or "远端已经提交" in line:
                if current_alpha_id:
                    lines.append(f"- {current_alpha_id}: ⚠️ 远端已提交，已同步本地状态")
                    for d in detail_buf:
                        lines.append(f"  {d}")
                current_alpha_id = None
                detail_buf.clear()

            elif "[FAIL] 提交未通过平台指标检查" in line:
                if current_alpha_id:
                    err_msg = line.split("[FAIL]")[-1].strip()
                    lines.append(f"- {current_alpha_id}: ❌ 失败，原因：{err_msg}")
                    for d in detail_buf:
                        lines.append(f"  {d}")
                current_alpha_id = None
                detail_buf.clear()

        if result.returncode != 0:
            lines.append(f"\n提交进程退出码: {result.returncode}；请在服务器终端排查详细错误。")
        # 不向群聊回显原始 stdout/stderr，避免泄露认证响应、堆栈和服务器路径。
        return "因子提交", "\n".join(lines)

    except subprocess.TimeoutExpired:
        return "提交超时", f"因子提交执行超时（>{SUBMIT_TIMEOUT}s）"
    except Exception as e:
        logger.error(f"[cmd_submit] 异常: {type(e).__name__}", exc_info=True)
        return "提交失败", "提交命令执行异常，请检查服务器日志。"


def cmd_list(args: list) -> tuple:
    """执行 /list 命令。

    支持格式：
      /list                      → 最近 20 条
      /list 50                   → 最近 50 条
      /list pending              → 按状态过滤
      /list good                 → 按 grade 过滤
      /list 50 good              → 最近 50 条中 grade=GOOD 的
      /list d5WRNqew             → 按 alpha_id 查详情
    """
    import json as _json
    db = get_alpha_db()

    VALID_STATUSES = {"submitted", "unsubmitted", "pending", "tested", "error"}
    VALID_GRADES = {"INFERIOR", "AVERAGE", "GOOD", "EXCELLENT", "SPECTACULAR"}

    limit = 20
    status_filter = None
    grade_filter = None
    alpha_id_query = None

    for arg in args:
        s = arg.strip()
        if not s:
            continue
        if s.isdigit():
            limit = min(int(s), 100)
            continue
        lower = s.lower()
        if lower in VALID_STATUSES:
            status_filter = lower
            continue
        upper = s.upper()
        if upper in VALID_GRADES:
            grade_filter = upper
            continue
        alpha_id_query = s

    # ── 模式 1：按 alpha_id 查详情 ──
    if alpha_id_query:
        try:
            with db._cursor() as cur:
                cur.execute("SELECT * FROM alphas WHERE alpha_id = ?", (alpha_id_query,))
                row = cur.fetchone()
        except Exception as e:
            return "因子详情", f"查询失败: {e}"

        if not row:
            return "因子详情", f"未找到 alpha: {alpha_id_query}"

        d = dict(row)
        lines = [
            f"## Alpha 详情：{alpha_id_query}",
            "",
            "### 基础",
            f"- **状态**: {d.get('status') or '-'}",
            f"- **分级**: {d.get('grade') or '-'}",
            f"- **来源**: {d.get('source') or '-'}",
            f"- **创建时间**: {d.get('created_at') or '-'}",
            "",
            "### 指标",
            f"- **Sharpe**: {d.get('sharpe') if d.get('sharpe') is not None else '-'}",
            f"- **Fitness**: {d.get('fitness') if d.get('fitness') is not None else '-'}",
            f"- **Turnover**: {d.get('turnover') if d.get('turnover') is not None else '-'}",
            f"- **Returns**: {d.get('returns') if d.get('returns') is not None else '-'}",
            f"- **Margin**: {d.get('margin') if d.get('margin') is not None else '-'}",
            f"- **Drawdown**: {d.get('drawdown') if d.get('drawdown') is not None else '-'}",
            f"- **Long/Short**: {d.get('long_count') or '-'} / {d.get('short_count') or '-'}",
            "",
            "### 设置",
            f"- **Region**: {d.get('region') or '-'}",
            f"- **Universe**: {d.get('universe') or '-'}",
            f"- **Delay**: {d.get('delay') if d.get('delay') is not None else '-'}",
            f"- **Decay**: {d.get('decay') if d.get('decay') is not None else '-'}",
            f"- **Neutralization**: {d.get('neutralization') or '-'}",
            f"- **Truncation**: {d.get('truncation') if d.get('truncation') is not None else '-'}",
            "",
            "### 表达式",
            f"```\n{d.get('expression') or '-'}\n```",
        ]

        checks_raw = d.get('checks') or '[]'
        try:
            checks = _json.loads(checks_raw) if isinstance(checks_raw, str) else checks_raw
        except Exception:
            checks = []
        if checks:
            lines.append("")
            lines.append("### Checks")
            for c in checks:
                name = c.get('name', '?')
                result = c.get('result', '?')
                value = c.get('value')
                limit_v = c.get('limit')
                icon = "✅" if result == "PASS" else ("❌" if result == "FAIL" else "⏳")
                if value is not None and limit_v is not None:
                    lines.append(f"- {icon} **{name}**: {result} ({value} / {limit_v})")
                else:
                    lines.append(f"- {icon} **{name}**: {result}")

        return "因子详情", "\n".join(lines)

    # ── 模式 2：列表查询 ──
    where_parts = []
    params = []
    if status_filter:
        where_parts.append("status = ?")
        params.append(status_filter)
    if grade_filter:
        where_parts.append("UPPER(grade) = ?")
        params.append(grade_filter)
    where_sql = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""

    try:
        with db._cursor() as cur:
            cur.execute(
                f"SELECT created_at, alpha_id, status, grade, fitness, sharpe "
                f"FROM alphas {where_sql} ORDER BY created_at DESC LIMIT ?",
                (*params, limit)
            )
            rows = cur.fetchall()
    except Exception as e:
        return "因子列表", f"查询失败: {e}"

    filter_desc = []
    if status_filter:
        filter_desc.append(f"status={status_filter}")
    if grade_filter:
        filter_desc.append(f"grade={grade_filter}")
    filter_str = " / ".join(filter_desc) if filter_desc else "全部"

    if not rows:
        return "因子列表", f"暂无数据（{filter_str}）"

    lines = [f"## 因子列表（{filter_str}，共 {len(rows)} 条）", ""]
    lines.append("| Created At | Alpha ID | Status | Grade | Fitness | Sharpe |")
    lines.append("|------------|----------|--------|-------|---------|--------|")

    for row in rows:
        if row["created_at"]:
            if isinstance(row["created_at"], datetime):
                created_at = row["created_at"].strftime("%Y-%m-%d %H:%M:%S")
            else:
                created_at = str(row["created_at"])
        else:
            created_at = "-"

        alpha_id = row["alpha_id"] or "-"
        status = row["status"] or "-"
        grade = row["grade"] or "-"
        fitness = f"{row['fitness']:.4f}" if row["fitness"] is not None else "-"
        sharpe = f"{row['sharpe']:.4f}" if row["sharpe"] is not None else "-"
        lines.append(
            f"| {created_at} | {alpha_id} | {status} | {grade} | {fitness} | {sharpe} |"
        )

    lines.append("")
    lines.append("*格式: `/list [数量] [状态|grade]` 或 `/list <alpha_id>` 查详情*")
    lines.append("*状态: submitted, unsubmitted, pending, tested, error*")
    lines.append("*Grade: INFERIOR, AVERAGE, GOOD, EXCELLENT, SPECTACULAR*")

    return "因子列表", "\n".join(lines)


def cmd_auto_check(args: list) -> tuple:
    """执行 /auto_check 命令。

    用法：
      /auto_check         → 查看状态
      /auto_check on      → 启动
      /auto_check off     → 停止
    """
    scheduler = get_scheduler("check")
    if scheduler is None:
        return "自动检查", "未初始化"
    if not args:
        return "自动检查状态", scheduler.status()
    action = args[0].lower()
    if action == "on":
        ok, msg = scheduler.start()
        return ("启动自动检查" if ok else "启动失败"), msg
    if action == "off":
        ok, msg = scheduler.stop()
        return ("停止自动检查" if ok else "停止失败"), msg
    return "参数错误", "用法: /auto_check [on|off]，无参数查状态"


def cmd_auto_submit(args: list) -> tuple:
    """执行 /auto_submit 命令。

    用法：
      /auto_submit        → 查看状态
      /auto_submit on     → 启动
      /auto_submit off    → 停止
    """
    scheduler = get_scheduler("submit")
    if scheduler is None:
        return "自动提交", "未初始化"
    if not args:
        return "自动提交状态", scheduler.status()
    action = args[0].lower()
    if action == "on":
        ok, msg = scheduler.start()
        return ("启动自动提交" if ok else "启动失败"), msg
    if action == "off":
        ok, msg = scheduler.stop()
        return ("停止自动提交" if ok else "停止失败"), msg
    return "参数错误", "用法: /auto_submit [on|off]，无参数查状态"


def cmd_help(args: list) -> tuple:
    """执行 /help 命令。返回 (title, content)。"""
    lines = [
        "## 可用命令",
        "",
        "| 命令 | 说明 | 示例 |",
        "|------|------|------|",
        "| `/summary` | 查看因子库统计 | `/summary` |",
        "| `/start [workers]` | 启动挖掘器 | `/start 2` |",
        "| `/stop` | 停止挖掘器 | `/stop` |",
        "| `/check` | 手动相关性检查（全量） | `/check` |",
        "| `/submit <id> [id2...]` | 手动提交指定因子 | `/submit akornja9` |",
        "| `/list [数量] [状态/grade]` | 查看因子列表 | `/list 20 submitted`、`/list 50 good` |",
        "| `/list <alpha_id>` | 查看单个因子完整详情 | `/list d5WRNqew` |",
        "| `/auto_check [on/off]` | 自动检查：无参数查状态，on 启动，off 停止 | `/auto_check on` |",
        "| `/auto_submit [on/off]` | 自动提交：无参数查状态，on 启动，off 停止 | `/auto_submit on` |",
        "| `/help` | 显示此帮助信息 | `/help` |",
        "",
        "**状态筛选:** submitted, unsubmitted, pending, tested, error",
        "",
        "**Grade 筛选:** INFERIOR, AVERAGE, GOOD, EXCELLENT, SPECTACULAR",
    ]
    return "帮助信息", "\n".join(lines)


COMMANDS = {
    "/summary": cmd_summary,
    "/start": cmd_start,
    "/stop": cmd_stop,
    "/check": cmd_check,
    "/submit": cmd_submit,
    "/list": cmd_list,
    "/auto_check": cmd_auto_check,
    "/auto_submit": cmd_auto_submit,
    "/help": cmd_help,
}


def parse_command(text: str) -> tuple:
    parts = text.strip().split()
    if not parts:
        return None, []
    return parts[0].lower(), parts[1:]


# ── 发消息 ────────────────────────────────────────────

def _safe_reply(message_id: str, title: str, content: str):
    """发送消息。成功记 DEBUG，失败记 ERROR，绝不向上抛异常。"""
    try:
        client = get_feishu_client()
        client.reply_message(message_id, title, content)
        logger.debug(f"回复成功: title={title[:30]}")
    except Exception as e:
        logger.error(f"回复失败: title={title[:30]}, error={e}", exc_info=True)


def _spawn_reply(message_id: str, title: str, content: str):
    """异步发送消息，用于 handler 内部避免阻塞。"""
    threading.Thread(
        target=_safe_reply,
        args=(message_id, title, content),
        daemon=True,
    ).start()


# ── 后台执行器 ────────────────────────────────────────

def _worker():
    """后台消费线程：串行执行 /check /submit。"""
    while True:
        task = task_queue.get()
        if task is None:
            task_queue.task_done()
            break

        message_id, cmd, args = task
        t0 = time.time()
        logger.info(f"执行开始: {cmd} {args} (队列)")
        try:
            title, content = COMMANDS[cmd](args)
            _safe_reply(message_id, title, content)
            logger.info(f"执行完成: {cmd}, 耗时={time.time() - t0:.2f}s")
        except Exception as e:
            logger.error(
                f"执行失败: {cmd}, 耗时={time.time() - t0:.2f}s, error={e}",
                exc_info=True,
            )
            _safe_reply(message_id, "执行失败", f"命令 {cmd} 执行异常，详细原因请查看服务器日志。")
        finally:
            task_queue.task_done()


def _run_control_in_background(message_id: str, cmd: str, args: list):
    """后台执行 /start /stop，持进程操作锁防并发冲突。"""
    with _process_op_lock:
        t0 = time.time()
        logger.info(f"执行开始: {cmd} {args} (控制命令)")
        try:
            title, content = COMMANDS[cmd](args)
            _safe_reply(message_id, title, content)
            logger.info(f"执行完成: {cmd}, 耗时={time.time() - t0:.2f}s")
        except Exception as e:
            logger.error(
                f"执行失败: {cmd}, 耗时={time.time() - t0:.2f}s, error={e}",
                exc_info=True,
            )
            _safe_reply(message_id, "执行失败", f"命令 {cmd} 执行异常，详细原因请查看服务器日志。")


def _run_fast_command(message_id: str, cmd: str, args: list):
    """后台执行快速命令，不阻塞 handler。"""
    t0 = time.time()
    logger.info(f"执行开始: {cmd} {args} (快速命令)")
    try:
        title, content = COMMANDS[cmd](args)
        _safe_reply(message_id, title, content)
        logger.info(f"执行完成: {cmd}, 耗时={time.time() - t0:.2f}s")
    except Exception as e:
        logger.error(
            f"执行失败: {cmd}, 耗时={time.time() - t0:.2f}s, error={e}",
            exc_info=True,
        )
        _safe_reply(message_id, "执行失败", f"命令 {cmd} 执行异常，详细原因请查看服务器日志。")


# ── 事件处理器 ───────────────────────────────────────

def _on_message(event: P2ImMessageReceiveV1) -> None:
    """
    飞书 im.message.receive_v1 事件处理器。

    铁律：
      - 3 秒内返回（SDK 收到 return 后自动 ACK）
      - 绝不向外抛异常
    """
    t0 = time.time()
    try:
        event_id = event.header.event_id
        message = event.event.message
        message_id = message.message_id
        chat_type = message.chat_type
        msg_type = message.message_type

        if msg_type != "text":
            return

        raw_text = lark.JSON.unmarshal(message.content, dict).get("text", "")
        text = _strip_mentions(raw_text)
        if not text:
            return

        with _processed_lock:
            if not _try_mark_processed(event_id):
                logger.warning(f"重复事件，跳过: event_id={event_id}")
                return

        cmd, args = parse_command(text)
        logger.info(
            f"收到命令: {cmd} {args}, 来源={chat_type}, event_id={event_id}"
        )

        if cmd not in COMMANDS:
            logger.warning(f"未知命令: {cmd}")
            _spawn_reply(
                message_id,
                "未知命令",
                "支持的命令:\n" + "\n".join(f"- `{k}`" for k in COMMANDS.keys()),
            )
            return

        sender_open_id = _get_sender_open_id(event)
        if not _is_authorized_sender(sender_open_id):
            # 日志保留 open_id 以便管理员配置白名单；不记录消息正文或凭据。
            logger.warning(
                f"拒绝未授权指令: {cmd}, sender_open_id={sender_open_id or 'missing'}, "
                f"event_id={event_id}"
            )
            _spawn_reply(
                message_id,
                "无权限",
                "你的账号不在机器人指令白名单中，请联系管理员配置 "
                "FEISHU_ALLOWED_OPEN_IDS。",
            )
            return

        if cmd in CONTROL_COMMANDS:
            logger.info(f"分发: {cmd} → 控制命令后台线程")
            _spawn_reply(message_id, f"{cmd} 已收到", "正在执行，请稍候...")
            threading.Thread(
                target=_run_control_in_background,
                args=(message_id, cmd, args),
                daemon=True,
            ).start()
            return

        if cmd in ASYNC_COMMANDS:
            try:
                task_queue.put_nowait((message_id, cmd, args))
                logger.info(f"分发: {cmd} → 队列 (当前队列长度={task_queue.qsize()})")
                _spawn_reply(
                    message_id,
                    "任务已入队",
                    f"命令 `{cmd}` 已加入队列，执行完成后推送结果。",
                )
            except queue.Full:
                logger.warning(f"队列已满，拒绝入队: {cmd}")
                _spawn_reply(message_id, "系统繁忙", "队列已满，请稍后重试。")
            return

        logger.info(f"分发: {cmd} → 快速命令后台线程")
        threading.Thread(
            target=_run_fast_command,
            args=(message_id, cmd, args),
            daemon=True,
        ).start()

    except Exception as e:
        logger.error(f"handler 异常: {e}", exc_info=True)
    finally:
        elapsed = time.time() - t0
        if elapsed > 1.0:
            logger.warning(f"handler 耗时偏高: {elapsed:.2f}s")
        else:
            logger.debug(f"handler 耗时: {elapsed * 1000:.1f}ms")


# ── 主入口 ───────────────────────────────────────────

def main():
    if not APP_ID or not APP_SECRET:
        print("错误: 请在 .env 中配置 FEISHU_APP_ID 和 FEISHU_APP_SECRET")
        sys.exit(1)

    if not ALLOWED_OPEN_IDS:
        logger.warning("FEISHU_ALLOWED_OPEN_IDS 未配置，机器人将拒绝所有指令")
        print("安全提示: FEISHU_ALLOWED_OPEN_IDS 未配置，所有指令都会被拒绝。")

    if os.path.exists(PID_FILE):
        try:
            with open(PID_FILE, "r") as f:
                existing_pid = int(f.read().strip())
            if existing_pid == os.getpid():
                logger.warning(f"启动时发现 PID 文件指向自身 ({existing_pid})，清理")
                _safe_remove(PID_FILE)
        except Exception:
            _safe_remove(PID_FILE)

    worker_thread = threading.Thread(target=_worker, daemon=True)
    worker_thread.start()
    logger.info("后台任务消费线程已启动")

    # 初始化自动任务（Check + Submit），配置非法只记录，不影响其他命令
    try:
        init_auto_tasks()
        for name in ("check", "submit"):
            _sched = get_scheduler(name)
            if _sched is not None:
                logger.info(
                    f"自动任务初始化完成: {name}, "
                    f"config_error={_sched._config_error or '无'}"
                )
            else:
                logger.warning(f"自动任务初始化完成，但未找到 {name} 调度器")
    except Exception as e:
        logger.error(f"init_auto_tasks 失败: {e}", exc_info=True)

    def _signal_handler(sig, frame):
        logger.info(f"收到信号 {sig}，准备优雅退出...")
        raise KeyboardInterrupt

    try:
        signal.signal(signal.SIGINT, _signal_handler)
        signal.signal(signal.SIGTERM, _signal_handler)
    except Exception as e:
        logger.warning(f"信号注册失败（非主线程运行时正常）: {e}")

    event_handler = lark.EventDispatcherHandler.builder(
        "", ""
    ).register_p2_im_message_receive_v1(_on_message).build()

    ws_client = lark.ws.Client(
        APP_ID,
        APP_SECRET,
        event_handler=event_handler,
        log_level=lark.LogLevel.INFO,
    )

    print("飞书机器人（长连接版）已启动，等待消息。Ctrl+C 退出。")

    try:
        ws_client.start()
    except KeyboardInterrupt:
        logger.info("收到 Ctrl+C，开始优雅退出...")
    except Exception as e:
        logger.error(f"WebSocket 客户端异常: {e}", exc_info=True)
    finally:
        logger.info("WebSocket 连接已断开，等待队列消费...")

        # 先停自动调度，再等调度线程收尾（有期限）
        try:
            shutdown_auto_tasks()
        except Exception as e:
            logger.warning(f"shutdown_auto_tasks 异常: {e}")

        try:
            task_queue.put_nowait(None)
        except queue.Full:
            pass
        try:
            task_queue.join()
        except Exception:
            pass
        _safe_remove(PID_FILE)
        logger.info("优雅退出完成")


if __name__ == "__main__":
    main()