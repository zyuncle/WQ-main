"""
自动任务模块：Auto Check + Auto Submit。

设计原则：
- 只新增这一个文件，不动 alpha_db.py。
- 自动任务自己用 sqlite3 查 pending / unsubmitted。
- AutoScheduler 是通用调度器，Check / Submit 共用。
- 手动 /check 和自动 Check 共用 AutoCheckService。
- 每日提交计数持久化到 data/submit_log.json，重启不丢。
- 每次提交前重新 check，避免污染后的相关性池导致误提交。
"""

import os
import re
import sys
import json
import time
import logging
import sqlite3
import threading
import subprocess
from dataclasses import dataclass, field
from datetime import datetime, time as dtime, timedelta
from zoneinfo import ZoneInfo
from typing import List, Optional, Callable, Any, Dict

logger = logging.getLogger(__name__)

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(PROJECT_DIR, "data", "alphas.db")
CHECK_SCRIPT = "check_correlation.py"
SUBMIT_SCRIPT = "submit_alpha.py"
SUBMIT_LOG = os.path.join(PROJECT_DIR, "data", "submit_log.json")

TZ = ZoneInfo("Asia/Shanghai")
MAX_WAIT_SEGMENT = 300
DEFAULT_CHECK_BATCH_SIZE = 5
CHECK_BATCH_TIMEOUT = 300
CHECK_TOTAL_TIMEOUT = 1800
SUBMIT_TIMEOUT = 120

GRADE_ORDER = {
    "SPECTACULAR": 5,
    "EXCELLENT": 4,
    "GOOD": 3,
    "AVERAGE": 2,
    "INFERIOR": 1,
}
HIGH_QUALITY_GRADES = {"SPECTACULAR", "EXCELLENT", "GOOD"}


# ══════════════════════════════════════════════════════
# 内部 DB 查询
# ══════════════════════════════════════════════════════

def _query_pending(limit: int) -> List[Dict]:
    """按 created_at 升序查 pending，旧的先。"""
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.execute(
            "SELECT * FROM alphas WHERE status='pending' "
            "ORDER BY created_at ASC LIMIT ?",
            (limit,),
        )
        return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def _count_pending() -> int:
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    try:
        row = conn.execute(
            "SELECT COUNT(*) FROM alphas WHERE status='pending'"
        ).fetchone()
        return row[0] if row else 0
    finally:
        conn.close()


def _query_unsubmitted(min_fitness: float = 0.0) -> List[Dict]:
    """查 unsubmitted 因子，按 grade 和 fitness 排序。低于 min_fitness 的排除。"""
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.execute(
            "SELECT * FROM alphas WHERE status='unsubmitted' "
            "AND grade != 'INFERIOR' AND grade IS NOT NULL "
            "AND fitness IS NOT NULL AND fitness >= ?",
            (min_fitness,),
        )
        rows = [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()

    def key(a):
        return (
            GRADE_ORDER.get((a.get("grade") or "").upper(), 0),
            a.get("fitness") or 0.0,
            a.get("sharpe") or 0.0,
        )
    return sorted(rows, key=key, reverse=True)


# ══════════════════════════════════════════════════════
# 每日提交计数持久化
# ══════════════════════════════════════════════════════

def _read_submit_log() -> dict:
    if not os.path.exists(SUBMIT_LOG):
        return {}
    try:
        with open(SUBMIT_LOG, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _get_today_submitted() -> int:
    log = _read_submit_log()
    return log.get(datetime.now(TZ).strftime("%Y-%m-%d"), 0)


def _increment_today_submitted(n: int = 1):
    log = _read_submit_log()
    today = datetime.now(TZ).strftime("%Y-%m-%d")
    log[today] = log.get(today, 0) + n
    cutoff = (datetime.now(TZ) - timedelta(days=30)).strftime("%Y-%m-%d")
    log = {k: v for k, v in log.items() if k >= cutoff}
    os.makedirs(os.path.dirname(SUBMIT_LOG), exist_ok=True)
    with open(SUBMIT_LOG, "w", encoding="utf-8") as f:
        json.dump(log, f, indent=2, ensure_ascii=False)


# ══════════════════════════════════════════════════════
# 结果对象
# ══════════════════════════════════════════════════════

@dataclass
class BatchResult:
    total_checked: int = 0
    passed: int = 0
    failed: int = 0
    pending: int = 0
    already_submitted: int = 0
    error: int = 0
    unknown: int = 0
    deleted: int = 0
    updated: int = 0
    passed_alphas: list = field(default_factory=list)
    failed_alphas: list = field(default_factory=list)
    unknown_alphas: list = field(default_factory=list)
    remaining_pending: int = 0
    error_detail: str = ""


@dataclass
class SubmitBatchResult:
    target_count: int = 0
    submitted: int = 0
    skipped: int = 0
    failed: int = 0
    submitted_ids: list = field(default_factory=list)
    skipped_details: list = field(default_factory=list)
    failed_details: list = field(default_factory=list)
    today_before: int = 0
    today_after: int = 0
    remaining_max: int = 0
    remaining_min: int = 0
    error_detail: str = ""


# ══════════════════════════════════════════════════════
# Check 业务
# ══════════════════════════════════════════════════════

class AutoCheckService:
    """持锁，单批执行 subprocess。结果只返回，不通知。"""

    def __init__(self):
        self._lock = threading.Lock()

    def _run_batch_unlocked(self, alpha_ids: List[str]) -> BatchResult:
        result = BatchResult(total_checked=len(alpha_ids))
        if not alpha_ids:
            result.remaining_pending = _count_pending()
            return result

        try:
            proc = subprocess.run(
                [sys.executable, CHECK_SCRIPT, "--no-notify", "--batch-mode",
                 "--alpha-ids", ",".join(alpha_ids)],
                cwd=PROJECT_DIR,
                capture_output=True, text=True,
                encoding="utf-8", errors="replace",
                env={**os.environ, "PYTHONIOENCODING": "utf-8"},
                timeout=CHECK_BATCH_TIMEOUT,
            )
            output = proc.stdout or ""
            marker = "__BATCH_RESULT__"
            if marker in output:
                json_str = output.split(marker, 1)[1].strip()
                json_line = json_str.split("\n", 1)[0].strip()
                data = json.loads(json_line)
                result.passed = data.get("pass", 0)
                result.failed = data.get("fail", 0)
                result.pending = data.get("pending", 0)
                result.already_submitted = data.get("already_submitted", 0)
                result.unknown = data.get("unknown", 0)
                result.error = data.get("error", 0)
                result.deleted = data.get("deleted", 0)
                result.updated = data.get("updated", 0)
                result.passed_alphas = data.get("passed_alphas", []) or []
                result.failed_alphas = data.get("failed_alphas", []) or []
                result.unknown_alphas = data.get("unknown_alphas", []) or []
            else:
                result.error = len(alpha_ids)
                result.error_detail = "未解析到 __BATCH_RESULT__"
                logger.warning(
                    f"[CheckService] 未解析到结果: rc={proc.returncode}, "
                    f"stderr={(proc.stderr or '')[-300:]!r}"
                )
        except subprocess.TimeoutExpired:
            result.error = len(alpha_ids)
            result.error_detail = f"批次超时(>{CHECK_BATCH_TIMEOUT}s)"
            logger.warning(f"[CheckService] 批次超时: {len(alpha_ids)} 个")
        except Exception as e:
            result.error = len(alpha_ids)
            result.error_detail = f"批次异常: {e}"
            logger.error(f"[CheckService] 批次异常: {e}", exc_info=True)

        result.remaining_pending = _count_pending()
        return result

    def run_one_batch(self, max_count: int) -> BatchResult:
        if not self._lock.acquire(blocking=False):
            return BatchResult(error_detail="busy")
        try:
            pending = _query_pending(limit=max_count)
            alpha_ids = [a["alpha_id"] for a in pending if a.get("alpha_id")]
            return self._run_batch_unlocked(alpha_ids)
        finally:
            self._lock.release()

    def run_all_pending(self, batch_size: int = DEFAULT_CHECK_BATCH_SIZE) -> List[BatchResult]:
        if not self._lock.acquire(blocking=False):
            return [BatchResult(error_detail="busy")]
        try:
            results: List[BatchResult] = []
            start = time.time()

            while True:
                if time.time() - start > CHECK_TOTAL_TIMEOUT:
                    results.append(BatchResult(error_detail="总超时"))
                    break

                pending = _query_pending(limit=batch_size)
                alpha_ids = [a["alpha_id"] for a in pending if a.get("alpha_id")]
                if not alpha_ids:
                    break

                r = self._run_batch_unlocked(alpha_ids)
                results.append(r)

                if r.error_detail.startswith("批次超时"):
                    break
                if r.remaining_pending == 0:
                    break

            return results
        finally:
            self._lock.release()


# ══════════════════════════════════════════════════════
# Submit 业务
# ══════════════════════════════════════════════════════

class AutoSubmitService:
    """持锁，按策略计算目标数量，逐个重新 check + submit。"""

    def __init__(self):
        self._lock = threading.Lock()
        self._session = None
        self._session_auth_time = 0

    def _ensure_session(self):
        now = time.time()
        if self._session is not None and now - self._session_auth_time < 6600:
            return self._session
        import requests
        from requests.auth import HTTPBasicAuth
        from core.config import load_credentials
        user, pwd = load_credentials()
        s = requests.Session()
        s.auth = HTTPBasicAuth(user, pwd)
        resp = s.post(
            "https://api.worldquantbrain.com/authentication",
            verify=True, timeout=15,
        )
        if resp.status_code != 201:
            raise RuntimeError(f"Auth failed: {resp.text[:200]}")
        self._session = s
        self._session_auth_time = now
        return s

    def _check_single(self, alpha_id: str) -> dict:
        from check_correlation import check_alpha, get_self_correlation
        session = self._ensure_session()
        r = check_alpha(session, alpha_id, max_wait=120)
        if not r["success"]:
            raise RuntimeError(r.get("error", "check failed"))
        return get_self_correlation(r["checks"])

    def _submit_single(self, alpha_id: str) -> tuple:
        try:
            proc = subprocess.run(
                [sys.executable, SUBMIT_SCRIPT, alpha_id],
                cwd=PROJECT_DIR,
                capture_output=True, text=True,
                encoding="utf-8", errors="replace",
                env={**os.environ, "PYTHONIOENCODING": "utf-8"},
                timeout=SUBMIT_TIMEOUT,
            )
            out = (proc.stdout or "") + "\n" + (proc.stderr or "")
            if "[OK] 提交成功并已通过平台核验" in out:
                return True, ""
            if "[OK] 平台显示已提交" in out or "远端已经提交" in out:
                return True, ""
            if "[FAIL]" in out:
                reason = out.split("[FAIL]", 1)[1].split("\n", 1)[0].strip()[:200]
                return False, reason
            return False, f"未知输出, rc={proc.returncode}, tail={out[-200:]!r}"
        except subprocess.TimeoutExpired:
            return False, f"提交超时(>{SUBMIT_TIMEOUT}s)"
        except Exception as e:
            return False, f"提交异常: {e}"

    def _try_submit_one(self, alpha: Dict, result: SubmitBatchResult) -> bool:
        aid = alpha.get("alpha_id")
        if not aid:
            return False

        try:
            sc = self._check_single(aid)
        except Exception as e:
            result.failed += 1
            result.failed_details.append({"alpha_id": aid, "error": f"check异常: {e}"})
            logger.warning(f"[submit] {aid} check 异常: {e}")
            return False

        if sc.get("status") != "PASS":
            result.skipped += 1
            result.skipped_details.append({"alpha_id": aid, "reason": sc.get("status")})
            logger.info(f"[submit] {aid} check={sc.get('status')}, 跳过")
            return False

        ok, err = self._submit_single(aid)
        if ok:
            _increment_today_submitted(1)
            result.submitted += 1
            result.submitted_ids.append(aid)
            logger.info(
                f"[submit] {aid} 提交成功 "
                f"(F={alpha.get('fitness'):.2f}, G={alpha.get('grade')})"
            )
            return True
        else:
            result.failed += 1
            result.failed_details.append({"alpha_id": aid, "error": err})
            logger.warning(f"[submit] {aid} 提交失败: {err}")
            return False

    def run_one_batch(self, _unused_batch_size: int = 0) -> SubmitBatchResult:
        if not self._lock.acquire(blocking=False):
            return SubmitBatchResult(error_detail="busy")
        try:
            return self._run_unlocked()
        finally:
            self._lock.release()

    def _run_unlocked(self) -> SubmitBatchResult:
        result = SubmitBatchResult()

        max_per_day = int(os.getenv("AUTO_SUBMIT_MAX_PER_DAY", "20") or "20")
        min_per_day = int(os.getenv("AUTO_SUBMIT_MIN_PER_DAY", "4") or "4")
        min_fitness = float(os.getenv("AUTO_SUBMIT_MIN_FITNESS", "1.1") or "1.1")

        today_before = _get_today_submitted()
        remaining_max = max(0, max_per_day - today_before)
        remaining_min = max(0, min_per_day - today_before)

        result.today_before = today_before
        result.remaining_max = remaining_max
        result.remaining_min = remaining_min

        if remaining_max <= 0:
            result.error_detail = f"已达每日上限({max_per_day})"
            result.today_after = today_before
            return result

        candidates = _query_unsubmitted(min_fitness=min_fitness)
        if not candidates:
            result.error_detail = f"无达标因子（fitness < {min_fitness} 或池为空）"
            result.today_after = today_before
            logger.info(f"[submit] {result.error_detail}, 跳过本次")
            return result

        high = [a for a in candidates if (a.get("grade") or "").upper() in HIGH_QUALITY_GRADES]
        avg = [a for a in candidates if (a.get("grade") or "").upper() == "AVERAGE"]

        # 目标数量：先满足 MIN，但不超过 MAX
        if len(high) >= remaining_min:
            high_target = min(len(high), remaining_max)
            avg_target = 0
        else:
            high_target = len(high)
            avg_target = min(remaining_min - len(high), remaining_max - len(high))

        result.target_count = high_target + avg_target
        logger.info(
            f"[submit] 目标 {result.target_count} "
            f"(high={high_target}, avg={avg_target}), "
            f"今日已交 {today_before}, "
            f"remaining_max={remaining_max}, remaining_min={remaining_min}"
        )

        # 先交 high（遍历上限防止全 FAIL 死循环）
        high_attempt_limit = min(len(high), max(high_target * 3, high_target + 5))
        for alpha in high[:high_attempt_limit]:
            if result.submitted >= high_target:
                break
            if result.submitted >= remaining_max:
                break
            self._try_submit_one(alpha, result)

        # 补充 AVERAGE（只有 high 不够 MIN 时才触发）
        if result.submitted < remaining_min:
            avg_needed = remaining_min - result.submitted
            avg_attempt_limit = min(len(avg), max(avg_needed * 3, avg_needed + 5))
            for alpha in avg[:avg_attempt_limit]:
                if result.submitted >= remaining_min:
                    break
                if result.submitted >= remaining_max:
                    break
                self._try_submit_one(alpha, result)

        result.today_after = _get_today_submitted()
        return result


# ══════════════════════════════════════════════════════
# 通用调度器
# ══════════════════════════════════════════════════════

def parse_schedule(raw: str) -> List[dtime]:
    if not raw or not raw.strip():
        raise ValueError("调度配置未设置")
    raw = raw.strip()

    if re.fullmatch(r"\d{1,2}", raw):
        n = int(raw)
        if not 1 <= n <= 24:
            raise ValueError(f"整数调度需在 1-24 之间，实际为 {n}")
        step = 24 // n
        return [dtime(hour=(step * i) % 24) for i in range(n)]

    parts = [p.strip() for p in raw.split(",") if p.strip()]
    if not parts:
        raise ValueError("调度配置格式非法")

    times: List[dtime] = []
    for p in parts:
        try:
            times.append(datetime.strptime(p, "%H:%M").time())
        except ValueError:
            raise ValueError(f"非法时间格式: {p!r}，应为 HH:MM")

    if len(set(times)) != len(times):
        raise ValueError("调度配置存在重复时间")
    return sorted(times)


class AutoScheduler:
    """通用调度器。run_fn(batch_size) -> Any，返回对象若 error_detail=="busy" 则跳过。"""

    def __init__(
        self,
        name: str,
        run_fn: Callable[[int], Any],
        times: List[dtime],
        batch_size: int,
        format_result_fn: Optional[Callable[[Any], str]] = None,
        remaining_fn: Optional[Callable[[], int]] = None,
        remaining_label: str = "剩余待检查",
        config_error: str = "",
    ):
        self.name = name
        self._run_fn = run_fn
        self._times = times
        self._batch_size = batch_size
        self._format_result_fn = format_result_fn
        self._remaining_fn = remaining_fn
        self._remaining_label = remaining_label
        self._config_error = config_error

        self._state = "STOPPED"
        self._state_lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

        self._last_result: Optional[Any] = None
        self._last_run_time: Optional[datetime] = None
        self._last_triggered_slot: Optional[str] = None
        self._next_run: Optional[datetime] = None
        self._executing_batch = False

    def start(self) -> tuple:
        if self._config_error:
            return False, f"{self.name} 配置错误：{self._config_error}"
        with self._state_lock:
            if self._state == "RUNNING":
                return True, self.status()
            if self._state == "STOPPING":
                return False, f"{self.name} 正在停止，请稍后再试"

            self._stop_event.clear()
            self._state = "RUNNING"
            self._next_run = self._next_run_datetime(datetime.now(TZ))
            self._thread = threading.Thread(
                target=self._loop, name=f"AutoScheduler-{self.name}", daemon=True
            )
            self._thread.start()

        next_str = self._next_run.strftime("%H:%M") if self._next_run else "-"
        return True, (
            f"{self.name} 已启动\n"
            f"执行频率：每天 {len(self._times)} 次\n"
            f"单批最大数量：{self._batch_size}\n"
            f"下次执行时间：{next_str}"
        )

    def stop(self) -> tuple:
        with self._state_lock:
            if self._state == "STOPPED":
                return True, f"{self.name} 已关闭"
            if self._state == "STOPPING":
                return True, f"{self.name} 正在停止"

            self._state = "STOPPING"
            self._stop_event.set()

            if self._thread is not None and self._thread.is_alive():
                if self._executing_batch:
                    return True, (
                        f"已停止后续自动调度，当前 {self.name} 批次仍在运行。\n"
                        f"当前批次完成后，自动调度将正式停止。"
                    )
                return True, f"{self.name} 已停止（调度线程即将退出）"
            return True, f"{self.name} 已停止"

    def status(self) -> str:
        if self._config_error:
            return (
                f"{self.name} 状态：配置错误\n"
                f"错误原因：{self._config_error}\n"
                f"其他命令不受影响"
            )
        with self._state_lock:
            state = self._state
            next_run = self._next_run
            last_result = self._last_result
            last_run_time = self._last_run_time

        state_str = {"RUNNING": "运行中", "STOPPING": "正在停止"}.get(state, "已关闭")
        lines = [
            f"{self.name} 状态：{state_str}",
            f"执行频率：每天 {len(self._times)} 次",
            f"单批最大数量：{self._batch_size}",
        ]
        if state == "STOPPING":
            lines.append("后续调度：已关闭")
        if state == "RUNNING" and next_run:
            lines.append(f"下次执行时间：{next_run.strftime('%H:%M')}")
        lines.append(
            f"上次执行时间：{last_run_time.strftime('%H:%M') if last_run_time else '无'}"
        )
        if last_result is not None and self._format_result_fn:
            try:
                lines.append(f"上次结果：{self._format_result_fn(last_result)}")
            except Exception:
                pass
        if self._remaining_fn is not None:
            try:
                lines.append(f"{self._remaining_label}：{self._remaining_fn()}")
            except Exception:
                pass
        return "\n".join(lines)

    def _loop(self):
        try:
            while not self._stop_event.is_set():
                try:
                    self._tick()
                except Exception as e:
                    logger.error(f"[AutoScheduler-{self.name}] tick 异常: {e}", exc_info=True)
                    if self._stop_event.wait(timeout=30):
                        break
        finally:
            with self._state_lock:
                self._state = "STOPPED"
            logger.info(f"[AutoScheduler-{self.name}] 线程已退出")

    def _tick(self):
        now = datetime.now(TZ)
        next_dt = self._next_run_datetime(now)
        with self._state_lock:
            self._next_run = next_dt

        remaining = (next_dt - now).total_seconds()
        while remaining > 0:
            segment = min(remaining, MAX_WAIT_SEGMENT)
            if self._stop_event.wait(timeout=segment):
                return
            remaining -= segment
        self._trigger()

    def _trigger(self):
        slot = datetime.now(TZ).strftime("%Y-%m-%d %H:%M")
        if slot == self._last_triggered_slot:
            logger.info(f"[AutoScheduler-{self.name}] slot {slot} 已触发过")
            return
        self._last_triggered_slot = slot

        logger.info(f"[AutoScheduler-{self.name}] 触发, slot={slot}")
        self._executing_batch = True
        try:
            result = self._run_fn(self._batch_size)
        except Exception as e:
            logger.error(f"[AutoScheduler-{self.name}] run_fn 异常: {e}", exc_info=True)
            return
        finally:
            self._executing_batch = False
        if getattr(result, "error_detail", "") == "busy":
            logger.info(f"[AutoScheduler-{self.name}] 任务忙，跳过本次")
            return
        self._last_result = result
        self._last_run_time = datetime.now(TZ)

    def _next_run_datetime(self, now: datetime) -> datetime:
        today = now.date()
        for t in self._times:
            candidate = datetime.combine(today, t, tzinfo=TZ)
            if candidate > now:
                return candidate
        tomorrow = today + timedelta(days=1)
        return datetime.combine(tomorrow, self._times[0], tzinfo=TZ)


# ══════════════════════════════════════════════════════
# 全局单例 + 注册表
# ══════════════════════════════════════════════════════

_check_service: Optional[AutoCheckService] = None
_check_service_lock = threading.Lock()
_submit_service: Optional[AutoSubmitService] = None
_submit_service_lock = threading.Lock()
_schedulers: Dict[str, AutoScheduler] = {}
_initialized = False
_init_lock = threading.Lock()


def get_check_service() -> AutoCheckService:
    global _check_service
    if _check_service is None:
        with _check_service_lock:
            if _check_service is None:
                _check_service = AutoCheckService()
    return _check_service


def get_submit_service() -> AutoSubmitService:
    global _submit_service
    if _submit_service is None:
        with _submit_service_lock:
            if _submit_service is None:
                _submit_service = AutoSubmitService()
    return _submit_service


def get_scheduler(name: str) -> Optional[AutoScheduler]:
    return _schedulers.get(name)


def init_auto_tasks():
    """初始化所有自动任务。配置非法不影响其他功能。由 feishu_bot 主入口调用一次。"""
    global _initialized
    with _init_lock:
        if _initialized:
            return
        _initialized = True

    _init_check_scheduler()
    _init_submit_scheduler()


def shutdown_auto_tasks():
    """主程序退出时调用。先停调度，再等当前批次完成（有期限）。"""
    for name, scheduler in list(_schedulers.items()):
        try:
            scheduler.stop()
            if scheduler._thread is not None and scheduler._thread.is_alive():
                scheduler._thread.join(timeout=330)
        except Exception as e:
            logger.warning(f"[auto_tasks] shutdown {name} 异常: {e}")


# ══════════════════════════════════════════════════════
# Check 初始化
# ══════════════════════════════════════════════════════

def _init_check_scheduler():
    raw_schedule = os.getenv("AUTO_CHECK_SCHEDULE", "").strip()
    raw_batch = os.getenv("AUTO_CHECK_BATCH_SIZE", "20").strip()

    try:
        batch_size = int(raw_batch)
        if batch_size <= 0:
            raise ValueError(f"必须为正整数，实际 {raw_batch}")
    except ValueError as e:
        batch_size = 20
        config_error = f"AUTO_CHECK_BATCH_SIZE 非法: {e}"
        logger.error(config_error)
        _schedulers["check"] = AutoScheduler(
            name="自动 Check",
            run_fn=lambda n: None,
            times=[], batch_size=batch_size, config_error=config_error,
        )
        return

    try:
        times = parse_schedule(raw_schedule)
        config_error = ""
    except ValueError as e:
        logger.error(f"AUTO_CHECK_SCHEDULE 非法: {e}")
        try:
            from core.notifier import get_notifier
            get_notifier().send("自动检查配置错误", f"AUTO_CHECK_SCHEDULE 非法: {e}")
        except Exception:
            pass
        times = []
        config_error = f"AUTO_CHECK_SCHEDULE 非法: {e}"

    service = get_check_service()

    def run_check_batch(max_count):
        result = service.run_one_batch(max_count)
        if result.error_detail == "busy":
            return result
        try:
            from core.alpha_db import get_alpha_db
            from core.notifier import get_notifier
            summary = get_alpha_db().get_alpha_summary()
            get_notifier().notify_correlation_check(
                total=result.total_checked,
                passed=result.passed,
                failed=result.failed,
                failed_alphas=result.failed_alphas,
                passed_alphas=result.passed_alphas,
                summary=summary,
                unknown_alphas=result.unknown_alphas,
                already_submitted=result.already_submitted,
                pending=result.pending,
                error=result.error,
            )
        except Exception as e:
            logger.error(f"[auto_check] 通知失败: {e}", exc_info=True)
        return result

    def format_result(r):
        return (
            f"PASS {r.passed} / FAIL {r.failed} / "
            f"PENDING {r.pending} / ERROR {r.error}"
        )

    _schedulers["check"] = AutoScheduler(
        name="自动 Check",
        run_fn=run_check_batch,
        times=times,
        batch_size=batch_size,
        format_result_fn=format_result,
        remaining_fn=_count_pending,
        remaining_label="剩余待检查",
        config_error=config_error,
    )
    logger.info(f"AutoCheckScheduler 初始化完成, config_error={config_error or '无'}")


# ══════════════════════════════════════════════════════
# Submit 初始化
# ══════════════════════════════════════════════════════

def _init_submit_scheduler():
    raw_schedule = os.getenv("AUTO_SUBMIT_SCHEDULE", "").strip()
    raw_max = os.getenv("AUTO_SUBMIT_MAX_PER_DAY", "20") or "20"
    raw_min = os.getenv("AUTO_SUBMIT_MIN_PER_DAY", "4") or "4"
    raw_fit = os.getenv("AUTO_SUBMIT_MIN_FITNESS", "1.1") or "1.1"

    config_error = ""

    try:
        max_per_day = int(raw_max)
    except ValueError:
        max_per_day = 20
        config_error = f"AUTO_SUBMIT_MAX_PER_DAY 非法: {raw_max}"

    try:
        min_per_day = int(raw_min)
    except ValueError:
        min_per_day = 4
        config_error = config_error or f"AUTO_SUBMIT_MIN_PER_DAY 非法: {raw_min}"

    try:
        min_fitness = float(raw_fit)
    except ValueError:
        min_fitness = 1.1
        config_error = config_error or f"AUTO_SUBMIT_MIN_FITNESS 非法: {raw_fit}"

    if not config_error:
        if max_per_day <= 0:
            config_error = f"AUTO_SUBMIT_MAX_PER_DAY 必须 > 0，实际 {max_per_day}"
        elif min_per_day < 0:
            config_error = f"AUTO_SUBMIT_MIN_PER_DAY 不能为负，实际 {min_per_day}"
        elif min_per_day > max_per_day:
            config_error = (
                f"AUTO_SUBMIT_MIN_PER_DAY({min_per_day}) "
                f"> AUTO_SUBMIT_MAX_PER_DAY({max_per_day})"
            )
        elif min_fitness < 0:
            config_error = f"AUTO_SUBMIT_MIN_FITNESS 不能为负，实际 {min_fitness}"

    if not config_error:
        try:
            times = parse_schedule(raw_schedule)
        except ValueError as e:
            times = []
            config_error = f"AUTO_SUBMIT_SCHEDULE 非法: {e}"
            logger.error(f"[auto_submit] {config_error}")

    if config_error:
        logger.error(f"[auto_submit] 配置错误: {config_error}")
        _schedulers["submit"] = AutoScheduler(
            name="自动 Submit",
            run_fn=lambda n: None,
            times=[], batch_size=0, config_error=config_error,
        )
        return

    service = get_submit_service()

    def run_submit_batch(_batch_size):
        result = service.run_one_batch()
        if result.error_detail == "busy":
            return result
        if result.error_detail.startswith("无达标因子"):
            logger.info(f"[auto_submit] {result.error_detail}, 不发送通知")
            return result
        if result.error_detail.startswith("已达每日上限"):
            logger.info(f"[auto_submit] {result.error_detail}, 不发送通知")
            return result
        try:
            _send_submit_notification(result)
        except Exception as e:
            logger.error(f"[auto_submit] 通知失败: {e}", exc_info=True)
        return result

    def format_result(r):
        if r.error_detail:
            return r.error_detail
        return (
            f"目标 {r.target_count} / 成功 {r.submitted} / "
            f"跳过 {r.skipped} / 失败 {r.failed} / "
            f"今日 {r.today_after}"
        )

    # ── 修改点：剩余可提交改为带候选池 + 今日进度 ──
    def remaining_submittable():
        try:
            mf = float(os.getenv("AUTO_SUBMIT_MIN_FITNESS", "1.1") or "1.1")
            pool = len(_query_unsubmitted(min_fitness=mf))
            today = _get_today_submitted()
            can_submit = max(0, max_per_day - today)
            return (
                f"候选池 {pool} 个 / 今日已交 {today}/{max_per_day} / "
                f"今日还可交 {can_submit}"
            )
        except Exception:
            return "查询失败"

    _schedulers["submit"] = AutoScheduler(
        name="自动 Submit",
        run_fn=run_submit_batch,
        times=times,
        batch_size=max_per_day,
        format_result_fn=format_result,
        remaining_fn=remaining_submittable,
        remaining_label="提交状态",     # ← 改了标签
        config_error="",
    )
    logger.info(
        f"AutoSubmitScheduler 初始化完成, schedule={raw_schedule}, "
        f"max={max_per_day}, min={min_per_day}, min_fitness={min_fitness}"
    )


def _send_submit_notification(r: SubmitBatchResult):
    from core.notifier import get_notifier
    notifier = get_notifier()
    if not notifier.enabled:
        return

    min_fitness = os.getenv("AUTO_SUBMIT_MIN_FITNESS", "1.1")
    lines = [
        "## 自动提交结果",
        "",
        f"- Fitness 门槛: **{min_fitness}**",
        f"- 本次目标: **{r.target_count}**",
        f"- 成功提交: **{r.submitted}** ✅",
        f"- 跳过（check 未 PASS）: **{r.skipped}** ⏭️",
        f"- 失败（submit 失败）: **{r.failed}** ❌",
        f"- 今日提交进度: **{r.today_after}/{r.today_before + r.remaining_max}**",
    ]

    if r.submitted_ids:
        lines.extend(["", "### 成功提交"])
        for aid in r.submitted_ids:
            lines.append(f"- `{aid}`")

    if r.skipped_details:
        lines.extend(["", f"### 跳过（{len(r.skipped_details)} 个）"])
        for s in r.skipped_details[:10]:
            lines.append(f"- `{s['alpha_id']}`: {s['reason']}")

    if r.failed_details:
        lines.extend(["", f"### 失败（{len(r.failed_details)} 个）"])
        for f in r.failed_details[:10]:
            lines.append(f"- `{f['alpha_id']}`: {f['error'][:100]}")

    if r.error_detail:
        lines.extend(["", f"### ⚠️ {r.error_detail}"])

    template = "green" if r.submitted > 0 else "orange"
    notifier.send_markdown("自动提交报告", "\n".join(lines), template=template)