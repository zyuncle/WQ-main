"""
Submit alpha to WorldQuant Brain by ID (Hardened Production Version).

Usage:
    python submit_alpha.py <alpha_id> [alpha_id2 ...]
    python submit_alpha.py --keep-failed pwnbR9Gq akNmojM1
    python submit_alpha.py --type REGULAR pwnbR9Gq
"""

import os
import sys
import time
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any, Tuple

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv
load_dotenv()

import requests
from requests.auth import HTTPBasicAuth
from core.config import load_credentials
from core.alpha_db import get_alpha_db

# ==================== 配置常量 ====================
BASE_URL = "https://api.worldquantbrain.com"
DEFAULT_TIMEOUT = 15
MAX_POLL_WAIT = 300       # 单个因子最大轮询等待秒数
MAX_POLL_RETRIES = 3      # 轮询期间单次 GET 网络异常的最大连续重试次数

# 默认提交类型参数（兼容平台对请求体 {"type": "REGULAR"} 的要求）
DEFAULT_SUBMIT_TYPE = os.getenv("WQB_SUBMIT_TYPE", "REGULAR")

# 【自动删除开关】True: 当平台明确返回硬性指标 Check FAIL 时自动从本地数据库删除
DEFAULT_DELETE_ON_CHECK_FAIL = os.getenv("WQB_DELETE_ON_FAIL", "true").lower() in ("true", "1", "yes")

# 强制开启 TLS 证书验证，避免 Basic Auth 凭据被中间人截获。
SSL_VERIFY = True


# ==================== 结构化结果定义 ====================
@dataclass
class SubmitResult:
    alpha_id: str
    success: bool = False
    already_submitted: bool = False
    http_status: Optional[int] = None
    verified_status: Optional[str] = None
    error_category: Optional[str] = None  # CHECK_FAILED, AUTH_ERROR, RATE_LIMIT, TIMEOUT_UNKNOWN, NETWORK_ERROR, API_ERROR, VERIFY_FAILED
    error_message: Optional[str] = None
    failed_checks: List[str] = field(default_factory=list)
    response_body: Optional[Dict[str, Any]] = None

    @property
    def is_safe_to_delete(self) -> bool:
        """
        只有明确属于硬性指标检查未通过 (CHECK_FAILED)，
        且拿到了具体的失败项列表（不包含 ALREADY_SUBMITTED），才允许安全删除。
        """
        if self.success or self.already_submitted:
            return False
        if self.error_category != "CHECK_FAILED" or not self.failed_checks:
            return False
        if any("ALREADY_SUBMITTED" in check for check in self.failed_checks):
            return False
        return True


# ==================== 核心辅助函数 ====================
def parse_failed_checks(data: Dict[str, Any]) -> List[str]:
    """从 API 响应 JSON 中提取所有未通过 (FAIL) 的检查项详情"""
    if not isinstance(data, dict):
        return []
    checks = data.get("is", {}).get("checks", [])
    details = []
    for check in checks:
        if check.get("result") == "FAIL":
            name = check.get("name", "UNKNOWN")
            value = check.get("value", "N/A")
            limit = check.get("limit", "N/A")
            details.append(f"{name}: value={value}, limit={limit}")
    return details


def is_status_submitted(remote_status: Optional[str]) -> bool:
    """
    【加固 1】严格使用白名单判断远端状态是否已提交，
    防止将 CHECKED 等中间状态误判为已提交。
    """
    if not remote_status:
        return False
    return remote_status.upper() in ("ACTIVE", "SUBMITTED")


def get_remote_alpha_status(
    session: requests.Session, alpha_id: str
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    【加固 4】查询平台侧 Alpha 的真实状态，并向上传递 429/401 等关键错误信号。
    返回值: (status, error_category, error_message)
    """
    url = f"{BASE_URL}/alphas/{alpha_id}"
    try:
        resp = session.get(url, verify=SSL_VERIFY, timeout=DEFAULT_TIMEOUT)

        # 若遇 429 限流，按 Retry-After 退避重试一次
        if resp.status_code == 429:
            try:
                retry_after = float(resp.headers.get("Retry-After", 5))
            except (ValueError, TypeError):
                retry_after = 5.0
            time.sleep(max(retry_after, 1.0))
            resp = session.get(url, verify=SSL_VERIFY, timeout=DEFAULT_TIMEOUT)
            if resp.status_code == 429:
                return None, "RATE_LIMIT", "Rate limited (HTTP 429) during remote status check."

        if resp.status_code == 200:
            return resp.json().get("status"), None, None
        elif resp.status_code == 401:
            return None, "AUTH_ERROR", "Session expired or unauthorized (HTTP 401) during status check."
        else:
            return None, "API_ERROR", f"Status check returned HTTP {resp.status_code}: {resp.text[:100]}"

    except requests.exceptions.RequestException as e:
        return None, "NETWORK_ERROR", f"Network error during status check: {e}"
    except ValueError as e:
        return None, "API_ERROR", f"Invalid JSON during status check: {e}"


def extract_retry_after(headers: Dict[str, Any]) -> Optional[float]:
    """
    解析响应头中的 Retry-After。
    如果不存在该头部或值 <= 0，返回 None 表示轮询任务已结束。
    """
    if "Retry-After" not in headers:
        return None
    try:
        val = float(headers["Retry-After"])
        return val if val > 0 else None
    except (ValueError, TypeError):
        return 3.0


# ==================== 四阶段主流程函数 ====================
def process_alpha_submission(
    session: requests.Session,
    alpha_id: str,
    submit_type: str = DEFAULT_SUBMIT_TYPE,
    max_wait: int = MAX_POLL_WAIT,
) -> SubmitResult:
    """执行完整的 预检 -> 提交 -> 轮询 -> 终态核验 流程"""
    result = SubmitResult(alpha_id=alpha_id)

    # ---------- 阶段 1：远端状态预检 ----------
    remote_status, pre_err_cat, pre_err_msg = get_remote_alpha_status(session, alpha_id)
    result.verified_status = remote_status

    # 【加固 4】若预检命中 429 限流或 401 认证失败，立即中止并返回对应信号，不继续发起 POST
    if pre_err_cat in ("RATE_LIMIT", "AUTH_ERROR"):
        result.error_category = pre_err_cat
        result.error_message = pre_err_msg
        return result

    if is_status_submitted(remote_status):
        result.success = True
        result.already_submitted = True
        return result

    # ---------- 阶段 2：发起提交请求 ----------
    submit_url = f"{BASE_URL}/alphas/{alpha_id}/submit"
    payload = {"type": submit_type} if submit_type else None

    try:
        # 【加固 3】保留 json={"type": "REGULAR"} 请求体以兼容平台参数规范
        resp = session.post(submit_url, json=payload, verify=SSL_VERIFY, timeout=DEFAULT_TIMEOUT)
        result.http_status = resp.status_code
    except requests.exceptions.Timeout:
        verified, _, _ = get_remote_alpha_status(session, alpha_id)
        result.verified_status = verified
        if is_status_submitted(verified):
            result.success = True
            return result
        result.error_category = "TIMEOUT_UNKNOWN"
        result.error_message = "POST request timed out; remote status remains unconfirmed."
        return result
    except requests.exceptions.RequestException as e:
        result.error_category = "NETWORK_ERROR"
        result.error_message = f"POST connection failed: {e}"
        return result

    if resp.status_code != 201:
        try:
            body = resp.json()
            result.response_body = body
            failed = parse_failed_checks(body)
            if failed:
                result.failed_checks = failed
                if any("ALREADY_SUBMITTED" in f for f in failed):
                    result.success = True
                    result.already_submitted = True
                    result.verified_status = "SUBMITTED"
                else:
                    result.error_category = "CHECK_FAILED"
                    result.error_message = "Pre-submission checks failed."
                return result
        except ValueError:
            pass

        text_snippet = resp.text[:200] if resp.text else f"HTTP {resp.status_code}"
        if resp.status_code == 401:
            result.error_category = "AUTH_ERROR"
            result.error_message = "Session expired or unauthorized (HTTP 401)."
        elif resp.status_code == 429:
            result.error_category = "RATE_LIMIT"
            result.error_message = f"Rate limited by server (HTTP 429): {text_snippet}"
        elif "ALREADY_SUBMITTED" in text_snippet:
            result.success = True
            result.already_submitted = True
            result.verified_status = "SUBMITTED"
        else:
            result.error_category = "API_ERROR"
            result.error_message = f"HTTP {resp.status_code}: {text_snippet}"
        return result

    # ---------- 阶段 3：异步轮询 (Submit -> Poll) ----------
    poll_url = resp.headers.get("Location", submit_url)
    start_time = time.time()
    consecutive_net_errors = 0

    # 【加固 2】清晰控制 Retry-After 状态机：若初始或后续响应 Retry-After 缺失或为 0，直接结束轮询
    retry_wait = extract_retry_after(resp.headers)
    if retry_wait is None:
        # 201 响应通常会指示首次轮询间隔，若未携带则默认给 3 秒初始等待以触发首次轮询结果获取
        retry_wait = 3.0

    while time.time() - start_time < max_wait:
        time.sleep(retry_wait)

        try:
            resp = session.get(poll_url, verify=SSL_VERIFY, timeout=DEFAULT_TIMEOUT)
            result.http_status = resp.status_code
            consecutive_net_errors = 0
        except requests.exceptions.RequestException as e:
            consecutive_net_errors += 1
            if consecutive_net_errors >= MAX_POLL_RETRIES:
                result.error_category = "NETWORK_ERROR"
                result.error_message = f"Polling interrupted after {MAX_POLL_RETRIES} network errors: {e}"
                break
            retry_wait = 3.0
            continue

        if resp.status_code == 429:
            retry_wait = extract_retry_after(resp.headers) or 5.0
            continue

        # 检查本次轮询响应是否仍包含大于 0 的 Retry-After
        next_wait = extract_retry_after(resp.headers)
        if next_wait is not None:
            retry_wait = next_wait
            continue

        # 【加固 2】当 Retry-After 消失或为 0 时，不再发起多余请求，直接解析最终响应并 break
        try:
            body = resp.json() if resp.text else {}
            result.response_body = body
        except ValueError:
            body = {}

        failed = parse_failed_checks(body)
        if failed:
            result.failed_checks = failed
            result.error_category = "CHECK_FAILED"
            result.error_message = "IS/Submission checks failed after polling."
            verified, _, _ = get_remote_alpha_status(session, alpha_id)
            result.verified_status = verified
            return result

        if resp.status_code in (200, 201):
            break
        else:
            text_snippet = resp.text[:200] if resp.text else f"HTTP {resp.status_code}"
            result.error_category = "API_ERROR"
            result.error_message = f"Polling finished with HTTP {resp.status_code}: {text_snippet}"
            verified, _, _ = get_remote_alpha_status(session, alpha_id)
            result.verified_status = verified
            return result
    else:
        result.error_category = "TIMEOUT_UNKNOWN"
        result.error_message = f"Polling timed out after {max_wait}s."

    # ---------- 阶段 4：平台终态核验 ----------
    final_status, post_err_cat, post_err_msg = get_remote_alpha_status(session, alpha_id)
    result.verified_status = final_status

    if is_status_submitted(final_status):
        result.success = True
        result.error_category = None
        result.error_message = None
    else:
        if not result.error_category:
            result.error_category = post_err_cat or "VERIFY_FAILED"
            result.error_message = post_err_msg or (
                f"Polling completed without explicit errors, "
                f"but platform status is '{final_status or 'UNKNOWN'}' (expected ACTIVE/SUBMITTED)."
            )

    return result


# ==================== 数据库辅助与主入口 ====================
def get_local_alpha_status(db, alpha_id: str) -> Optional[str]:
    try:
        with db._cursor() as cur:
            cur.execute("SELECT status FROM alphas WHERE alpha_id = ?", (alpha_id,))
            row = cur.fetchone()
            return row["status"] if row else None
    except Exception:
        return None


def sync_local_db_submitted(db, alpha_id: str):
    try:
        rows = db.update_alpha_status(alpha_id, "submitted")
        if rows > 0:
            print(f"  [DB SYNC] 本地数据库状态已更新为 'submitted'")
        else:
            print(f"  [DB WARN] 平台已提交，但本地数据库中未找到该 Alpha")
    except Exception as e:
        print(f"  [DB ERROR] 本地数据库更新异常: {e}")


def main():
    raw_args = sys.argv[1:]
    if not raw_args or "-h" in raw_args or "--help" in raw_args:
        print(__doc__)
        sys.exit(0)

    # 解析命令行参数
    delete_on_fail = DEFAULT_DELETE_ON_CHECK_FAIL
    submit_type = DEFAULT_SUBMIT_TYPE
    alpha_ids = []

    i = 0
    while i < len(raw_args):
        arg = raw_args[i]
        if arg == "--keep-failed":
            delete_on_fail = False
        elif arg == "--delete-failed":
            delete_on_fail = True
        elif arg == "--type" and i + 1 < len(raw_args):
            submit_type = raw_args[i + 1]
            i += 1
        elif not arg.startswith("-"):
            alpha_ids.append(arg)
        i += 1

    if not alpha_ids:
        print("[ERROR] No alpha IDs provided.")
        print(__doc__)
        sys.exit(1)

    # 1. 认证会话初始化
    username, password = load_credentials()
    session = requests.Session()
    session.auth = HTTPBasicAuth(username, password)

    print(
        f"Authenticating with WorldQuant Brain "
        f"(Type: {submit_type}, Auto-Delete on Check Fail: {delete_on_fail})..."
    )
    try:
        auth_resp = session.post(f"{BASE_URL}/authentication", verify=SSL_VERIFY, timeout=DEFAULT_TIMEOUT)
    except requests.exceptions.RequestException as e:
        print(f"[FATAL] Authentication network failure: {e}")
        sys.exit(1)

    if auth_resp.status_code != 201:
        print(f"[FATAL] Authentication failed (HTTP {auth_resp.status_code}): {auth_resp.text[:200]}")
        sys.exit(1)

    print("Authentication successful.\n" + "=" * 55)

    db = get_alpha_db()

    stats = {
        "newly_submitted": [],
        "already_submitted_synced": [],
        "check_failed_deleted": [],
        "check_failed_kept": [],
        "pending_verification": [],
    }

    # 2. 逐个处理 Alpha
    for idx, alpha_id in enumerate(alpha_ids, 1):
        print(f"\n[{idx}/{len(alpha_ids)}] Processing Alpha: {alpha_id}")
        local_status = get_local_alpha_status(db, alpha_id)

        res = process_alpha_submission(session, alpha_id, submit_type=submit_type)

        if res.error_category == "AUTH_ERROR":
            print(f"  [ABORT] {res.error_message} 终止批处理。")
            stats["pending_verification"].append(alpha_id)
            break

        # 情况 A：远端原本已提交
        if res.success and res.already_submitted:
            print(f"  [SKIP] 平台显示该 Alpha 已处于提交状态 (remote={res.verified_status})")
            if local_status != "submitted":
                sync_local_db_submitted(db, alpha_id)
            stats["already_submitted_synced"].append(alpha_id)

        # 情况 B：本次新提交成功
        elif res.success:
            print(f"  [OK] 提交成功并已通过平台核验 (remote={res.verified_status})")
            sync_local_db_submitted(db, alpha_id)
            stats["newly_submitted"].append(alpha_id)

        # 情况 C：明确的硬性指标检查未通过（可触发安全自动删除）
        elif res.is_safe_to_delete:
            print(f"  [FAIL] 提交未通过平台指标检查 ({res.error_message})")
            for detail in res.failed_checks:
                print(f"    - {detail}")

            if delete_on_fail:
                try:
                    rows = db.delete_alpha_by_alpha_id(alpha_id)
                    if rows > 0:
                        print(f"  [DELETED] 指标不合格，已从本地数据库中自动删除")
                    else:
                        print(f"  [DB INFO] 本地数据库中原本不存在该记录")
                    stats["check_failed_deleted"].append(alpha_id)
                except Exception as e:
                    print(f"  [DB ERROR] 自动删除失败: {e}")
                    stats["check_failed_kept"].append(alpha_id)
            else:
                print(f"  [KEEP] 已保留本地记录（当前未开启自动删除）")
                stats["check_failed_kept"].append(alpha_id)

        # 情况 D：超时、限流、网络异常或未知的 API 错误（强制保护，不删除）
        else:
            print(f"  [PROTECTED/PENDING] 提交未完成: [{res.error_category}] {res.error_message}")
            print(f"  [KEEP] 非硬性指标错误，已触发保护机制保留数据库记录，可稍后重试")
            stats["pending_verification"].append(alpha_id)

    # 3. 批处理统计汇总
    print("\n" + "=" * 55)
    print("Batch Submission Summary:")
    print(f"  - 总计处理 (Total Processed)        : {len(alpha_ids)}")
    print(f"  - 本次新提交成功 (Newly Submitted)  : {len(stats['newly_submitted'])} {stats['newly_submitted'] or ''}")
    print(f"  - 远端已提交/同步 (Already Active)  : {len(stats['already_submitted_synced'])} {stats['already_submitted_synced'] or ''}")
    print(f"  - 检查未过已删除 (Failed & Deleted) : {len(stats['check_failed_deleted'])} {stats['check_failed_deleted'] or ''}")
    print(f"  - 检查未过已保留 (Failed & Kept)    : {len(stats['check_failed_kept'])} {stats['check_failed_kept'] or ''}")
    print(f"  - 保护保留/待重试 (Protected/Retry) : {len(stats['pending_verification'])} {stats['pending_verification'] or ''}")
    print(f"  - 本地数据库剩余总数 (DB Total)     : {db.count_alphas()}")
    print("=" * 55)


if __name__ == "__main__":
    main()