"""
通知模块 — 飞书 Webhook 通知，支持 Alpha 发现、定期汇总、异常熔断报警。
"""

import os
import logging
import requests
from typing import Optional

logger = logging.getLogger(__name__)

# 明细表格最多列多少行，超出部分用「共 N 个」汇总
MAX_DETAIL_ROWS = 10


def _fmt_num(value, digits: int = 4) -> str:
    """把可能为 None / 字符串的指标格式化成单元格文本，避免格式化抛 TypeError。"""
    try:
        return f"{float(value):.{digits}f}"
    except (TypeError, ValueError):
        return "-"


# 卡片里用两字母缩写，避免 SPECTACULAR 在手机上换行；未知值原样显示
GRADE_ABBR = {
    "SPECTACULAR": "SP",
    "EXCELLENT": "EX",
    "GOOD": "GD",
    "AVERAGE": "AV",
    "INFERIOR": "IN",
}


def _fmt_grade(value) -> str:
    if not value:
        return "-"
    text = str(value).strip()
    return GRADE_ABBR.get(text.upper(), text)


class Notifier:
    """飞书 Webhook 通知器。"""

    def __init__(self):
        self.webhook_url: Optional[str] = os.getenv("FEISHU_WEBHOOK")
        if self.webhook_url:
            logger.info("飞书通知已启用")
        else:
            logger.info("未配置 FEISHU_WEBHOOK，通知已禁用")

        # 熔断计数器
        self._consecutive_auth_failures = 0
        self._consecutive_llm_errors = 0

    @property
    def enabled(self) -> bool:
        return bool(self.webhook_url)

    def send(self, title: str, content: str) -> bool:
        """发送飞书消息（富文本格式）。返回是否成功。"""
        if not self.webhook_url:
            return False

        try:
            resp = requests.post(
                self.webhook_url,
                json={
                    "msg_type": "post",
                    "content": {
                        "post": {
                            "zh_cn": {
                                "title": title,
                                "content": [[{"tag": "text", "text": content}]],
                            }
                        }
                    },
                },
                timeout=10,
            )
            if resp.status_code == 200:
                data = resp.json()
                if data.get("code") == 0:
                    logger.info(f"飞书通知发送成功: {title}")
                    return True
                else:
                    logger.warning(f"飞书通知失败: {data}")
            else:
                logger.warning(f"飞书通知 HTTP {resp.status_code}")
        except Exception as e:
            # requests 异常文本可能包含完整 webhook URL（含访问令牌），禁止写入日志。
            logger.warning(f"飞书通知异常: {type(e).__name__}")
        return False

    def send_markdown(self, title: str, markdown: str, template: str = "blue") -> bool:
        """发送飞书 Markdown 卡片消息。返回是否成功。"""
        if not self.webhook_url:
            return False

        # 飞书卡片 V2 格式
        payload = {
            "msg_type": "interactive",
            "card": {
                "schema": "2.0",
                "header": {
                    "title": {"tag": "plain_text", "content": title},
                    "template": template
                },
                "body": {
                    "elements": [
                        {
                            "tag": "markdown",
                            "content": markdown
                        }
                    ]
                }
            }
        }

        try:
            resp = requests.post(
                self.webhook_url,
                json=payload,
                timeout=10,
            )
            if resp.status_code == 200:
                data = resp.json()
                if data.get("code") == 0:
                    logger.info(f"飞书 Markdown 通知发送成功: {title}")
                    return True
                else:
                    logger.warning(f"飞书通知失败: {data}")
            else:
                logger.warning(f"飞书通知 HTTP {resp.status_code}")
        except Exception as e:
            # requests 异常文本可能包含完整 webhook URL（含访问令牌），禁止写入日志。
            logger.warning(f"飞书通知异常: {type(e).__name__}")
        return False

    # ── Alpha 发现通知 ──────────────────────────────────────────────

    def notify_alpha(
        self,
        alpha_id: str,
        sharpe: float,
        fitness: float,
        turnover: float,
        expression: str,
        member_id: str = "",
        returns: float = None,
        drawdown: float = None,
        margin: float = None,
        long_count: int = None,
        short_count: int = None,
        grade: str = None,
    ):
        """发现符合条件的 Alpha 时发送通知（Markdown 卡片格式）。"""
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # expr_short = expression[:80] + ("..." if len(expression) > 80 else "")

        lines = [
            f"**发现时间:** {timestamp}",
            "",
            "## Alpha 信息",
            f"- **ID:** {alpha_id}",
            f"- **Member:** {member_id or 'N/A'}",
            "",
            "## 指标",
            "| 指标 | 值 |",
            "|------|------|",
            f"| Sharpe | **{sharpe:.2f}** |",
            f"| Fitness | **{fitness:.2f}** |",
            f"| Turnover | **{turnover:.2f}** |",
        ]

        # 可选指标：有值才显示
        if returns is not None:
            lines.append(f"| Returns | **{returns * 100:.2f}%** |")
        if drawdown is not None:
            lines.append(f"| Drawdown | **{drawdown * 100:.2f}%** |")
        if margin is not None:
            lines.append(f"| Margin | **{margin:.6f}** |")
        if long_count is not None or short_count is not None:
            lc = long_count if long_count is not None else "-"
            sc = short_count if short_count is not None else "-"
            lines.append(f"| Long/Short | **{lc} / {sc}** |")
        if grade:
            lines.append(f"| Grade | **{grade}** |")

        lines.extend([
            "",
            "## Expression",
            f"```\n{expression}\n```",
        ])

        # self.send_markdown("发现新 Alpha!", "\n".join(lines), template="green")
        title = "发现新 Alpha!"
        if grade:
            title = f"发现新 Alpha! [{grade}]"
        
        self.send_markdown(title, "\n".join(lines), template="green")

    # ── 相关性检查通知 ──────────────────────────────────────────────

    # [CHANGED] 增加 unknown_alphas 参数：未识别状态的 alpha 原样展示在通知里
    def notify_correlation_check(
        self,
        total: int,
        passed: int,
        failed: int,
        failed_alphas: list,
        summary: dict = None,
        unknown_alphas: list = None,
        already_submitted: int = 0,   # [CHANGED]
        pending: int = 0,             # [CHANGED]
        error: int = 0,               # [CHANGED]
        passed_alphas: list = None,   # 通过相关性检查的因子明细
    ):
        """相关性检查结果通知（Markdown 卡片格式）。"""
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        lines = [
            f"**检查时间:** {timestamp}",
            "",
            "## 相关性检查结果",
            f"- 检查总数: **{total}**",
            f"- PASS: **{passed}** ✅",
            f"- FAIL: **{failed}** ❌",
            f"- 已提交过: **{already_submitted}** ℹ️",   # [CHANGED]
            f"- 仍 PENDING: **{pending}** ⏳",             # [CHANGED]
            f"- ERROR: **{error}** ⚠️",                   # [CHANGED]
        ]

        if failed_alphas:
            lines.append("")
            lines.append(f"## 失败详情（{len(failed_alphas)} 个）")
            lines.append("| Alpha ID | 相关性值 | 限制值 |")
            lines.append("|----------|----------|--------|")
            for alpha in failed_alphas[:MAX_DETAIL_ROWS]:
                lines.append(
                    f"| {alpha.get('alpha_id', '-')} "
                    f"| {_fmt_num(alpha.get('value'))} "
                    f"| {_fmt_num(alpha.get('limit'), 2)} |"
                )
            if len(failed_alphas) > MAX_DETAIL_ROWS:
                lines.append(f"| ... | 共 {len(failed_alphas)} 个 | |")

        # 通过的因子：可直接进入提交流程，附 Grade / Fitness / Sharpe 方便挑选
        if passed_alphas:
            lines.append("")
            lines.append(f"## 通过详情（{len(passed_alphas)} 个）")
            lines.append("| Alpha ID | 相关性值 | Grade | Fitness | Sharpe |")
            lines.append("|----------|----------|-------|---------|--------|")
            for alpha in passed_alphas[:MAX_DETAIL_ROWS]:
                lines.append(
                    f"| {alpha.get('alpha_id', '-')} "
                    f"| {_fmt_num(alpha.get('value'))} "
                    f"| {_fmt_grade(alpha.get('grade'))} "
                    f"| {_fmt_num(alpha.get('fitness'), 2)} "
                    f"| {_fmt_num(alpha.get('sharpe'), 2)} |"
                )
            if len(passed_alphas) > MAX_DETAIL_ROWS:
                lines.append(f"| ... | 共 {len(passed_alphas)} 个 | | | |")

        # [CHANGED] 未识别状态：列出 alpha_id、状态名、以及原始 checks 的 name=result
        if unknown_alphas:
            lines.append("")
            lines.append(f"## ⚠️ 未识别状态（{len(unknown_alphas)} 个，需人工确认）")
            lines.append("| Alpha ID | 状态 | checks 原文 |")
            lines.append("|----------|------|-------------|")
            for u in unknown_alphas[:MAX_DETAIL_ROWS]:
                detail = ", ".join(
                    f"{c.get('name')}={c.get('result')}"
                    for c in (u.get("checks") or [])
                ) or "空 checks"
                lines.append(f"| {u.get('alpha_id')} | {u.get('status')} | {detail} |")
            if len(unknown_alphas) > MAX_DETAIL_ROWS:
                lines.append(f"| ... | 共 {len(unknown_alphas)} 个 | |")

        if summary:
            lines.append("")
            lines.append("## 因子库汇总")
            lines.append(f"- 因子总数: **{summary.get('total', 0)}**")
            lines.append(f"- 已提交: **{summary.get('submitted', 0)}**")
            lines.append(f"- 待检查: **{summary.get('pending', 0)}**")
            lines.append(f"- 可提交: **{summary.get('unsubmitted', 0)}**")
            lines.append("")
            lines.append("## 累计统计")
            lines.append(f"- 全部因子: **{summary.get('new_all_time', 0)}**")
            lines.append(f"- 可提交: **{summary.get('submittable_all_time', 0)}**")

        # [CHANGED] 有未识别状态时卡片用橙色警示，否则保持默认蓝色
        template = "orange" if unknown_alphas else "blue"
        self.send_markdown("相关性检查报告", "\n".join(lines), template=template)

    # ── 定期汇总通知 ────────────────────────────────────────────────

    def notify_summary(
        self,
        tested: int,
        passed: int,
        failed: int,
        best_sharpe: float,
        best_fitness: float,
        rescue_pool: int,
        member_id: str = "",
    ):
        """定期汇总通知（Markdown 卡片格式）。"""
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        lines = [
            f"**汇总时间:** {timestamp}",
            "",
            "## 测试结果",
            "| 项目 | 数量 |",
            "|------|------|",
            f"| 已测试 | **{tested}** |",
            f"| 通过 | **{passed}** ✅ |",
            f"| 失败 | **{failed}** ❌ |",
            "",
            "## 最佳指标",
            f"- Sharpe: **{best_sharpe:.2f}**",
            f"- Fitness: **{best_fitness:.2f}**",
            "",
            f"**Rescue Pool:** {rescue_pool}",
        ]
        if member_id:
            lines.append(f"**Member:** {member_id}")

        self.send_markdown("挖矿进度汇总", "\n".join(lines), template="orange")

    # ── 异常熔断报警 ────────────────────────────────────────────────

    def record_auth_failure(self):
        """记录一次鉴权失败，连续 3 次触发熔断报警。"""
        self._consecutive_auth_failures += 1
        if self._consecutive_auth_failures >= 3:
            self.send(
                "矿机宕机警告",
                "连续 {} 次鉴权失败 (AUTH_FAILED)\n"
                "Token 可能已过期，矿机已停止运行。\n"
                "请检查账号密码或重新登录。".format(
                    self._consecutive_auth_failures
                ),
            )

    def record_auth_success(self):
        """鉴权成功，重置计数器。"""
        self._consecutive_auth_failures = 0

    def record_llm_error(self):
        """记录一次 LLM 调用失败，连续 5 次触发熔断报警。"""
        self._consecutive_llm_errors += 1
        if self._consecutive_llm_errors >= 5:
            self.send(
                "矿机宕机警告",
                "连续 {} 次 LLM 调用失败\n"
                "LLM API 额度可能已耗尽。\n"
                "请检查 API Key 余额。".format(
                    self._consecutive_llm_errors
                ),
            )

    def record_llm_success(self):
        """LLM 调用成功，重置计数器。"""
        self._consecutive_llm_errors = 0

    def notify_fatal(self, reason: str, member_id: str = ""):
        """致命错误导致矿机停止时发送最高级别警告。"""
        lines = [reason]
        if member_id:
            lines.append("Member: {}".format(member_id))
        self.send("矿机宕机警告", "\n".join(lines))


_notifier: Optional[Notifier] = None


def get_notifier() -> Notifier:
    global _notifier
    if _notifier is None:
        _notifier = Notifier()
    return _notifier