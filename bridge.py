#!/usr/bin/env python3
"""Feishu <-> Orca bridge.

Milestone 1: receive a Feishu message, route it, reply.

    phone Feishu  ->  Feishu cloud  --WSS(long-conn, outbound)-->  this process
                                                                        |
                                              lark-cli event consume -> NDJSON
                                                                        v
                                                            route() -> reply text
                                                                        |
                                              lark-cli im +messages-reply

The long connection is outbound, so this works behind NAT / campus network
without a public IP, port forwarding, or a tunnel.

Run:  python bridge.py
Test: python bridge.py --selftest
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
from collections import OrderedDict, deque
from dataclasses import dataclass
from pathlib import Path

READY_MARKER = "[event] ready event_key="
DEFAULT_EVENT_KEY = "im.message.receive_v1"
HERE = Path(__file__).resolve().parent

log = logging.getLogger("bridge")


# --------------------------------------------------------------------------
# locating lark-cli
# --------------------------------------------------------------------------

def find_lark_cmd(override: str | None = None) -> list[str]:
    """Resolve the command prefix that runs lark-cli.

    Order: --lark-cli flag > LARK_CLI env > `node .../scripts/run.js` > PATH shim.

    We deliberately prefer running the JS entry through node. On Windows the
    npm-installed `lark-cli.cmd` goes through cmd.exe, which re-parses the
    argument string: an agent reply containing `|`, `&`, `%`, `^` or unbalanced
    quotes gets mangled or executed. Passing argv straight to node avoids
    cmd.exe entirely.
    """
    if override:
        return [override]
    env = os.environ.get("LARK_CLI")
    if env:
        return [env]

    appdata = os.environ.get("APPDATA")
    if appdata:
        run_js = Path(appdata) / "npm" / "node_modules" / "@larksuite" / "cli" / "scripts" / "run.js"
        if run_js.is_file():
            node = shutil.which("node")
            if node:
                return [node, str(run_js)]

    for name in ("lark-cli.exe", "lark-cli.cmd", "lark-cli"):
        hit = shutil.which(name)
        if hit:
            return [hit]
    return ["lark-cli"]


def run_lark(cmd: list[str], args: list[str], timeout: float = 30.0) -> subprocess.CompletedProcess:
    """Run a one-shot lark-cli command with UTF-8 decoding.

    Explicit encoding is required: on a Chinese-locale Windows box the default
    is cp936 and every Chinese reply would come back as mojibake.
    """
    return subprocess.run(
        [*cmd, *args],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout,
    )


# --------------------------------------------------------------------------
# event consumer
# --------------------------------------------------------------------------

class EventConsumer:
    """Owns one `lark-cli event consume` child process.

    Subprocess contract (from the lark-event skill):
      * stderr emits `[event] ready event_key=<key>` once the websocket is up;
        only then is stdout meaningful.
      * stdin EOF is a graceful-shutdown signal, so we hold the pipe open for
        the whole run and close it to stop.
      * SIGTERM is safe; SIGKILL leaks server-side subscriptions. Never -9.
    """

    def __init__(
        self,
        cmd: list[str],
        event_key: str,
        identity: str = "bot",
        extra_args: list[str] | None = None,
    ):
        self.cmd = cmd
        self.event_key = event_key
        self.identity = identity
        self.extra_args = extra_args or []
        self.proc: subprocess.Popen | None = None
        self.ready = threading.Event()
        self.stderr_tail: deque[str] = deque(maxlen=60)
        self._stderr_thread: threading.Thread | None = None

    def start(self, timeout: float = 90.0) -> None:
        argv = [
            *self.cmd,
            "event", "consume", self.event_key,
            "--as", self.identity,
            *self.extra_args,
        ]
        log.info("starting: %s", " ".join(argv))
        self.proc = subprocess.Popen(
            argv,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
        )
        self._stderr_thread = threading.Thread(
            target=self._drain_stderr, name="lark-stderr", daemon=True
        )
        self._stderr_thread.start()

        if not self.ready.wait(timeout):
            tail = "\n".join(self.stderr_tail) or "(no stderr)"
            raise RuntimeError(
                f"lark-cli event consume never reported ready within {timeout:.0f}s.\n"
                f"stderr tail:\n{tail}"
            )
        log.info("event bus ready (key=%s)", self.event_key)

    def _drain_stderr(self) -> None:
        assert self.proc and self.proc.stderr
        for line in self.proc.stderr:
            line = line.rstrip("\r\n")
            if not line:
                continue
            self.stderr_tail.append(line)
            if line.startswith(READY_MARKER):
                self.ready.set()
                log.info(line)
            elif "[source]" in line:
                log.info(line)
            elif "WARN" in line or "error" in line.lower():
                log.warning("consumer: %s", line)
            else:
                log.debug("consumer: %s", line)

    def events(self):
        """Yield decoded event dicts. Returns when the child exits."""
        assert self.proc and self.proc.stdout
        for line in self.proc.stdout:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                log.warning("non-JSON line from consumer: %r", line[:200])

    def stop(self) -> None:
        if not self.proc:
            return
        log.info("stopping consumer (closing stdin, then SIGTERM)")
        try:
            if self.proc.stdin:
                self.proc.stdin.close()
        except OSError:
            pass
        try:
            self.proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                log.warning("consumer did not exit after SIGTERM; leaving it (do not SIGKILL)")
        log.info("consumer exited with code %s", self.proc.returncode)


# --------------------------------------------------------------------------
# reply
# --------------------------------------------------------------------------

@dataclass
class Reply:
    """A reply plus how to render it.

    markdown=True goes out as a `post` message with an `md` block, which gives
    us bold headings and paragraph breaks. On a phone-width screen that is the
    difference between a scannable list and a wall of wrapped text.
    """

    text: str
    markdown: bool = False


def send_reply(
    cmd: list[str], message_id: str, text: str, max_chars: int, markdown: bool = False
) -> bool:
    body = text if len(text) <= max_chars else text[: max_chars - 20] + "\n…(已截断)"
    res = run_lark(
        cmd,
        [
            "im", "+messages-reply",
            "--as", "bot",
            "--message-id", message_id,
            "--markdown" if markdown else "--text", body,
            "--idempotency-key", message_id,
            "--json",
        ],
    )
    if res.returncode != 0:
        log.error("reply failed rc=%s stderr=%s", res.returncode, (res.stderr or "").strip()[:500])
        return False
    log.info("replied to %s (%d chars)", message_id, len(body))
    return True


# --------------------------------------------------------------------------
# routing
# --------------------------------------------------------------------------

HELP = """\
**Feishu ⇄ Orca** · 里程碑 3

**会话**
`/sessions` 有哪些 agent 会话、谁在跑
`/to myapp 跑一下测试` 点名发给某个会话
`/send 继续` 发给当前焦点会话

**看**
`/tail` 读焦点会话最后输出
`/wt` workspace
`/term` 终端（调试用）

**闸门**
`/arm` 解锁写操作 5 分钟
`/disarm` 立刻上锁

`/ping` `/id` `/help`"""


def normalize(text: str) -> str:
    """Strip leading @-mentions that Feishu renders into group message text."""
    text = text.strip()
    while True:
        if text.startswith("@"):
            parts = text.split(None, 1)
            if len(parts) == 2:
                text = parts[1].strip()
                continue
        if text.startswith("@_user_"):
            parts = text.split(None, 1)
            if len(parts) == 2:
                text = parts[1].strip()
                continue
        break
    return text


def fmt_age(ts_ms) -> str:
    if not ts_ms:
        return ""
    delta = time.time() - (int(ts_ms) / 1000.0)
    if delta < 90:
        return "刚刚"
    if delta < 3600:
        return f"{delta / 60:.0f}m前"
    if delta < 86400:
        return f"{delta / 3600:.1f}h前"
    return f"{delta / 86400:.0f}d前"


def shorten(text: str, limit: int) -> str:
    text = " ".join((text or "").split())
    return text if len(text) <= limit else text[: limit - 1] + "…"


def mask(value: str, keep: int = 10) -> str:
    """Log an identifier without writing the whole thing to disk.

    Logs get copied into bug reports and pasted into issues; a full open_id
    or chat_id there is personal data that outlives us. The prefix is enough
    to tell two ids apart while debugging.
    """
    if not value:
        return "-"
    return value if len(value) <= keep + 3 else f"{value[:keep]}…"


def trim_tail(lines: list[str], budget: int) -> str:
    """Keep the *end* of the output, which is what a tail is for."""
    out: list[str] = []
    used = 0
    for line in reversed(lines):
        if used + len(line) + 1 > budget:
            out.append("…(更早的输出已省略)")
            break
        out.append(line)
        used += len(line) + 1
    return "\n".join(reversed(out))


STATUS_DOT = {
    "todo": "⚪",
    "in-progress": "🔵",
    "in-review": "🟣",
    "completed": "🟢",
}


@dataclass
class Session:
    """One agent conversation, as Orca models it.

    Orca already knows the thing that matters: `worktree ps` reports each
    agent under `agents[]` with a `paneKey` and a `state` of working|done.
    The paneKey is `<tabId>:<leafId>`, which joins straight onto a row from
    `terminal list` — that is how we get the handle to write to.

    Users should never see any of these identifiers. They are the router's
    private vocabulary; the user-facing name is `label`.
    """

    key: str
    handle: str
    label: str
    short: str
    state: str
    connected: bool

    @property
    def busy(self) -> bool:
        return self.state == "working"


class Bridge:
    def __init__(self, cfg: dict, cmd: list[str]):
        self.cfg = cfg
        self.cmd = cmd
        self.started_at = time.time()
        self.seen: OrderedDict[str, float] = OrderedDict()
        self.max_seen = 2000
        self.count = 0
        self.armed_until = 0.0
        self.default_term: int | None = None
        self.focus_key: str | None = None
        self.stale_note = ""
        self.load_state()

    # -- focus persistence ------------------------------------------------
    #
    # The focus is which session a bare `/send` addresses. Keeping it only in
    # memory means every restart makes the bridge forget the conversation you
    # were in the middle of. Persisting it is the difference between "it asks
    # me to disambiguate every time" and "it remembers the thread".

    def _state_path(self) -> Path:
        return HERE / str(self.cfg.get("state_file", "state.json"))

    def load_state(self) -> None:
        path = self._state_path()
        if not path.is_file():
            return
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            log.warning("state file unreadable, starting fresh: %s", path)
            return
        self.focus_key = data.get("focus_key") or None
        if self.focus_key:
            log.info("restored focus: %s", mask(str(self.focus_key)))

    def save_state(self) -> None:
        path = self._state_path()
        payload = {"focus_key": self.focus_key, "updated_at": int(time.time())}
        tmp = path.with_suffix(path.suffix + ".tmp")
        try:
            tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            tmp.replace(path)  # atomic-ish: never leave a half-written file
        except OSError as exc:
            log.warning("could not persist focus: %s", exc)

    def _already_seen(self, message_id: str) -> bool:
        if not message_id:
            return False
        if message_id in self.seen:
            return True
        self.seen[message_id] = time.time()
        while len(self.seen) > self.max_seen:
            self.seen.popitem(last=False)
        return False

    def authorized(self, sender_id: str) -> bool:
        allow = self.cfg.get("allowed_open_ids") or []
        return not allow or sender_id in allow

    # -- orca -------------------------------------------------------------

    def orca(self, args: list[str], timeout: float = 20.0) -> dict:
        """Call the Orca CLI and return result. UTF-8 forced for the same
        reason as lark-cli: Chinese-locale Windows defaults to cp936."""
        res = subprocess.run(
            ["orca", *args],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout,
        )
        if res.returncode != 0:
            raise RuntimeError((res.stderr or res.stdout or "").strip()[:400])
        payload = json.loads(res.stdout)
        if not payload.get("ok"):
            err = payload.get("error") or {}
            raise RuntimeError(f"{err.get('type') or 'error'}: {err.get('message') or payload}")
        return payload.get("result") or {}

    def cmd_wt(self, show_all: bool = False) -> str:
        """Mobile is a supervision surface, not a dashboard.

        Default to the few workspaces that moved recently — ten cards of
        wrapped text on a phone is worse than useless. `/wt all` is the
        escape hatch.
        """
        wts = self.orca(["worktree", "ps", "--json"]).get("worktrees") or []
        if not wts:
            return "没有 workspace。"
        wts.sort(key=lambda w: w.get("lastActivityAt") or 0, reverse=True)
        limit = 20 if show_all else 5
        shown = wts[:limit]
        out = [f"**{len(wts)} 个 workspace** · 最近活动在前", ""]
        for i, w in enumerate(shown, 1):
            name = w.get("displayName") or Path(w.get("path") or "?").name
            status = w.get("workspaceStatus") or "-"
            dot = STATUS_DOT.get(status, "⚫")
            age = fmt_age(w.get("lastActivityAt"))
            branch = (w.get("branch") or "").replace("refs/heads/", "")
            out.append(f"**{i}** {shorten(name, 24)}")
            out.append(f"{dot} {status}" + (f" · {age}" if age else ""))
            tail = [w.get("repo") or "?"]
            if branch:
                tail.append(branch)
            pr = w.get("linkedPR")
            if pr:
                checks = pr.get("checksSummary") or {}
                ci = checks.get("state") or ""
                if checks.get("total"):
                    ci += f" {checks.get('passed', 0)}/{checks.get('total')}"
                mark = {"success": "✓", "failure": "✗", "pending": "…"}.get(ci.split()[0], "")
                tail.append(f"PR#{pr.get('number')} CI {ci}{mark}".strip())
            out.append(" · ".join(tail))
            out.append("")
        if len(wts) > len(shown):
            out.append(f"还有 {len(wts) - len(shown)} 个 · `/wt all` 看全部")
        return "\n".join(out).rstrip()

    def _terminals(self) -> list[dict]:
        return self.orca(["terminal", "list", "--json"]).get("terminals") or []

    def sessions(self) -> list[Session]:
        """Join agent sessions onto their terminal handles.

        Busy first, then alphabetical — the list is a supervision queue, not
        an inventory.
        """
        wts = self.orca(["worktree", "ps", "--json"]).get("worktrees") or []
        by_pane = {}
        for t in self._terminals():
            by_pane.setdefault((t.get("tabId"), t.get("leafId")), t)
        out: list[Session] = []
        for w in wts:
            path = (w.get("path") or "").replace("\\", "/").rstrip("/")
            short = path.rsplit("/", 1)[-1] if path else "?"
            label = " · ".join(x for x in (w.get("repo"), w.get("displayName")) if x)
            for a in w.get("agents") or []:
                pane = a.get("paneKey") or ""
                tab, _, leaf = pane.partition(":")
                t = by_pane.get((tab, leaf))
                if not t:
                    continue
                out.append(
                    Session(
                        key=pane,
                        handle=t["handle"],
                        label=label,
                        short=short,
                        state=a.get("state") or "?",
                        connected=bool(t.get("connected")),
                    )
                )
        out.sort(key=lambda s: (not s.busy, s.label))
        # Two agent tabs in the same worktree share a basename, which makes
        # them indistinguishable both to the router and to the person reading
        # the list. Suffix the later ones.
        seen: dict[str, int] = {}
        for s in out:
            n = seen.get(s.short, 0) + 1
            seen[s.short] = n
            if n > 1:
                s.short = f"{s.short}#{n}"
        return out

    def cmd_sessions(self) -> str:
        ss = self.sessions()
        if not ss:
            return "没有 agent 会话。"
        busy = sum(1 for s in ss if s.busy)
        out = [f"**{len(ss)} 个会话** · {busy} 个在跑", ""]
        for s in ss[:8]:
            dot = "●" if s.busy else "○"
            mark = " ← 焦点" if s.key == self.focus_key else ""
            out.append(f"{dot} **{shorten(s.short, 18)}**{mark}")
            out.append(shorten(s.label, 42))
            out.append("")
        if len(ss) > 8:
            out.append(f"还有 {len(ss) - 8} 个")
        return "\n".join(out).rstrip()

    def _match_sessions(self, name: str) -> list[Session]:
        q = name.strip().lower()
        if not q:
            return []
        ss = self.sessions()
        exact = [s for s in ss if q == s.short.lower()]
        if exact:
            return exact
        return [s for s in ss if q in s.short.lower() or q in s.label.lower()]

    def _focus_session(self) -> tuple[Session | None, str | None]:
        """Pick the session a bare `/send` should go to.

        Ladder: current focus > the only busy one > the only one at all.
        Anything less certain is a question, never a guess — writing to the
        wrong agent is the one operation that cannot be undone.

        A persisted focus is re-validated against the live session list on
        every use: the session may have been closed while the bridge was
        down, and a focus pointing at nothing is worse than no focus.
        """
        ss = self.sessions()
        if not ss:
            return None, "没有 agent 会话。"
        self.stale_note = ""
        if self.focus_key:
            hit = next((s for s in ss if s.key == self.focus_key), None)
            if hit:
                return hit, None
            log.info("persisted focus is gone; clearing")
            self.focus_key = None
            self.save_state()
            self.stale_note = "上次的焦点会话已经不在了，已清除。\n\n"
        busy = [s for s in ss if s.busy]
        if len(busy) == 1:
            return busy[0], None
        if len(ss) == 1:
            return ss[0], None
        names = "、".join(f"`{s.short}`" for s in (busy or ss)[:4])
        return None, f"{self.stale_note}不知道发给谁。用 `/to <名字> <文本>` 点名，候选：{names}"

    def cmd_to(self, arg: str) -> str:
        if time.time() >= self.armed_until:
            return "🔒 写操作上锁中。先 `/arm` 解锁（默认 5 分钟）。"
        parts = arg.split(None, 1)
        if len(parts) < 2:
            return "`/to <名字> <文本>`，名字看 `/sessions`"
        name, text = parts[0], parts[1]
        hits = self._match_sessions(name)
        if not hits:
            ss = self.sessions()
            names = "、".join(f"`{s.short}`" for s in ss[:6]) or "（空）"
            return f"没找到 `{name}`。候选：{names}"
        if len(hits) > 1:
            names = "、".join(f"`{s.short}`" for s in hits[:4])
            return f"`{name}` 匹配到多个，说清楚点：{names}"
        self.focus_key = hits[0].key
        self.save_state()
        return self._write_to(hits[0], text)

    def _write_to(self, s: Session, text: str) -> str:
        if not text.strip():
            return "文本为空。"
        if not s.connected:
            return f"{s.short} 的终端未连接，拒绝发送。"
        argv = ["terminal", "send", "--terminal", s.handle, "--text", text, "--enter"]
        argv += ["--wait-submit", "8", "--json"]
        data = self.orca(argv)
        send = data.get("send") or {}
        stages = (send.get("prompt") or {}).get("stages") or []
        proof = "已提交" if "turn_started" in stages else "已送达（未确认开始执行）"
        return (
            f"→ **{shorten(s.short, 18)}**\n{proof}\n\n"
            f"> {shorten(text, 300)}\n\n"
            f"`/tail` 看它的输出"
        )

    def cmd_term(self) -> str:
        terms = self._terminals()
        if not terms:
            return "没有终端。"
        out = [f"**{len(terms)} 个终端**", ""]
        for i, t in enumerate(terms, 1):
            dot = "●" if t.get("connected") else "○"
            wpath = (t.get("worktreePath") or "").replace("\\", "/").rstrip("/")
            where = wpath.rsplit("/", 1)[-1] if wpath else "-"
            agent = t.get("agentIdentity")
            out.append(f"**{i}** {dot} {shorten(t.get('title') or 'untitled', 22)}")
            out.append(where + (f" · {agent}" if agent else ""))
            out.append("")
        out.append("`/tail <编号>` 看输出")
        return "\n".join(out).rstrip()

    def cmd_tail(self, arg: str) -> str:
        terms = self._terminals()
        if not terms:
            return "没有终端。"
        if not arg:
            return f"/tail <编号>，当前 1..{len(terms)}"
        try:
            n = int(arg)
        except ValueError:
            return "/tail <编号>，编号是 /term 前面的数字"
        if not 1 <= n <= len(terms):
            return f"编号超出范围：1..{len(terms)}"
        t = terms[n - 1]
        data = self.orca(["terminal", "read", "--terminal", t["handle"], "--json"])
        term = data.get("terminal") or {}
        tail = term.get("tail") or []
        header = f"终端 {n} · {shorten(t.get('title') or '', 28)} · {term.get('status') or '?'}"
        rule = "─" * 16
        budget = int(self.cfg.get("max_reply_chars", 4000)) - len(header) - len(rule) - 8
        body = trim_tail(tail[-60:], max(budget, 200)) if tail else "(无输出)"
        return f"{header}\n{rule}\n{body}"

    # -- write path -------------------------------------------------------

    def guarded(self, fn, *args):
        try:
            return fn(*args)
        except FileNotFoundError:
            return "找不到 orca 命令，检查 PATH。"
        except subprocess.TimeoutExpired:
            return "orca 命令超时。"
        except Exception as exc:
            log.exception("orca command failed: %s", getattr(fn, "__name__", fn))
            return f"⚠️ 失败：{shorten(str(exc), 300)}"

    def cmd_use(self, arg: str) -> str:
        if not arg.isdigit():
            return "/use <编号>，编号看 `/term`"
        n = int(arg)
        terms = self._terminals()
        if not 1 <= n <= len(terms):
            return f"编号超出范围：1..{len(terms)}"
        self.default_term = n
        t = terms[n - 1]
        return f"默认终端 → **{n}** {shorten(t.get('title') or '', 20)}\n写之前还需要 `/arm`。"

    def cmd_arm(self, arg: str) -> str:
        try:
            minutes = float(arg) if arg else 5.0
        except ValueError:
            return "/arm <分钟>，默认 5 分钟"
        minutes = max(1.0, min(minutes, 60.0))
        self.armed_until = time.time() + minutes * 60
        return f"🔓 写操作解锁 {minutes:g} 分钟（到 {time.strftime('%H:%M:%S', time.localtime(self.armed_until))}）。"

    def cmd_disarm(self) -> str:
        self.armed_until = 0.0
        self.default_term = None
        self.focus_key = None
        self.save_state()
        return "🔒 已上锁，焦点和默认终端已清除。"

    def _resolve_target(self, n: int | None):
        terms = self._terminals()
        if not terms:
            return None, None, "没有终端。"
        if n is None:
            n = self.default_term
        if n is None:
            return None, None, "没说发给谁：`/send <编号> <文本>`，或先 `/use <编号>` 设默认。"
        if not 1 <= n <= len(terms):
            return None, None, f"编号超出范围：1..{len(terms)}"
        return terms[n - 1], n, None

    def cmd_send(self, arg: str) -> str:
        """The only irreversible operation in this bridge.

        Two gates before anything reaches a PTY: an explicit `/arm` window,
        and a resolved target we echo back. A bare `/send` goes to the focus
        session, never to "whatever looked closest".
        """
        if time.time() >= self.armed_until:
            return "🔒 写操作上锁中。先 `/arm` 解锁（默认 5 分钟）。"
        if not arg.strip():
            return "`/send <文本>` 或 `/send <编号> <文本>`"

        parts = arg.split(None, 1)
        if parts[0].isdigit():
            # Debug escape hatch: address a raw terminal by index.
            term, idx, err = self._resolve_target(int(parts[0]))
            if err:
                return err
            text = parts[1] if len(parts) > 1 else ""
            if not text.strip():
                return "`/send <编号> <文本>`"
            if not term.get("connected"):
                return f"终端 {idx} 未连接，拒绝发送。"
            argv = ["terminal", "send", "--terminal", term["handle"], "--text", text, "--enter"]
            if term.get("agentIdentity"):
                argv += ["--wait-submit", "8"]
            data = self.orca(argv + ["--json"])
            stages = ((data.get("send") or {}).get("prompt") or {}).get("stages") or []
            proof = "已提交" if "turn_started" in stages else "已送达（未确认开始执行）"
            return f"→ **{idx} 号终端**\n{proof}\n\n> {shorten(text, 300)}"

        sess, err = self._focus_session()
        if err:
            return err
        if self.focus_key != sess.key:
            self.focus_key = sess.key
            self.save_state()
        return self.stale_note + self._write_to(sess, arg)

    def handle(self, ev: dict) -> str | None:
        # Our own replies can echo back in group chats; never answer a bot.
        if ev.get("sender_type") == "bot":
            return None

        message_id = ev.get("message_id") or ev.get("id") or ""
        if self._already_seen(message_id):
            log.debug("duplicate delivery ignored: %s", message_id)
            return None

        text = normalize(ev.get("content") or "")
        sender = ev.get("sender_id") or ""
        chat_id = ev.get("chat_id") or ""

        if not text:
            log.debug("empty message (%s) ignored", ev.get("message_type"))
            return None

        log.info(
            "recv %s chat=%s sender=%s text=%r",
            mask(message_id), mask(chat_id), mask(sender), text[:120],
        )

        # /id is intentionally available to everyone so the operator can learn
        # the open_id the bridge actually sees (open_id is app-scoped; the one
        # from `lark-cli auth status` is the API's view, worth confirming).
        if text.split()[0] in ("/id", "/ID"):
            return f"open_id: {sender}\nchat_id: {chat_id}\nchat_type: {ev.get('chat_type')}"

        if not self.authorized(sender):
            log.warning("rejected unauthorized sender %s", sender)
            return "⛔ 你的 open_id 不在白名单里。发 /id 查看，把它加进 config.json 的 allowed_open_ids。"

        parts = text.split(None, 1)
        head = parts[0].lower()
        arg = parts[1].strip() if len(parts) > 1 else ""

        if head == "/ping":
            return f"pong · uptime {time.time() - self.started_at:.0f}s · handled {self.count}"
        if head in ("/help", "/?"):
            return Reply(HELP, markdown=True)

        read_only = {
            "/wt": (self.cmd_wt, (arg.lower() == "all",)),
            "/workspace": (self.cmd_wt, (arg.lower() == "all",)),
            "/term": (self.cmd_term, ()),
            "/terminals": (self.cmd_term, ()),
            "/tail": (self.cmd_tail, (arg,)),
        }
        if head in read_only:
            fn, fn_args = read_only[head]
            return Reply(self.guarded(fn, *fn_args), markdown=True)

        if head == "/use":
            return Reply(self.guarded(self.cmd_use, arg), markdown=True)
        if head == "/sessions":
            return Reply(self.guarded(self.cmd_sessions), markdown=True)
        if head == "/to":
            return Reply(self.guarded(self.cmd_to, arg), markdown=True)
        if head == "/arm":
            return Reply(self.cmd_arm(arg), markdown=True)
        if head == "/disarm":
            return Reply(self.cmd_disarm(), markdown=True)
        if head == "/send":
            return Reply(self.guarded(self.cmd_send, arg), markdown=True)

        self.count += 1
        return (
            f"收到 #{self.count}：{text}\n\n"
            "（还没接转发。先用 `/arm` + `/send <文本>`，或 `/help`。）"
        )

    def reply(self, message_id: str, text: str | Reply) -> None:
        if not message_id or text is None:
            return
        markdown = text.markdown if isinstance(text, Reply) else False
        body = text.text if isinstance(text, Reply) else text
        send_reply(
            self.cmd,
            message_id,
            body,
            self.cfg.get("max_reply_chars", 4000),
            markdown=markdown,
        )


# --------------------------------------------------------------------------
# setup / main
# --------------------------------------------------------------------------

def setup_logging(cfg: dict, verbose: bool) -> None:
    level = getattr(logging, str(cfg.get("log_level", "INFO")).upper(), logging.INFO)
    if verbose:
        level = logging.DEBUG
    fmt = logging.Formatter("%(asctime)s %(levelname)-7s %(name)s: %(message)s", "%H:%M:%S")

    root = logging.getLogger()
    root.setLevel(level)
    root.handlers.clear()

    console = logging.StreamHandler(sys.stderr)
    console.setFormatter(fmt)
    root.addHandler(console)

    log_path = HERE / str(cfg.get("log_file", "logs/bridge.log"))
    log_path.parent.mkdir(parents=True, exist_ok=True)
    from logging.handlers import RotatingFileHandler

    file_h = RotatingFileHandler(log_path, maxBytes=5_000_000, backupCount=3, encoding="utf-8")
    file_h.setFormatter(fmt)
    root.addHandler(file_h)


def load_config(path: Path) -> dict:
    cfg = {
        "allowed_open_ids": [],
        "max_reply_chars": 4000,
        "event_key": DEFAULT_EVENT_KEY,
        "log_file": "logs/bridge.log",
        "log_level": "INFO",
    }
    if path.is_file():
        with path.open(encoding="utf-8") as fh:
            cfg.update(json.load(fh))
    else:
        log.warning("config not found at %s; using defaults", path)
    return cfg


def selftest(cmd: list[str], cfg: dict) -> int:
    print(f"lark-cli command: {' '.join(cmd)}")
    ver = run_lark(cmd, ["--version"])
    print(f"  version: {(ver.stdout or ver.stderr).strip()}")

    print("opening event bus for 12s (expect '[source] feishu-websocket: connected')...")
    # Bound the child itself: `events()` does a blocking read, so a deadline
    # checked between events would never fire when nothing arrives.
    consumer = EventConsumer(
        cmd,
        cfg.get("event_key", DEFAULT_EVENT_KEY),
        extra_args=["--max-events", "1", "--timeout", "12s"],
    )
    try:
        consumer.start(timeout=30)
        got = 0
        for ev in consumer.events():
            got += 1
            print(f"  event: chat={ev.get('chat_id')} sender={ev.get('sender_id')} "
                  f"type={ev.get('message_type')} text={(ev.get('content') or '')[:60]!r}")
        if got == 0:
            print("  no message arrived during the window (send the bot a DM to test end-to-end)")
    finally:
        consumer.stop()

    print("checking reply path with --dry-run...")
    res = run_lark(cmd, [
        "im", "+messages-reply", "--as", "bot",
        "--message-id", "om_selftest_placeholder",
        "--text", "selftest", "--dry-run", "--json",
    ])
    ok = res.returncode == 0
    print(f"  reply build: {'ok' if ok else 'FAILED'}")
    if not ok:
        print((res.stderr or res.stdout or "").strip()[:800])
    return 0 if ok else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Feishu <-> Orca bridge")
    parser.add_argument("--config", default=str(HERE / "config.json"))
    parser.add_argument("--lark-cli", default=None, help="explicit lark-cli executable")
    parser.add_argument("--selftest", action="store_true", help="verify wiring, do not run the loop")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)

    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")

    cfg = load_config(Path(args.config))
    setup_logging(cfg, args.verbose)
    cmd = find_lark_cmd(args.lark_cli)

    if args.selftest:
        return selftest(cmd, cfg)

    stop = threading.Event()

    def _on_signal(signum, _frame):
        log.info("signal %s received, shutting down", signum)
        stop.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, _on_signal)
        except (ValueError, OSError):
            pass

    consumer = EventConsumer(cmd, cfg.get("event_key", DEFAULT_EVENT_KEY))
    consumer.start()
    bridge = Bridge(cfg, cmd)
    log.info("bridge ready; waiting for messages")

    try:
        for ev in consumer.events():
            if stop.is_set():
                break
            try:
                text = bridge.handle(ev)
            except Exception:
                log.exception("handler crashed on event")
                text = "⚠️ 桥接内部错误，详情见 logs/bridge.log"
            if text:
                bridge.reply(ev.get("message_id") or ev.get("id") or "", text)
            if consumer.proc and consumer.proc.poll() is not None:
                break
    finally:
        consumer.stop()

    log.info("bridge stopped")
    return 0


if __name__ == "__main__":
    sys.exit(main())
