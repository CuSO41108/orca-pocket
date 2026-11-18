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
import hashlib
import json
import logging
import os
import queue
import re
import secrets
import shutil
import signal
import subprocess
import sys
import threading
import time
from collections import OrderedDict, deque
from dataclasses import dataclass
from pathlib import Path

import router
from llm import LLM, LLMError

import cards as cards_mod

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

    markdown=True goes out as a `post` message with an `md` block. A `card`
    wins over both: it is an interactive card, which is the only form the
    phone can act on without typing.
    """

    text: str
    markdown: bool = False
    card: dict | None = None


def _reply_card(card: dict, fallback: str = "") -> Reply:
    return Reply(fallback, card=card)


def as_reply(out) -> Reply:
    return out if isinstance(out, Reply) else Reply(out, markdown=True)


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


def send_card(
    cmd: list[str],
    card: dict,
    *,
    message_id: str | None = None,
    chat_id: str | None = None,
    idempotency: str | None = None,
) -> bool:
    """Send an interactive card, as a reply or as a new message."""
    if message_id:
        args = ["im", "+messages-reply", "--as", "bot", "--message-id", message_id]
    elif chat_id:
        args = ["im", "+messages-send", "--as", "bot", "--chat-id", chat_id]
    else:
        return False

    args += ["--msg-type", "interactive", "--content", json.dumps(card, ensure_ascii=False), "--json"]
    if idempotency:
        args += ["--idempotency-key", idempotency]

    try:
        res = run_lark(cmd, args)
    except Exception as exc:  # timeout / OSError / anything the CLI shells into
        log.error("card send raised: %s", exc)
        return False
    if res.returncode != 0:
        log.error("card send failed rc=%s %s", res.returncode, (res.stderr or "").strip()[:400])
        return False
    log.info("sent card (%d bytes)", len(json.dumps(card)))
    return True


# --------------------------------------------------------------------------
# routing
# --------------------------------------------------------------------------

HELP = """\
**Feishu ⇄ Orca** · 直接说话就行

**这样问**
「现在什么情况」
「myapp 在干嘛」
「让 my-video 继续」
「接下来 30 分钟我说什么都照做」

**命令（逃生阀，日常不用记）**
`/status` `/sessions` `/tail` `/wt` `/term`
`/arm` `/disarm` `/send` `/to` `/use`
`/id` `/help`"""


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


def event_key(raw: str) -> str:
    """A stable, bounded idempotency key.

    Feishu rejects keys longer than 50 characters, and a raw
    `done:<paneKey>:<ts>` is ~89 with real UUID pane keys — which silently
    turned every completion notification into a validation failure.
    """
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:40]


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


# Agent TUIs repaint: spinners, token counters and box drawing arrive as
# stacked fragments rather than a picture of the screen. Dropping those leaves
# something a person can actually read on a phone.
_TUI_NOISE = re.compile(
    r"(tokens?\s*·|thought for|\(ctrl\+[a-z] to expand\)|^\s*[●○✢✻✽·⎿─┌└├│╭╰]|^\s*\.\.\.\s*\+\d+\s+lines)"
)


def clean_preview(text: str, limit: int = 12) -> str:
    lines = []
    for raw in (text or "").splitlines():
        line = raw.rstrip()
        if not line.strip():
            continue
        if _TUI_NOISE.search(line):
            continue
        lines.append(line)
    return "\n".join(lines[-limit:])


def repo_of(label: str) -> str:
    return (label or "").split(" · ")[0]


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
    last_output_at: int | None = None
    preview: str = ""
    path: str = ""
    branch: str = ""
    agent: str = ""
    incarnation: str = ""

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
        self.chat_id: str | None = None
        self.chat_owner: str | None = None
        self.watch: dict = {}
        self._state_lock = threading.RLock()
        self.stale_note = ""
        self._session_cache: list[Session] | None = None
        self.pending: dict[str, dict] = {}
        self.recent_resolved: dict[str, dict] = {}
        self.undelivered: list[dict] = []
        self.cards_available = True
        self._watch_baselined = False
        self._dirty_cache: dict[str, tuple[float, list[str] | None]] = {}
        self.pending_ttl = float(cfg.get("pending_ttl_seconds", 300))
        self.llm = LLM(cfg.get("llm") or {})
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
        self.chat_id = data.get("chat_id") or None
        self.chat_owner = data.get("chat_owner") or None
        self.watch = data.get("watch") or {}
        if self.focus_key:
            log.info("restored focus: %s", mask(str(self.focus_key)))

    def save_state(self) -> None:
        # The watchdog thread and the message thread both persist state.
        # Without the lock they interleave on the same temp path: one can
        # overwrite the other's newer snapshot.
        with self._state_lock:
            path = self._state_path()
            payload = {
                "focus_key": self.focus_key,
                "chat_id": self.chat_id,
                "chat_owner": self.chat_owner,
                "watch": self.watch,
                "updated_at": int(time.time()),
            }
            tmp = path.with_suffix(path.suffix + ".tmp")
            try:
                tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
                tmp.replace(path)  # atomic-ish: never leave a half-written file
            except OSError as exc:
                log.warning("could not persist state: %s", exc)

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

    def sessions(self, fresh: bool = False) -> list[Session]:
        """Join agent sessions onto their terminal handles.

        Busy first, then alphabetical — the list is a supervision queue, not
        an inventory.
        """
        if not fresh and self._session_cache is not None:
            return self._session_cache
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
                        last_output_at=t.get("lastOutputAt"),
                        preview=t.get("preview") or "",
                        path=(w.get("path") or "").replace("\\", "/"),
                        branch=(w.get("branch") or "").replace("refs/heads/", ""),
                        agent=t.get("agentIdentity") or "",
                        incarnation=t.get("incarnationId") or "",
                    )
                )
        # Aliases must be stable: assigning `#n` after the busy-first sort made
        # a pane's name change when a sibling finished, so a follow-up message
        # quoting that name could address a different agent. Number by pane key
        # first, then sort for display.
        out.sort(key=lambda s: (s.short, s.key))
        seen: dict[str, int] = {}
        for s in out:
            n = seen.get(s.short, 0) + 1
            seen[s.short] = n
            if n > 1:
                s.short = f"{s.short}#{n}"
        out.sort(key=lambda s: (not s.busy, s.label))
        # A fresh read belongs to the caller alone. Writing it into the shared
        # cache let a watchdog tick replace the list that an in-flight message
        # was still routing against, which could retarget a write to a
        # neighbouring terminal (name vs name#2 swap).
        if not fresh:
            self._session_cache = out
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

    def _send_text(self, handle: str, text: str) -> str:
        data = self.orca(
            ["terminal", "send", "--terminal", handle, "--text", text,
             "--enter", "--wait-submit", "8", "--json"]
        )
        stages = ((data.get("send") or {}).get("prompt") or {}).get("stages") or []
        return "已提交" if "turn_started" in stages else "已送达（未确认开始执行）"

    def _write_to(self, s: Session, text: str) -> str:
        if not text.strip():
            return "文本为空。"
        if not s.connected:
            return f"{s.short} 的终端未连接，拒绝发送。"
        proof = self._send_text(s.handle, text)
        return (
            f"→ **{shorten(s.short, 18)}**\n{proof}\n\n"
            f"> {shorten(text, 300)}\n\n"
            f"`/tail` 看它的输出"
        )

    # -- confirmation ------------------------------------------------------
    #
    # Writing into a live agent cannot be undone, so the default path asks
    # first: a card with what will be sent and to whom, and two buttons.
    # `/arm` stays as the "I'm busy, stop asking" escape hatch.

    def _live_target(self, sess: Session) -> Session | None:
        """Re-read the session and require it to still be the same thing.

        Routing can take seconds (the model call); the pane may have been
        closed or handed to a different agent in that time.
        """
        live = next((s for s in self.sessions(fresh=True) if s.key == sess.key), None)
        if (live is None or not live.connected or live.handle != sess.handle
                or live.agent != sess.agent or live.incarnation != sess.incarnation):
            return None
        return live

    def _request_write(self, sess: Session, text: str) -> Reply:
        if time.time() < self.armed_until or not self.cfg.get("confirm_writes", True):
            live = self._live_target(sess)
            if live is None:
                return Reply("⚠️ 目标会话在路由期间变了，没有发送。请再发一次。", markdown=True)
            return Reply(self._write_to(live, text), markdown=True)

        if len(text) > 2000:
            # What the operator approves must be exactly what executes, so an
            # instruction too long to show in full cannot go through a card.
            # This limit belongs here, not above the armed bypass.
            return Reply(
                "指令太长（超过 2000 字），没法在卡片里完整展示，拒绝执行。\n"
                "请缩短，或用 `/arm` 解锁后直接发。",
                markdown=True,
            )

        if not self.cards_available:
            # Without a callback listener the buttons would never fire, so
            # offering them would be a dead end.
            return Reply(
                "🔒 写操作需要确认，但卡片回调没有启用。先 `/arm` 解锁，"
                "或检查 `enable_cards` 与飞书后台的回调配置。",
                markdown=True,
            )

        # 64 bits, and expired entries are pruned first. A short id with a
        # leaky pending map would eventually collide and make an old card
        # execute a different action than the one it displayed.
        now = time.time()
        for stale in [k for k, v in self.pending.items() if now - v["created"] > self.pending_ttl]:
            self.pending.pop(stale, None)

        pid = secrets.token_hex(8)
        while pid in self.pending:
            pid = secrets.token_hex(8)
        self.pending[pid] = {
            "key": sess.key,
            "agent": sess.agent,
            "incarnation": sess.incarnation,
            "short": sess.short,
            "handle": sess.handle,
            "text": text,
            "created": now,
        }
        body = f"发给 **{sess.short}**"
        return _reply_card(
            # The full, untruncated instruction: approving a shortened
            # rendering would mean approving something other than what runs.
            cards_mod.confirm(pid, "等你确认", body, subtitle=sess.label, detail=text),
            fallback="⚠️ 确认卡片没发出去，操作没有执行。用 `/arm` 解锁后直接发，或稍后再试一次。",
        )

    def handle_card(self, ev: dict) -> None:
        operator = ev.get("operator_id") or ""
        if not self.authorized(operator):
            log.warning("card action from unauthorized operator %s", mask(operator))
            return

        raw = ev.get("action_value")
        try:
            data = json.loads(raw) if isinstance(raw, str) else (raw or {})
        except json.JSONDecodeError:
            log.warning("card action with unparseable value: %r", str(raw)[:120])
            return
        if data.get("op") != "confirm":
            return

        token = ev.get("token")
        pid = str(data.get("pid") or "")
        choice = data.get("choice")
        log.info("card action pid=%s choice=%s operator=%s", pid, choice, mask(operator))

        pending = self.pending.pop(pid, None)
        if not pending:
            # A double click must not overwrite the result the first click
            # already produced.
            previous = self.recent_resolved.get(pid)
            self._update_card(
                token,
                previous or cards_mod.resolved("已失效", "这个确认已经处理过了。", template="grey"),
            )
            return
        if time.time() - pending["created"] > self.pending_ttl:
            self._update_card(token, cards_mod.resolved("已过期", "确认超时，没有执行。", template="grey"), pid)
            return

        target = pending["short"]
        if choice != "yes":
            self._update_card(token, cards_mod.resolved("已取消", f"没有发给 **{target}**。", template="grey"), pid)
            return

        # The confirmation window is minutes long. The pane may have closed or
        # been repurposed since, and the stored handle could now address a
        # different process. Require the same session, still connected, still
        # owning that handle.
        try:
            live = next((s for s in self.sessions(fresh=True) if s.key == pending.get("key")), None)
        except Exception:
            # A transient orca failure must not eat the approval: put it back
            # so the same button works on the next click.
            self.pending[pid] = pending
            log.exception("could not verify the target; confirmation kept for retry")
            return
        if (live is None or not live.connected or live.handle != pending["handle"]
                or live.agent != pending.get("agent", live.agent)
                or live.incarnation != pending.get("incarnation", live.incarnation)):
            # Three cheap identities have to agree: the handle, the agent type,
            # and the PTY incarnation. Exiting one agent and starting another
            # in the same pane keeps the handle; restarting the pane changes
            # the incarnation. What we still cannot see is a *conversation*
            # id inside an unchanged pane — Orca does not expose one — so the
            # remaining window is one agent type, restarted, inside the TTL.
            self._update_card(
                token,
                cards_mod.resolved("目标已变化", f"**{target}** 已经不是原来那个会话了，没有发送。", template="grey"),
                pid,
            )
            return

        try:
            proof = self._send_text(live.handle, pending["text"])
        except Exception as exc:
            log.exception("confirmed write failed")
            self._update_card(
                token,
                cards_mod.resolved("执行失败", shorten(str(exc), 300), template="red"),
                pid,
            )
            return

        self._update_card(
            token,
            cards_mod.resolved(
                "已发送",
                f"→ **{target}**\n{proof}",
                template="green",
                detail=pending["text"],
            ),
            pid,
        )

    def _update_card(self, token: str | None, card: dict, pid: str | None = None) -> None:
        if pid:
            self.recent_resolved[pid] = card
            while len(self.recent_resolved) > 50:
                self.recent_resolved.pop(next(iter(self.recent_resolved)))
        if not token:
            return
        payload = json.dumps({"token": token, "card": card}, ensure_ascii=False)
        res = run_lark(
            self.cmd,
            ["api", "POST", "/open-apis/interactive/v1/card/update", "--as", "bot", "--data", payload],
        )
        if res.returncode != 0:
            log.warning("card update failed: %s", (res.stderr or "").strip()[:300])

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

    def cmd_send(self, arg: str) -> str | Reply:
        """The only irreversible operation in this bridge.

        The default path asks for confirmation with a card; `/arm` skips the
        asking for a window. A bare `/send` goes to the focus session, never
        to "whatever looked closest".
        """
        if not arg.strip():
            return "`/send <文本>` 或 `/send <编号> <文本>`"

        parts = arg.split(None, 1)
        if parts[0].isdigit():
            # Debug escape hatch: address a raw terminal by index. Kept behind
            # /arm because it bypasses the confirmation card entirely.
            if time.time() >= self.armed_until:
                return "🔒 按编号发送是调试通道，需要先 `/arm`。"
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
        out = self._request_write(sess, arg)
        if isinstance(out, Reply) and self.stale_note:
            out.text = self.stale_note + out.text
        return out

    # -- the supervision surface ------------------------------------------
    #
    # Everything below is what a person actually sees. It is deliberately
    # rule-driven: status is assembled from structured fields, never parsed
    # out of terminal text. The model is only consulted for "what did they
    # mean", never for "what is true".

    def _alerts(self) -> list[str]:
        wts = self.orca(["worktree", "ps", "--json"]).get("worktrees") or []
        out = []
        for w in wts:
            pr = w.get("linkedPR") or {}
            checks = pr.get("checksSummary") or {}
            if checks.get("state") == "failure":
                ratio = ""
                if checks.get("total"):
                    ratio = f"（{checks.get('passed', 0)}/{checks.get('total')}）"
                out.append(f"⚠ {w.get('repo')} PR#{pr.get('number')} CI 失败{ratio}")
        return out

    def cmd_status(self) -> str:
        ss = self.sessions()
        alerts = self._alerts()
        if not ss:
            # A worktree can keep a linked PR with failing CI after all its
            # agent terminals close; that is exactly when the alert matters.
            return "\n".join(["没有 agent 会话。"] + ([""] + alerts if alerts else []))
        busy = [s for s in ss if s.busy]
        idle = [s for s in ss if not s.busy]
        out = [f"**{len(busy)} 个在跑** · {len(idle)} 个空闲", ""]
        for s in busy:
            out.append(f"● **{s.short}** · {repo_of(s.label)} · {fmt_age(s.last_output_at)}")
        for s in idle[:5]:
            badge = self._dirty_badge(s.path)
            line = f"○ {s.short} · {fmt_age(s.last_output_at) or '—'}"
            out.append(f"{line} · **{badge}**" if badge else line)
        if len(idle) > 5:
            out.append(f"…还有 {len(idle) - 5} 个空闲")
        if alerts:
            out.append("")
            out.extend(alerts)
        return "\n".join(out).rstrip()

    def _changed_files(self, path: str, ttl: float = 10.0) -> list[str] | None:
        """Uncommitted files, from git — a fact, not a screen scrape.

        Cached briefly: a status card touches every visible session and each
        check spawns a git process, which is slow enough on Windows to be
        noticeable when asking twice in a row.
        """
        if not path:
            return None
        hit = self._dirty_cache.get(path)
        if hit and time.time() - hit[0] < ttl:
            return hit[1]
        result: list[str] | None
        try:
            res = subprocess.run(
                ["git", "-c", "core.quotepath=false", "-C", path,
                 "status", "--porcelain", "--untracked-files=all"],
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=10,
            )
            result = (
                [line[3:].strip() for line in res.stdout.splitlines() if len(line) > 3]
                if res.returncode == 0
                else None
            )
        except (OSError, subprocess.TimeoutExpired):
            result = None
        self._dirty_cache[path] = (time.time(), result)
        return result

    def _dirty_badge(self, path: str) -> str:
        files = self._changed_files(path)
        return f"{len(files)} 未提交" if files else ""

    def cmd_ask(self, name: str | None) -> str:
        """Report facts about a session.

        Deliberately does NOT quote the terminal. Doing so turned every
        "what is it doing" into a mirror: asking about the session that is
        currently answering you returns your own last message verbatim, and
        asking about any other session returns repainted TUI fragments.
        Facts here are structured, cheap, and cannot be misread.
        """
        ss = self.sessions()
        if not ss:
            return "没有 agent 会话。"
        if name:
            hits = self._match_sessions(name)
            if not hits:
                return f"没找到 `{name}`。"
            if len(hits) > 1:
                return "匹配到多个：" + "、".join(f"`{s.short}`" for s in hits)
            s = hits[0]
        else:
            s, err = self._focus_session()
            if err:
                return err

        lines = [f"**{s.short}** · {'在跑' if s.busy else '空闲'} · {repo_of(s.label)}"]
        lines.append(f"最近活动：{fmt_age(s.last_output_at) or '—'}")
        if s.branch:
            lines.append(f"分支：`{s.branch}`")
        files = self._changed_files(s.path)
        if files is None:
            lines.append("未提交改动：读不到")
        elif not files:
            lines.append("未提交改动：无")
        else:
            shown = "、".join(f"`{f}`" for f in files[:3])
            more = f" 等 {len(files)} 个" if len(files) > 3 else ""
            lines.append(f"未提交改动：{len(files)} 个文件（{shown}{more}）")
        lines.append("\n_这是事实，不是摘要。想看它在想什么需要开 AI 摘要。_")
        return "\n".join(lines)

    def cmd_say(self, session_name: str | None, text: str) -> str | Reply:
        """Natural-language send: same gates as /send, no slash required."""
        if not text.strip():
            return "没听清要发什么。"
        if session_name:
            hits = self._match_sessions(session_name)
            if not hits:
                return f"没找到 `{session_name}`。"
            if len(hits) > 1:
                return "匹配到多个：" + "、".join(f"`{s.short}`" for s in hits)
            sess = hits[0]
        else:
            sess, err = self._focus_session()
            if err:
                return err
        self.focus_key = sess.key
        self.save_state()
        out = self._request_write(sess, text)
        if isinstance(out, Reply) and self.stale_note:
            out.text = self.stale_note + out.text
        return out

    # -- proactive notifications -------------------------------------------
    #
    # Only transitions are worth interrupting someone for, and only ones we
    # can read off structured fields. "Stuck" is deliberately not detected:
    # a long-running render and a hung agent look identical from the outside,
    # and a notifier that cries wolf gets muted.

    def _ci_states(self) -> dict[str, str]:
        wts = self.orca(["worktree", "ps", "--json"]).get("worktrees") or []
        out: dict[str, str] = {}
        for w in wts:
            pr = w.get("linkedPR") or {}
            number = pr.get("number")
            if not number:
                continue
            state = (pr.get("checksSummary") or {}).get("state")
            if state:
                out[f"{w.get('repo')}#{number}"] = state
        return out

    def poll_once(self) -> list[dict]:
        """One watchdog tick. Returns what should be pushed."""
        now = time.time()
        sessions = self.sessions(fresh=True)

        # Snapshot CI before mutating anything. If this call fails after the
        # session loop already advanced `self.watch`, the completion event it
        # collected is thrown away and the next tick sees no transition —
        # the notification is lost forever.
        ci = self._ci_states()

        # First tick after start establishes a baseline and emits nothing.
        # Persisted state describes the *previous* process: if an agent
        # finished while we were down, replaying it as a fresh completion
        # would also report the downtime as its duration.
        with self._state_lock:
            return self._poll_locked(sessions, ci, now)

    def _poll_locked(self, sessions: list[Session], ci: dict[str, str], now: float) -> list[dict]:
        events: list[dict] = []
        changed = False

        if not self._watch_baselined:
            # Replace, don't merge. A merged baseline keeps entries for panes
            # that are gone right now; if one reappears as done, the old
            # process's completion gets replayed with a bogus duration.
            self.watch = {
                s.key: {"state": s.state, "since": now, "short": s.short} for s in sessions
            }
            self.watch["_ci"] = dict(ci)
            self._watch_baselined = True
            self.save_state()
            log.info("watch baseline established over %d session(s)", len(sessions))
            return []

        for s in sessions:
            prev = self.watch.get(s.key)
            if prev is None:
                # First sighting adopts the current state silently, so a
                # restart never replays last night's completions.
                self.watch[s.key] = {"state": s.state, "since": now, "short": s.short}
                changed = True
                continue
            if prev.get("state") == "working" and s.state != "working":
                minutes = max(1, int((now - (prev.get("since") or now)) / 60))
                events.append({
                    "kind": "done",
                    # Stable across retries so a timed-out send that actually
                    # landed is not delivered twice.
                    "id": event_key(f"done:{s.key}:{int(prev.get('since') or now)}"),
                    "short": s.short,
                    "label": s.label,
                    "minutes": minutes,
                })
            if prev.get("state") != s.state:
                prev["since"] = now
                changed = True
            prev["state"] = s.state
            prev["short"] = s.short

        seen_ci = self.watch.setdefault("_ci", {})
        for pr_key, state in ci.items():
            prev_state = seen_ci.get(pr_key)
            if prev_state is not None and prev_state != "failure" and state == "failure":
                events.append({"kind": "ci", "id": event_key(f"ci:{pr_key}:{int(now)}"), "pr": pr_key})
            if prev_state != state:
                seen_ci[pr_key] = state
                changed = True

        if changed:
            self.save_state()
        return events

    def notify(self, events: list[dict]) -> None:
        # Anything that failed last time is retried here: poll_once() has
        # already advanced the baseline, so a dropped event would never be
        # regenerated.
        queue = self.undelivered + events
        self.undelivered = []
        if not queue:
            return
        if not self.chat_id:
            log.info("no chat to notify yet; %d event(s) held", len(queue))
            self.undelivered = queue[-20:]
            return
        # Re-check the destination on every push: the whitelist can change, and
        # a restored chat_id must not outlive the authorization that created it.
        if not self.authorized(self.chat_owner or ""):
            log.warning("notification destination is no longer authorized; dropped")
            return
        for e in queue:
            if e["kind"] == "done":
                card = cards_mod.notice(
                    f"{e['short']} 跑完了",
                    f"**{e['short']}** · {repo_of(e['label'])}\n用时约 {e['minutes']} 分钟\n\n"
                    f"`/tail` 看结果，`/to {e['short']} 继续` 接着干",
                    template="green",
                )
            else:
                card = cards_mod.notice(
                    "CI 挂了",
                    f"**{e['pr']}** 的检查失败了\n\n`/wt` 看详情",
                    template="red",
                )
            if not send_card(self.cmd, card, chat_id=self.chat_id, idempotency=e.get("id")):
                self.undelivered.append(e)
        if len(self.undelivered) > 20:
            log.warning("dropping %d oldest undelivered notification(s)", len(self.undelivered) - 20)
            self.undelivered = self.undelivered[-20:]

    def status_reply(self) -> Reply:
        body = self.guarded(self.cmd_status)
        alerts = body.count("⚠")
        card = cards_mod.notice(
            "现在的情况",
            body,
            subtitle="有事情需要你看" if alerts else None,
            template="orange" if alerts else "blue",
        )
        return _reply_card(card, fallback=body)

    def nl_reply(self, text: str):
        """Route a plain sentence. Returns None when it is not understandable."""
        parsed = None
        sessions: list[Session] = []
        if self.llm.available():
            try:
                # Reading orca can fail independently of the model. Losing the
                # session list must not make "上锁" impossible.
                sessions = self.sessions()
            except Exception:
                log.exception("could not read sessions before routing")
            try:
                parsed = router.classify(self.llm, text, sessions)
            except LLMError as exc:
                log.warning("router unavailable: %s", exc)
        if parsed is None:
            parsed = router.fallback(text)
            if parsed is None:
                return None

        intent = parsed["intent"]
        if parsed.get("invalid_target"):
            return Reply(
                f"没找到会话 `{parsed['invalid_target']}`。发 `/sessions` 看看有哪些。",
                markdown=True,
            )
        if intent == "status":
            return self.status_reply()
        if intent == "ask":
            return Reply(self.guarded(self.cmd_ask, parsed["session"]), markdown=True)
        if intent == "send":
            return as_reply(self.guarded(self.cmd_say, parsed["session"], parsed["text"]))
        if intent == "arm":
            return Reply(self.cmd_arm(str(parsed["minutes"])), markdown=True)
        if intent == "disarm":
            return Reply(self.cmd_disarm(), markdown=True)
        if intent == "help":
            return Reply(HELP, markdown=True)
        return None

    def handle(self, ev: dict) -> str | None:
        # Our own replies can echo back in group chats; never answer a bot.
        if ev.get("sender_type") == "bot":
            return None

        message_id = ev.get("message_id") or ev.get("id") or ""
        if self._already_seen(message_id):
            log.debug("duplicate delivery ignored: %s", message_id)
            return None

        # One message may read the session list several times (route it, then
        # act on it). Cache for the duration of the message only.
        self._session_cache = None

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

        # Only now may this chat receive proactive notifications. Setting it
        # before the check let a rejected sender become the destination and
        # made every later push a disclosure to them.
        # Only private chats may become the notification destination. A group
        # would send workspace events to every member, most of whom are not in
        # allowed_open_ids.
        if (chat_id and ev.get("chat_type") == "p2p"
                and (chat_id != self.chat_id or sender != self.chat_owner)):
            # Refresh the owner whenever an authorized sender writes here: a
            # chat can outlive the account that first authorized it, and
            # keeping the old owner would silence notifications forever.
            self.chat_id = chat_id
            self.chat_owner = sender
            self.save_state()

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
        if head == "/status":
            return self.status_reply()
        if head == "/to":
            return Reply(self.guarded(self.cmd_to, arg), markdown=True)
        if head == "/arm":
            return Reply(self.cmd_arm(arg), markdown=True)
        if head == "/disarm":
            return Reply(self.cmd_disarm(), markdown=True)
        if head == "/send":
            return as_reply(self.guarded(self.cmd_send, arg))

        nl = self.nl_reply(text)
        if nl is not None:
            return nl

        self.count += 1
        return (
            "没太听懂。可以试试：\n"
            "「现在什么情况」\n"
            "「myapp 在干嘛」\n"
            "「让 my-video 继续」\n\n"
            "`/help` 看全部命令。"
        )

    def reply(self, message_id: str, text: str | Reply) -> None:
        if not message_id or text is None:
            return
        markdown = False
        body = text
        if isinstance(text, Reply):
            markdown = text.markdown
            body = text.text
            if text.card and send_card(self.cmd, text.card, message_id=message_id, idempotency=message_id):
                return
            if text.card:
                log.warning("card failed; falling back to plain text")
        if not (body or "").strip():
            return
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

    msg_consumer = EventConsumer(cmd, cfg.get("event_key", DEFAULT_EVENT_KEY))
    msg_consumer.start()

    # The card callback runs as its own `event consume` process: one EventKey
    # per process is the supported shape. If the app has not enabled the
    # callback in the console the consumer still starts but stays silent, so a
    # failure here must not take the message loop down with it.
    card_consumer: EventConsumer | None = None
    card_key = cfg.get("card_event_key", "card.action.trigger")
    if cfg.get("enable_cards", True):
        card_consumer = EventConsumer(cmd, card_key)
        try:
            card_consumer.start(timeout=45)
        except Exception as exc:
            log.warning("card callbacks unavailable (%s); cards will send but not respond", exc)
            try:
                card_consumer.stop()  # a timed-out start leaves the child running
            except Exception:
                log.exception("could not stop the failed card consumer")
            card_consumer = None

    bridge = Bridge(cfg, cmd)
    bridge.cards_available = card_consumer is not None
    log.info("bridge ready; waiting for messages")

    if cfg.get("notify_enabled", True):
        interval = float(cfg.get("notify_interval_seconds", 30))

        def watch_loop() -> None:
            while not stop.wait(interval):
                events: list[dict] = []
                try:
                    events = bridge.poll_once()
                except Exception:
                    log.exception("watch poll failed")
                # Retry queued notifications even when polling is broken: an
                # Orca outage must not also silence the Feishu side.
                try:
                    bridge.notify(events)
                except Exception:
                    log.exception("watch notify failed")

        threading.Thread(target=watch_loop, name="watch", daemon=True).start()
        log.info("watching for completions every %.0fs", interval)

    inbox: queue.Queue = queue.Queue()

    def pump(c: EventConsumer, label: str) -> None:
        try:
            for ev in c.events():
                inbox.put((label, ev))
                if stop.is_set():
                    break
        except Exception:
            log.exception("consumer %s stopped unexpectedly", label)
        if not stop.is_set():
            # The consumer died on its own. Tell the main loop, or it sits on
            # an empty queue forever: no messages, no exit, nothing for a
            # supervisor to restart.
            inbox.put(("__eof__", label))

    consumers = [("message", msg_consumer)]
    if card_consumer:
        consumers.append(("card", card_consumer))
    for label, c in consumers:
        threading.Thread(target=pump, args=(c, label), name=f"pump-{label}", daemon=True).start()

    try:
        while not stop.is_set():
            try:
                label, ev = inbox.get(timeout=1.0)
            except queue.Empty:
                continue
            if label == "__eof__":
                log.error("%s consumer ended; stopping so a supervisor can restart the bridge", ev)
                stop.set()
                break
            try:
                if label == "card":
                    bridge.handle_card(ev)
                    continue
                reply = bridge.handle(ev)
                if reply:
                    bridge.reply(ev.get("message_id") or ev.get("id") or "", reply)
            except Exception:
                log.exception("handler crashed on %s event", label)
                if label == "message":
                    bridge.reply(
                        ev.get("message_id") or ev.get("id") or "",
                        "⚠️ 桥接内部错误，详情见 logs/bridge.log",
                    )
    finally:
        for _, c in consumers:
            c.stop()

    log.info("bridge stopped")
    return 0


if __name__ == "__main__":
    sys.exit(main())
