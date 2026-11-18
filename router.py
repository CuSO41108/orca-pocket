#!/usr/bin/env python3
"""Turn a sentence into a structured action.

This is the only piece of the bridge that needs a model. Everything it can
emit is a small, closed set of intents, and the model never decides *what to
send* — only which intent the sentence is and which session it names. The
rules layer still owns every orca call.

If the model is missing or misbehaves, `fallback()` keeps basic queries working
rather than dropping the message on the floor.
"""

from __future__ import annotations

import json
import logging

from llm import LLM, LLMError

log = logging.getLogger("bridge.router")

INTENTS = {"status", "ask", "send", "arm", "disarm", "help", "unknown"}

SYSTEM = """\
你是一个路由器。用户在用中文和飞书机器人聊天，这个机器人管理着一台电脑上的多个
coding agent 会话。你的任务只是把用户这句话翻译成一个 JSON 动作，不执行任何操作。

当前可用的会话名：{sessions}

输出 JSON，字段固定为：
- intent: status | ask | send | arm | disarm | help | unknown
  * status —— 想了解总体情况：有几个任务在跑、都在干嘛
  * ask    —— 打听某一个具体会话的进度或内容
  * send   —— 明确要求某个会话去做一件事（这会把文字真的发进终端）
  * arm    —— 想在一段时间内放开写权限，比如"接下来半小时我说什么都照做"
  * disarm —— 想立刻上锁
  * help   —— 想知道机器人能做什么
  * unknown —— 以上都不是
- session: 会话名，必须原样取自上面列表；说不清或没提到就是 null
- session_mentioned: 用户是否点名了某个具体会话（true/false）。
  比如"让它重跑"没有点名 -> false；"让 foo 重跑"即使 foo 不在列表里也 -> true
- text: intent 为 send 时，要发给会话的正文（去掉"让X"、"告诉X"这类引导语）；
        其它 intent 一律留空字符串
- minutes: intent 为 arm 时的分钟数；用户没说就 30

判断要点：
- 提问、打听、好奇 -> ask，不是 send。只有明确要求它去做事才是 send。
- 用户只是感叹或闲聊 -> unknown，不要硬套成 send。
- 点名了一个列表里没有的会话，session 填 null，但 session_mentioned 必须是 true。
  不要为了凑出一个结果而挑一个相近的名字。

只输出 JSON 对象，不要解释。"""


def _session_names(sessions) -> list[str]:
    return [s.short for s in sessions]


def fallback(text: str) -> dict | None:
    """Keyword-only routing for when the model is unavailable.

    Deliberately conservative: it recognises read-only intents and nothing
    else. Guessing at a write intent without the model is how you send
    "现在几点了" into someone's editor.
    """
    t = text.strip().lower()
    if any(k in t for k in ("上锁", "锁定", "disarm")):
        return {"intent": "disarm", "session": None, "text": "", "minutes": 30}
    if any(k in t for k in ("什么情况", "状态", "进度", "哪些", "在跑", "怎么样")):
        return {"intent": "status", "session": None, "text": "", "minutes": 30}
    if any(k in t for k in ("help", "帮助", "能干什么", "会什么")):
        return {"intent": "help", "session": None, "text": "", "minutes": 30}
    return None


def _normalize(data: dict, sessions) -> dict:
    names = {n.lower(): n for n in _session_names(sessions)}
    intent = str(data.get("intent") or "unknown").lower().strip()
    if intent not in INTENTS:
        intent = "unknown"

    raw_session = data.get("session")
    session = names.get(str(raw_session).lower().strip()) if raw_session else None

    # A named target that does not resolve must not silently become "no
    # target": that turns an explicit "send to X" into "send to whatever has
    # focus". `session_mentioned` is what separates "said nothing" from
    # "said something we could not match".
    mentioned = data.get("session_mentioned")
    # A send route must say explicitly whether a target was named. Defaulting
    # a missing flag to `false` turned an incomplete response into "no target",
    # which then resolves to whatever has focus — and writes there.
    if intent == "send" and not isinstance(mentioned, bool):
        log.warning("router omitted session_mentioned on a send route; refusing to guess")
        return {"intent": "unknown", "session": None, "invalid_target": None, "text": "", "minutes": 30}

    invalid_target = None
    if session is None and (raw_session or mentioned):
        invalid_target = str(raw_session or "（未识别的会话名）")

    text = data.get("text") or ""
    if not isinstance(text, str):
        text = str(text)

    try:
        minutes = int(data.get("minutes") or 30)
    except (TypeError, ValueError):
        minutes = 30
    minutes = max(1, min(minutes, 120))

    return {
        "intent": intent,
        "session": session,
        "invalid_target": invalid_target,
        "text": text.strip(),
        "minutes": minutes,
    }


def classify(llm: LLM, text: str, sessions) -> dict:
    """Ask the model what this sentence means.

    Raises LLMError; the caller decides whether to fall back.
    """
    messages = [
        {"role": "system", "content": SYSTEM.format(sessions="、".join(_session_names(sessions)) or "（无）")},
        {"role": "user", "content": text},
    ]
    raw = llm.complete(
        messages,
        json_mode=True,
        # The JSON has to carry the instruction verbatim. A fixed 200-token
        # ceiling truncated anything longer, and a truncated object degrades
        # to the keyword path, which cannot send at all.
        max_tokens=min(1500, 200 + 2 * len(text)),
        temperature=0,
    )
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        raise LLMError(f"router returned non-JSON: {raw[:80]!r}") from None
    if not isinstance(data, dict):
        raise LLMError("router returned a non-object")
    return _normalize(data, sessions)
