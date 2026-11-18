#!/usr/bin/env python3
"""Feishu interactive card builders (Card 2.0).

Cards are the difference between "a wall of text you scroll past" and
"something you can act on". Every builder returns a plain dict that
`json.dumps` can serialise straight into `--content` / `--data`.

Only the pieces this bridge actually uses are here: a header, markdown,
a divider, and buttons. Adding more card chrome is easy later; starting
minimal keeps the failure modes small.
"""

from __future__ import annotations


def _button(text: str, kind: str, value: dict) -> dict:
    return {
        "tag": "button",
        "text": {"tag": "plain_text", "content": text},
        "type": kind,
        "width": "fill",
        "behaviors": [{"type": "callback", "value": value}],
    }


def _base(title: str, subtitle: str | None, template: str, elements: list) -> dict:
    header: dict = {
        "title": {"tag": "plain_text", "content": title},
        "template": template,
    }
    if subtitle:
        header["subtitle"] = {"tag": "plain_text", "content": subtitle}
    return {
        "schema": "2.0",
        "config": {"width_mode": "default"},
        "header": header,
        "body": {"elements": elements},
    }


def markdown(text: str) -> dict:
    return {"tag": "markdown", "content": text}


def plain(text: str) -> dict:
    """Literal text.

    Anything that must be shown exactly — an instruction the user is about to
    approve — goes here, not through markdown. Feishu parses markup inside a
    markdown component, so an instruction containing something like
    `<person id='x'></person>` would be rejected as an invalid person tag
    instead of being displayed as the text it is.
    """
    return {"tag": "div", "text": {"tag": "plain_text", "content": text}}


def divider() -> dict:
    return {"tag": "hr"}


def notice(title: str, body: str, *, subtitle: str | None = None, template: str = "blue") -> dict:
    """A read-only card: status, results, errors."""
    elements = [markdown(body)]
    return _base(title, subtitle, template, elements)


def confirm(
    pid: str,
    title: str,
    body: str,
    *,
    subtitle: str | None = None,
    detail: str | None = None,
    approve: str = "同意",
    reject: str = "拒绝",
    template: str = "orange",
) -> dict:
    """A card that asks before doing something irreversible.

    `pid` round-trips through the button value and keys the pending action
    on the bridge side. The card itself carries no authority — the callback
    handler re-checks the operator and the action's age before acting.
    """
    buttons = {
        "tag": "column_set",
        "flex_mode": "stretch",
        "columns": [
            {
                "tag": "column",
                "width": "weighted",
                "weight": 1,
                "elements": [_button(approve, "primary_filled", {"op": "confirm", "pid": pid, "choice": "yes"})],
            },
            {
                "tag": "column",
                "width": "weighted",
                "weight": 1,
                "elements": [_button(reject, "default", {"op": "confirm", "pid": pid, "choice": "no"})],
            },
        ],
    }
    elements = [markdown(body)]
    if detail:
        elements.append(plain(detail))
    elements.append(buttons)
    return _base(title, subtitle, template, elements)


def resolved(
    title: str,
    body: str,
    *,
    template: str = "green",
    subtitle: str | None = None,
    detail: str | None = None,
) -> dict:
    """The same card after the decision was made — buttons removed.

    `detail` stays a plain-text block here too: the instruction must render
    the same before and after approval, markup and all.
    """
    elements = [markdown(body)]
    if detail:
        elements.append(plain(detail))
    return _base(title, subtitle, template, elements)
