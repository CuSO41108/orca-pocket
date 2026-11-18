#!/usr/bin/env python3
"""Fail if anything that should stay private is about to be published.

Privacy here is not a one-time scrub. The bridge logs ids, the config holds
an open_id, and both creep back in the moment someone pastes a real example
into the README. Run this before every push; the pre-commit hook does it for
you if you enable it (see README).

    python check_privacy.py          # scan files git would publish
    python check_privacy.py --all    # scan every file too

Extra literal terms (your name, your project names, your machine name) go in
privacy.local.txt, one per line, which is gitignored.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SKIP_DIRS = {".git", "logs", "__pycache__", ".venv", "node_modules"}
# Local-only files: they are *supposed* to hold real ids. The danger is
# publishing them, which check_tracked_config() covers separately.
SKIP_FILES = {"config.json", "privacy.local.txt", "state.json", "state.json.tmp"}

# Anything that looks like real Feishu data rather than a placeholder.
PATTERNS = {
    "open_id": re.compile(r"\bou_[A-Za-z0-9_-]{20,}\b"),
    "chat_id": re.compile(r"\boc_[A-Za-z0-9_-]{20,}\b"),
    "app_id": re.compile(r"\bcli_[a-z0-9]{10,}\b"),
    "windows_user_path": re.compile(r"[Cc]:\\+Users\\+[A-Za-z0-9._-]+"),
    "posix_user_path": re.compile(r"/(?:c|mnt/c)/Users/[A-Za-z0-9._-]+"),
    "bearer_token": re.compile(r"Bearer\s+[A-Za-z0-9._-]{20,}"),
    "app_secret": re.compile(r"""app_secret["']?\s*[:=]\s*["'][^"']{8,}""", re.I),
    "private_key": re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----"),
}


def tracked_files() -> list[Path]:
    try:
        out = subprocess.run(
            ["git", "ls-files", "-z"],
            cwd=ROOT, capture_output=True, check=True,
        ).stdout.decode("utf-8", "replace")
    except (subprocess.CalledProcessError, FileNotFoundError):
        return []
    return [ROOT / p for p in out.split("\0") if p]


def all_files() -> list[Path]:
    found: list[Path] = []
    for path in ROOT.rglob("*"):
        if not path.is_file():
            continue
        if path.name in SKIP_FILES:
            continue
        if any(part in SKIP_DIRS for part in path.relative_to(ROOT).parts):
            continue
        found.append(path)
    return found


def local_terms() -> list[str]:
    path = ROOT / "privacy.local.txt"
    if not path.is_file():
        return []
    return [
        line.strip()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.startswith("#")
    ]


def scan(files: list[Path], terms: list[str]) -> list[str]:
    findings: list[str] = []
    for path in files:
        try:
            text = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue
        rel = path.relative_to(ROOT).as_posix()
        for lineno, line in enumerate(text.splitlines(), 1):
            for name, rx in PATTERNS.items():
                if rx.search(line):
                    findings.append(f"{rel}:{lineno}: {name} -> {line.strip()[:100]}")
            for term in terms:
                if term.lower() in line.lower():
                    findings.append(f"{rel}:{lineno}: denylist {term!r} -> {line.strip()[:100]}")
    return findings


def check_tracked_config() -> list[str]:
    """config.json carries a real open_id; it must never be tracked."""
    try:
        out = subprocess.run(
            ["git", "ls-files", "--error-unmatch", "config.json"],
            cwd=ROOT, capture_output=True,
        )
    except FileNotFoundError:
        return []
    if out.returncode == 0:
        return ["config.json is tracked by git — `git rm --cached config.json`"]
    return []


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="scan for private data before publishing")
    parser.add_argument("--all", action="store_true", help="scan every file, not just tracked ones")
    args = parser.parse_args(argv)

    files = all_files() if args.all else (tracked_files() or all_files())
    terms = local_terms()
    findings = scan(files, terms) + check_tracked_config()

    if not findings:
        where = "every file" if args.all else f"{len(files)} tracked file(s)"
        print(f"privacy check passed ({where}, {len(terms)} local term(s))")
        return 0

    print(f"privacy check FAILED — {len(findings)} hit(s):\n")
    for finding in findings:
        print(f"  {finding}")
    print("\nFix: replace with a <PLACEHOLDER>, or gitignore the file.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
