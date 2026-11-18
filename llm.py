#!/usr/bin/env python3
"""Minimal OpenAI-compatible chat client.

DeepSeek, most self-hosted relays, and OpenAI itself all speak this request
shape, so one implementation covers the default and whatever a user swaps in
via config. Standard library only — this process has to survive on a bare
Windows box with no venv.

The API key is read from the environment by variable *name*; it is never
written to config.json, never logged, and never echoed in errors.
"""

from __future__ import annotations

import http.client
import json
import os
import urllib.error
import urllib.request

DEFAULTS = {
    "deepseek": {
        "base_url": "https://api.deepseek.com",
        "model": "deepseek-chat",
        "api_key_env": "DEEPSEEK_API_KEY",
    },
    "openai": {
        "base_url": "https://api.openai.com/v1",
        "model": "gpt-4o-mini",
        "api_key_env": "OPENAI_API_KEY",
    },
}


class LLMError(RuntimeError):
    """Raised for any failure to get a completion. Always safe to log."""


class LLM:
    def __init__(self, cfg: dict | None = None):
        cfg = cfg or {}
        provider = (cfg.get("provider") or "deepseek").lower()
        defaults = DEFAULTS.get(provider, DEFAULTS["deepseek"])
        self.provider = provider
        self.base_url = (cfg.get("base_url") or defaults["base_url"]).rstrip("/")
        self.model = cfg.get("model") or defaults["model"]
        self.api_key_env = cfg.get("api_key_env") or defaults["api_key_env"]
        self.timeout = float(cfg.get("timeout", 20))
        self.enabled = bool(cfg.get("enabled", True))

    @property
    def api_key(self) -> str:
        # A trailing newline in an env var makes header serialization raise a
        # ValueError that embeds the whole Authorization header — which would
        # then be logged. Strip it, and treat the key as a plain string.
        return os.environ.get(self.api_key_env, "").strip()

    def available(self) -> bool:
        """Whether a call could be attempted at all."""
        return self.enabled and bool(self.api_key) and bool(self.base_url)

    def why_unavailable(self) -> str:
        if not self.enabled:
            return "llm.enabled is false"
        if not self.api_key:
            return f"environment variable {self.api_key_env} is empty"
        return "no base_url"

    def complete(
        self,
        messages: list[dict],
        *,
        json_mode: bool = False,
        max_tokens: int = 500,
        temperature: float = 0.2,
    ) -> str:
        if not self.available():
            raise LLMError(f"model unavailable: {self.why_unavailable()}")

        payload = {
            "model": self.model,
            "messages": messages,
            "max_tokens": max_tokens,
            "temperature": temperature,
        }
        if json_mode:
            payload["response_format"] = {"type": "json_object"}

        try:
            # Request construction is inside the guard: a malformed header
            # raises ValueError here, and that message contains the key.
            req = urllib.request.Request(
                f"{self.base_url}/chat/completions",
                data=json.dumps(payload).encode("utf-8"),
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                },
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                data = json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            # Never surface the request body: it carries the Authorization
            # header in some proxies' error echoes.
            raise LLMError(f"HTTP {exc.code} from {self.provider}") from None
        except ValueError:
            # Header validation. The message would contain the credential.
            raise LLMError("request rejected locally (malformed credential?)") from None
        except (urllib.error.URLError, TimeoutError, OSError, http.client.HTTPException) as exc:
            # HTTPException covers a connection dropped mid-response
            # (IncompleteRead), which is not an OSError.
            raise LLMError(f"network error talking to {self.provider}: {exc}") from None
        except json.JSONDecodeError:
            raise LLMError(f"{self.provider} returned non-JSON") from None

        try:
            content = data["choices"][0]["message"]["content"]
        except (KeyError, IndexError, TypeError):
            raise LLMError(f"unexpected response shape from {self.provider}") from None
        if not isinstance(content, str):
            # An OpenAI-compatible refusal puts `content: null` and the reason
            # in `refusal`. Returning the null made callers crash with a
            # TypeError that their LLMError fallback never saw.
            raise LLMError(f"{self.provider} returned no text content (refusal?)")
        return content
