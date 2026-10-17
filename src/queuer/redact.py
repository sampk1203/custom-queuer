"""Secret redaction for text that leaves the machine (F31/S4/S5).

Everything queuer sends to Telegram (finished-job message, Details, Logs tail, list and
status text) goes through here first. Local output (`qur show`, `qur logs`, `qur list`)
is never redacted: you see your own data.

Built-in patterns (all replaced by `***`):

  * `NAME=VALUE` where NAME ends in key/token/secret/password/passwd/passphrase, or has a
    segment that is secret/password/passwd/passphrase (`SECRET_KEY_BASE`, `db.password`).
    Deliberately NOT "contains": `--tokenizer=gpt2`, `max_tokens=100`, `token_count=5`
    and `keyboard=us` are normal and stay readable.
  * `--password=VAL`, `--password VAL`, `--api-key VAL`, `--access-token=VAL` (same name rule)
  * `Authorization: Bearer ...` (also Basic/Token) and a bare `Bearer <long token>`
  * AWS access key ids (AKIA.../ASIA...), GitHub tokens (ghp_/gho_/ghu_/ghs_/ghr_/github_pat_),
    Telegram bot tokens (`123456789:AAH...`)
  * the password in `scheme://user:pass@host`

Extra patterns: a JSON list of regexes in `redact.json` next to `notify.json`
(`~/.config/queuer/redact.json`, or `QUEUER_REDACT_PATH`). A match is replaced by `***`;
if the regex has a group, only the first group is replaced. The file must be yours and
not writable by others (a loose read mode is tightened to 0600). A bad regex, or a
pattern that matches the empty string, is skipped with one line in the journal (the
pattern itself is never printed: it may be the secret). It never stops the daemon.

Limits: this is pattern matching, not magic. A secret passed as a plain argument
(`mysql -pSECRET`, `sshpass -p SECRET`) or printed without a label is not recognised.
Pass secrets through the environment or a file, not through arguments.
"""

from __future__ import annotations

import json
import os
import re
import stat
import sys
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any

MASK = "***"
REDACT_PATH_ENV = "QUEUER_REDACT_PATH"
MAX_EXTRA_PATTERNS = 100
MAX_EXTRA_PATTERN_CHARS = 500

_NAME_CHARS = r"[A-Za-z0-9_.-]"
# a name that looks like it holds a secret (matched case-insensitively)
_NAME_CORE = (
    r"(?:" + _NAME_CHARS + r"*(?:key|token|secret|passw(?:or)?d|passphrase)"
    r"|(?:[A-Za-z0-9]+[_.-])*(?:secret|passw(?:or)?d|passphrase)(?:[_.-][A-Za-z0-9]+)*)"
    r"(?![A-Za-z0-9_.-])"
)
# a quoted value, or an unquoted one up to whitespace / a quote / a shell separator (`;&|<>`)
_VALUE = r"""(?:"[^"\n]*"|'[^'\n]*'|[^\s"';&|<>]+)"""

_Repl = str | Callable[[re.Match[str]], str]


def _auth_repl(m: re.Match[str]) -> str:
    return m[1] + (m[2] + " " if m[2] else "") + MASK


_BUILTIN: list[tuple[re.Pattern[str], _Repl]] = [
    # Authorization: Bearer xxx / Basic xxx / Token xxx (keeps the scheme)
    (re.compile(r"""\b(authorization[ \t]*[:=][ \t]*)(?:(bearer|basic|token|digest|negotiate)[ \t]+)?[^\s"']+""", re.I), _auth_repl),
    # scheme://user:pass@host -> scheme://user:***@host
    (re.compile(r"""([A-Za-z][A-Za-z0-9+.-]*://[^\s/@:'"]+:)[^\s/@'"]+(?=@)"""), r"\1" + MASK),
    # Telegram bot token
    (re.compile(r"(?<![0-9])[0-9]{6,12}:AA[A-Za-z0-9_-]{30,}"), MASK),
    # GitHub tokens
    (re.compile(r"(?<![A-Za-z0-9])(?:gh[pousr]_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,})"), MASK),
    # AWS access key id
    (re.compile(r"(?<![A-Za-z0-9])(?:AKIA|ASIA)[0-9A-Z]{16}(?![A-Za-z0-9])"), MASK),
    # a bare "Bearer <long token>"
    (re.compile(r"\b(Bearer[ \t]+)[A-Za-z0-9._~+/=-]{16,}"), r"\1" + MASK),
    # --password=VAL, --api-key VAL
    (re.compile(rf"(?<![A-Za-z0-9_-])(--{_NAME_CORE}=){_VALUE}", re.I), r"\1" + MASK),
    (re.compile(rf"(?<![A-Za-z0-9_-])(--{_NAME_CORE}[ \t]+)(?!-){_VALUE}", re.I), r"\1" + MASK),
    # NAME=VALUE (also inside URLs: ?access_token=...)
    (re.compile(rf"(?<![A-Za-z0-9_.-])({_NAME_CORE}[ \t]*=(?!=)[ \t]*){_VALUE}", re.I), r"\1" + MASK),
]

_FLAG_NAME = re.compile(rf"--{_NAME_CORE}", re.I)
_ASSIGN_NAME = re.compile(_NAME_CORE, re.I)


# ---------------------------------------------------------------------------
# user patterns file
# ---------------------------------------------------------------------------

_warned: set[str] = set()
_extra_cache: tuple[tuple[str, int, int, int], list[re.Pattern[str]]] | None = None


def default_path() -> Path:
    """`redact.json` next to the notification config (so a test or a scratch setup that
    moves `notify.json` moves this file too), unless QUEUER_REDACT_PATH says otherwise."""
    override = os.environ.get(REDACT_PATH_ENV)
    if override:
        return Path(override)
    notify_override = os.environ.get("QUEUER_NOTIFY_PATH")
    if notify_override:
        return Path(notify_override).with_name("redact.json")
    base = os.environ.get("XDG_CONFIG_HOME") or str(Path.home() / ".config")
    return Path(base) / "queuer" / "redact.json"


def _warn(msg: str) -> None:
    """One journal line per distinct problem per process."""
    if msg in _warned:
        return
    _warned.add(msg)
    print(f"[queuerd] redact: {msg}", file=sys.stderr)


def _compile(i: int, raw: Any) -> re.Pattern[str] | None:
    if not isinstance(raw, str) or not raw or len(raw) > MAX_EXTRA_PATTERN_CHARS:
        _warn(f"pattern #{i} is not a non-empty string of at most {MAX_EXTRA_PATTERN_CHARS} characters; skipped")
        return None
    try:
        rx = re.compile(raw)
    except re.error as e:
        _warn(f"pattern #{i} is not a valid regex ({e.msg}); skipped")
        return None
    if rx.search("") is not None:
        _warn(f"pattern #{i} matches the empty string; skipped")
        return None
    return rx


def extra_patterns(path: Path | None = None) -> list[re.Pattern[str]]:
    """The compiled patterns from redact.json ([] if there is no usable file). Re-read
    only when the file changes."""
    global _extra_cache
    path = path or default_path()
    try:
        st = os.lstat(path)
    except OSError:
        return []
    if stat.S_ISLNK(st.st_mode) or not stat.S_ISREG(st.st_mode):
        _warn(f"{path} is not a regular file; ignored")
        return []
    if st.st_uid != os.getuid():
        _warn(f"{path} is owned by another user; ignored")
        return []
    if st.st_mode & 0o022:
        _warn(f"{path} is writable by others; ignored (chmod 600 it)")
        return []
    key = (str(path), st.st_mtime_ns, st.st_size, st.st_ino)
    if _extra_cache is not None and _extra_cache[0] == key:
        return _extra_cache[1]
    if st.st_mode & 0o044:
        try:
            os.chmod(path, 0o600)
            _warn(f"{path} was mode {st.st_mode & 0o777:o}; tightened to 600")
        except OSError:
            pass
    try:
        raw = json.loads(path.read_text())
    except (OSError, ValueError):
        _warn(f"{path} is not readable JSON; ignored")
        return []
    if not isinstance(raw, list):
        _warn(f"{path} must hold a JSON list of regex strings; ignored")
        return []
    compiled = [rx for i, item in enumerate(raw[:MAX_EXTRA_PATTERNS]) if (rx := _compile(i, item)) is not None]
    if len(raw) > MAX_EXTRA_PATTERNS:
        _warn(f"only the first {MAX_EXTRA_PATTERNS} patterns are used")
    _extra_cache = (key, compiled)
    return compiled


def _mask_match(m: re.Match[str]) -> str:
    """Whole match -> `***`; with a group, only the first group."""
    if m.re.groups and m.start(1) != -1:
        whole, base = m.group(0), m.start()
        return whole[: m.start(1) - base] + MASK + whole[m.end(1) - base:]
    return MASK


# ---------------------------------------------------------------------------
# public API
# ---------------------------------------------------------------------------


def redact_text(text: str) -> str:
    """`text` with secrets replaced by `***` (built-in patterns, then redact.json)."""
    if not text:
        return text
    for rx, repl in _BUILTIN:
        text = rx.sub(repl, text)
    for rx in extra_patterns():
        text = rx.sub(_mask_match, text)
    return text


def redact_argv(argv: Iterable[Any]) -> list[Any]:
    """A command's words with secrets masked. Works word by word, so a value with spaces
    (`--password="a b"` is one word) is hidden completely and the display quoting stays
    correct: `--flag=VAL` and `NAME=VAL` words, and the word after a bare `--flag`."""
    out: list[Any] = []
    mask_next = False
    for word in argv:
        if not isinstance(word, str):
            out.append(word)
            mask_next = False
            continue
        if mask_next and not (word.startswith("-") and len(word) > 1):
            out.append(MASK)
            mask_next = False
            continue
        mask_next = False
        name, eq, value = word.partition("=")
        if word.startswith("--") and _FLAG_NAME.fullmatch(name):
            if not eq:
                mask_next = True
                out.append(word)
            else:
                out.append(f"{name}={MASK}" if value else word)
        elif eq and value and _ASSIGN_NAME.fullmatch(name):
            out.append(f"{name}={MASK}")
        else:
            out.append(redact_text(word))
    return out
