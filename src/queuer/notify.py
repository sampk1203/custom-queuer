"""Telegram notifier for queuer.

One qur can be connected to several bots at once. Every finished-job message
goes to every connected bot (and every chat paired with it), and the daemon
polls each bot that has `listen` on for presses of the Requeue / Details
buttons. A press on a bot's message reaches the qur that polls that bot, so
give each machine its own bot if you want its buttons to always reach it
(Telegram allows only one `getUpdates` consumer per bot token).

Any number of qurs can share one bot for *sending*. Listening is used in two
places: `qur connect` while waiting for you to press Start after scanning the
QR, and the machines whose bot entry has `listen` on (the default), whose
daemon polls for button presses. Machines set up with `qur connect --import`
get `listen` off for bots that another machine already handles.

Config lives in a 0600 JSON file (it holds the bot tokens):

    {"qur_id": "a3f9",
     "bots": [{"token": "123:ABC...", "chat_ids": [111111111],
               "listen": true, "username": "my_bot"}]}

`listen` and `username` are optional (`listen` defaults to on). The older
single-bot layout `{"qur_id", "token", "chat_ids", "listen"}` is still read,
and is rewritten in the layout above the next time the config is saved.

`qur_id` is generated per machine (rename it with `qur connect --id`) and is
never exported, so every qur keeps its own id in the messages it sends.

Most functions work on a flat single-bot view `{"qur_id", "token",
"chat_ids", "listen"}`; `bot_cfgs` produces one per connected bot.

Stdlib only (urllib); every network call goes through `_api` so tests can
replace it.
"""

from __future__ import annotations

import base64
import binascii
import contextlib
import html
import json
import os
import re
import secrets
import shlex
import socket
import stat
import tempfile
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterator

from queuer import jobenv

API_BASE = "https://api.telegram.org"
SEND_TIMEOUT_SECS = 10
POLL_SECS = 10
PAIR_TIMEOUT_SECS = 300
EXPORT_PREFIX = "qurtg1:"  # one bot
EXPORT_PREFIX_MULTI = "qurtg2:"  # several bots
TOKEN_RE = re.compile(r"^\d+:[A-Za-z0-9_-]+$")
ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,16}$")
CALLBACK_ACTIONS = ("rq", "sh", "lg", "lc", "rl", "cn")  # requeue, details, log tail, list one channel (3rd field = channel), requeue from a list, cancel
DETAIL_CMD_MAX_CHARS = 1200  # each command in Details (halved until the message fits)
DETAIL_FIELD_MAX_CHARS = 500  # cwd, log path, note in Details
CMD_MAX_CHARS = 1000  # command in the finished-job message
LIST_CMD_MAX_CHARS = 300  # command per line in a channel list, at most (less when the list is long)
LIST_CMD_MIN_CHARS = 40
TG_MAX_CHARS = 4096
LOG_TAIL_LINES = 20  # lines the Logs button shows (fewer if they would not fit in one message)
LOG_READ_BYTES = 16384  # how far back from the end of the log file to read
LIST_MAX_BACKLOG = 10  # finished jobs with buttons in a list message (Telegram allows 100 buttons per message)
LIST_TEXT_MAX_BACKLOG = 100  # finished jobs considered for the list text, however long the history is (_fit trims to 4096 chars)
LIST_MAX_QUEUE_BUTTONS = 20  # Telegram allows 100 buttons per message
MENU_STATUS = "📊 Status"
MENU_LIST = "📋 List"
MENU_ACTIONS = {MENU_STATUS: "status", MENU_LIST: "list", "/status": "status", "/list": "list", "/menu": "menu"}


class NotifyError(Exception):
    """A Telegram call failed. `code` is the Bot API error_code (or HTTP
    status) when there is one. Never contains the bot token."""

    def __init__(self, message: str, code: int | None = None) -> None:
        super().__init__(message)
        self.code = code


class PairingConflict(NotifyError):
    """getUpdates refused: another process is already polling this bot."""


# ---------------------------------------------------------------------------
# config file
# ---------------------------------------------------------------------------


def default_path() -> Path:
    override = os.environ.get("QUEUER_NOTIFY_PATH")
    if override:
        return Path(override)
    base = os.environ.get("XDG_CONFIG_HOME") or str(Path.home() / ".config")
    return Path(base) / "queuer" / "notify.json"


def _clean_bot(raw: Any) -> dict[str, Any] | None:
    """One bot entry in canonical form, or None if it is malformed."""
    if not isinstance(raw, dict):
        return None
    token, chat_ids = raw.get("token"), raw.get("chat_ids")
    if not (isinstance(token, str) and TOKEN_RE.match(token)):
        return None
    if not (isinstance(chat_ids, list) and all(isinstance(c, int) and not isinstance(c, bool) for c in chat_ids)):
        return None
    bot: dict[str, Any] = {"token": token, "chat_ids": list(chat_ids)}
    if isinstance(raw.get("listen"), bool):
        bot["listen"] = raw["listen"]
    if isinstance(raw.get("username"), str) and raw["username"]:
        bot["username"] = raw["username"]
    return bot


def normalize(raw: Any) -> dict[str, Any] | None:
    """Canonical `{"qur_id", "bots": [...]}` from either config layout (the
    older single-bot one included), or None if it is malformed or has no bot.
    Bots with the same token are merged away: the first one wins."""
    if not isinstance(raw, dict):
        return None
    if not isinstance(raw.get("qur_id"), str) or not raw["qur_id"]:
        return None
    entries = raw["bots"] if "bots" in raw else [raw]
    if not isinstance(entries, list) or not entries:
        return None
    bots: list[dict[str, Any]] = []
    for entry in entries:
        bot = _clean_bot(entry)
        if bot is None:
            return None
        if all(b["token"] != bot["token"] for b in bots):
            bots.append(bot)
    return {"qur_id": raw["qur_id"], "bots": bots}


def load(path: Path | None = None) -> dict[str, Any] | None:
    """The saved config (canonical layout), or None if missing, unreadable
    or incomplete."""
    path = path or default_path()
    try:
        raw = json.loads(path.read_text())
    except (OSError, ValueError):
        return None
    return normalize(raw)


def save(cfg: dict[str, Any], path: Path | None = None) -> None:
    """Atomic write, mode 0600 from the first byte (the file holds secrets).
    Accepts either layout; always writes the canonical one."""
    norm = normalize(cfg)
    if norm is None:
        raise ValueError("invalid notify config")
    path = path or default_path()
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=".notify-")
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(norm, f)
        os.chmod(tmp, 0o600)
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def check_permissions(path: Path | None = None, fix: bool = False) -> str | None:
    """Problem with the config file's (or its folder's) permissions, as a
    sentence, or None if it is fine or missing. The file holds bot tokens: it
    must be a regular file owned by us with no group/other access. With `fix`,
    a loose mode on something we own is tightened (0600 file, 0700 folder);
    a symlink or a file owned by someone else is only reported."""
    path = path or default_path()
    try:
        st = os.lstat(path)
    except OSError:
        return None
    if stat.S_ISLNK(st.st_mode):
        return f"{path} is a symlink; the bot token could be read from elsewhere"
    if st.st_uid != os.getuid():
        return f"{path} is owned by another user"
    problems = []
    if st.st_mode & 0o077:
        problems.append(f"{path} is mode {st.st_mode & 0o777:o} (other users can read the bot token)")
        if fix:
            os.chmod(path, 0o600)
            problems = [f"{path} was mode {st.st_mode & 0o777:o}; tightened to 600"]
    try:
        dst = os.lstat(path.parent)
        if stat.S_ISDIR(dst.st_mode) and dst.st_uid == os.getuid() and dst.st_mode & 0o077:
            problems.append(f"{path.parent} is mode {dst.st_mode & 0o777:o}")
            if fix:
                os.chmod(path.parent, 0o700)
                problems[-1] += "; tightened to 700"
    except OSError:
        pass
    return "; ".join(problems) or None


def remove(path: Path | None = None) -> bool:
    """Delete the config (every bot). True if there was one."""
    path = path or default_path()
    try:
        path.unlink()
        return True
    except FileNotFoundError:
        return False


def new_qur_id() -> str:
    return secrets.token_hex(2)


# ---------------------------------------------------------------------------
# several bots in one config
# ---------------------------------------------------------------------------


def bot_id(token: str) -> str:
    """The public half of a token (the bot's numeric id); safe to print."""
    return token.split(":")[0]


def bot_label(bot: dict[str, Any]) -> str:
    """`@username (id)` when the username is known, else just the id."""
    name = bot.get("username")
    return f"@{name} ({bot_id(bot['token'])})" if name else bot_id(bot["token"])


def bot_cfgs(cfg: dict[str, Any]) -> list[dict[str, Any]]:
    """Flat single-bot views `{"qur_id", "token", "chat_ids", ...}`, one per
    connected bot -- the shape `send`, `job_markup`, `format_job` etc. take."""
    return [{"qur_id": cfg["qur_id"], **bot} for bot in cfg["bots"]]


def find_bots(cfg: dict[str, Any], ref: str) -> list[dict[str, Any]]:
    """Bots matching `ref`: the numeric bot id or the @username (case and
    leading @ ignored). Empty if none match."""
    ref = ref.strip().lstrip("@").lower()
    return [
        bot for bot in cfg["bots"]
        if ref and (bot_id(bot["token"]) == ref or (bot.get("username") or "").lower() == ref)
    ]


def with_bot(cfg: dict[str, Any] | None, bot: dict[str, Any], qur_id: str | None = None) -> dict[str, Any]:
    """A copy of `cfg` with `bot` added, or replacing the bot that has the
    same token. `cfg` may be None (first bot): a new qur id is made."""
    base_id = qur_id or (cfg["qur_id"] if cfg else new_qur_id())
    bots = [dict(b) for b in cfg["bots"]] if cfg else []
    for i, existing in enumerate(bots):
        if existing["token"] == bot["token"]:
            bots[i] = dict(bot)
            break
    else:
        bots.append(dict(bot))
    return {"qur_id": base_id, "bots": bots}


def without_bot(cfg: dict[str, Any], token: str) -> dict[str, Any] | None:
    """A copy of `cfg` minus the bot with this token; None if none is left."""
    bots = [dict(b) for b in cfg["bots"] if b["token"] != token]
    return {"qur_id": cfg["qur_id"], "bots": bots} if bots else None


def send_all(cfg: dict[str, Any], text: str, markup_for: Callable[[dict[str, Any]], dict[str, Any] | None] | None = None,
             parse_mode: str | None = None) -> list[str]:
    """`send` through every bot that has a chat. `markup_for(bot_cfg)` gives
    the keyboard for that bot (or None). One failing bot does not stop the
    others. Returns error strings, each prefixed with the bot."""
    errors: list[str] = []
    for bcfg in bot_cfgs(cfg):
        if not bcfg["chat_ids"]:
            continue
        for err in send(bcfg, text, markup_for(bcfg) if markup_for else None, parse_mode):
            errors.append(f"bot {bot_id(bcfg['token'])}: {err}")
    return errors


# ---------------------------------------------------------------------------
# export / import (tokens + chat_ids, never qur_id)
# ---------------------------------------------------------------------------


def export_string(cfg: dict[str, Any], bots: list[dict[str, Any]] | None = None) -> str:
    """Setup string for other machines: one bot -> `qurtg1:` (older qurs can
    read it), several -> `qurtg2:`. `bots` limits it to some of the config's
    bots; bots with no paired chat are never exported."""
    entries = cfg["bots"] if "bots" in cfg else [cfg]  # either layout; no qur_id needed
    cleaned = [b for b in (_clean_bot(e) for e in entries) if b is not None]
    chosen = [b for b in (bots if bots is not None else cleaned) if b["chat_ids"]]
    if not chosen:
        raise ValueError("nothing to export")
    if len(chosen) == 1:
        payload = json.dumps({"t": chosen[0]["token"], "c": chosen[0]["chat_ids"]}, separators=(",", ":"))
        prefix = EXPORT_PREFIX
    else:
        payload = json.dumps({"b": [{"t": b["token"], "c": b["chat_ids"]} for b in chosen]}, separators=(",", ":"))
        prefix = EXPORT_PREFIX_MULTI
    return prefix + base64.urlsafe_b64encode(payload.encode()).decode().rstrip("=")


def parse_export_bots(s: str) -> list[tuple[str, list[int]]]:
    """Every (token, chat_ids) in an export string (`qurtg1:` or `qurtg2:`).
    Raises ValueError on anything malformed."""
    s = s.strip()
    if s.startswith(EXPORT_PREFIX):
        prefix, multi = EXPORT_PREFIX, False
    elif s.startswith(EXPORT_PREFIX_MULTI):
        prefix, multi = EXPORT_PREFIX_MULTI, True
    else:
        raise ValueError("not a qur connect export string")
    body = s[len(prefix):]
    try:
        raw = base64.urlsafe_b64decode(body + "=" * (-len(body) % 4))
        data = json.loads(raw)
        items = [(b["t"], b["c"]) for b in data["b"]] if multi else [(data["t"], data["c"])]
    except (binascii.Error, ValueError, KeyError, TypeError):
        raise ValueError("export string is corrupt") from None
    if not items:
        raise ValueError("export string has no bots")
    seen: set[str] = set()
    for token, chat_ids in items:
        if not (isinstance(token, str) and TOKEN_RE.match(token)):
            raise ValueError("export string has an invalid bot token")
        if not (isinstance(chat_ids, list) and chat_ids
                and all(isinstance(c, int) and not isinstance(c, bool) for c in chat_ids)):
            raise ValueError("export string has no valid chat ids")
        if token in seen:
            raise ValueError("export string lists the same bot twice")
        seen.add(token)
    return [(t, list(c)) for t, c in items]


def parse_export_string(s: str) -> tuple[str, list[int]]:
    """Inverse of export_string for a one-bot string. Raises ValueError on
    anything malformed, or if the string holds several bots (use
    parse_export_bots)."""
    items = parse_export_bots(s)
    if len(items) != 1:
        raise ValueError("export string has several bots")
    return items[0]


# ---------------------------------------------------------------------------
# Telegram HTTP
# ---------------------------------------------------------------------------


# urllib reads HTTPS_PROXY / ALL_PROXY / ... from the environment. The bot token is
# part of the request URL, so by default it must never be handed to a proxy.
class _NoProxyHandler(urllib.request.ProxyHandler):
    """A ProxyHandler with an empty proxy table that stays registered on the opener.

    OpenerDirector.add_handler silently drops a handler that defines no
    `<protocol>_open/_request/_response` method, and ProxyHandler({}) defines none (it
    creates `<scheme>_open` methods only for the proxies it is given). A plain
    ProxyHandler({}) is therefore discarded, leaving the opener to behave correctly only
    by accident. The no-op request pre-processors below make it a real, inspectable
    member of the handler chain; they do not alter the request."""

    def __init__(self) -> None:
        super().__init__({})

    def http_request(self, req: urllib.request.Request) -> urllib.request.Request:
        return req

    https_request = http_request


_NO_PROXY_OPENER = urllib.request.build_opener(_NoProxyHandler())


def _open(req: urllib.request.Request, timeout: float) -> Any:
    """Open `req` with NO environment proxy, unless QUEUER_USE_PROXY is set (to
    anything but empty or 0) -- for a network that only reaches Telegram through one."""
    if os.environ.get("QUEUER_USE_PROXY", "") not in ("", "0"):
        # nosec B310: `req` is always the https://api.telegram.org URL built in _api()
        return urllib.request.urlopen(req, timeout=timeout)  # nosec B310
    return _NO_PROXY_OPENER.open(req, timeout=timeout)


def _api(token: str, method: str, params: dict[str, Any] | None = None, timeout: float = SEND_TIMEOUT_SECS) -> Any:
    """Call a Bot API method, return its `result`. Raises NotifyError.

    Error text is built from Telegram's own description or the exception
    type -- never from the request URL, which contains the token."""
    req = urllib.request.Request(
        f"{API_BASE}/bot{token}/{method}",
        data=json.dumps(params or {}).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with _open(req, timeout) as resp:
            body = json.loads(resp.read())
    except urllib.error.HTTPError as e:
        try:
            body = json.loads(e.read())
        except ValueError:
            raise NotifyError(f"HTTP {e.code}", e.code) from None
    except (urllib.error.URLError, OSError, ValueError) as e:
        raise NotifyError(f"network error ({type(e).__name__})") from None
    if not body.get("ok"):
        raise NotifyError(body.get("description", "unknown error"), body.get("error_code"))
    return body.get("result")


def check_token(token: str) -> str:
    """Validate a bot token. Returns the bot's username."""
    return _api(token, "getMe")["username"]


def send(
    cfg: dict[str, Any], text: str, reply_markup: dict[str, Any] | None = None, parse_mode: str | None = None
) -> list[str]:
    """Send `text` (plus an optional inline keyboard) to every saved chat.
    One failing chat does not stop the others. Returns a list of error
    strings (empty = all delivered)."""
    errors: list[str] = []
    for chat_id in cfg["chat_ids"]:
        params: dict[str, Any] = {"chat_id": chat_id, "text": text}
        if reply_markup:
            params["reply_markup"] = reply_markup
        if parse_mode:
            params["parse_mode"] = parse_mode
        try:
            _api(cfg["token"], "sendMessage", params)
        except NotifyError as e:
            errors.append(f"chat {chat_id}: {e}")
    return errors


def hello_text(cfg: dict[str, Any]) -> str:
    return f"🔗 connected\n{_who(cfg)}"


def _who(cfg: dict[str, Any]) -> str:
    return f"{socket.gethostname()} [{cfg['qur_id']}]"


# ---------------------------------------------------------------------------
# pairing (only used by `qur connect`)
# ---------------------------------------------------------------------------


def pair(
    token: str,
    on_link: Callable[[str], None],
    timeout: float = PAIR_TIMEOUT_SECS,
) -> int:
    """Wait for the user to open the deep link and press Start. Returns the
    chat_id. `on_link` is called once with the t.me link to show/QR.

    Only a private-chat message that is exactly `/start <secret>` is
    accepted, so a stranger messaging the (public) bot cannot pair. Raises
    PairingConflict if another process is already polling this bot, and
    TimeoutError if nobody pressed Start in time."""
    username = check_token(token)
    secret = secrets.token_urlsafe(9)
    on_link(f"https://t.me/{username}?start={secret}")

    deadline = time.monotonic() + timeout
    offset: int | None = None
    while time.monotonic() < deadline:
        params: dict[str, Any] = {"timeout": POLL_SECS, "allowed_updates": ["message"]}
        if offset is not None:
            params["offset"] = offset
        try:
            updates = _api(token, "getUpdates", params, timeout=POLL_SECS + 10)
        except NotifyError as e:
            if e.code == 409:
                raise PairingConflict(
                    f"{e} -- another qur may be pairing with this bot right now; "
                    "wait for it to finish and retry"
                ) from None
            raise
        for update in updates:
            offset = update["update_id"] + 1
            msg = update.get("message") or {}
            chat = msg.get("chat") or {}
            if chat.get("type") == "private" and msg.get("text") == f"/start {secret}":
                return int(chat["id"])
    raise TimeoutError("nobody pressed Start in time")


# ---------------------------------------------------------------------------
# validation helpers
# ---------------------------------------------------------------------------


def valid_qur_id(qur_id: str) -> bool:
    return bool(ID_RE.match(qur_id))


def listening(cfg: dict[str, Any]) -> bool:
    """Does this machine's daemon poll for Requeue button presses? On unless
    the config says otherwise (older configs have no `listen` key)."""
    return cfg.get("listen", True) is not False


# ---------------------------------------------------------------------------
# pairing lock: the daemon stops polling while `qur connect` pairs
# ---------------------------------------------------------------------------


def _pairing_marker(path: Path | None = None) -> Path:
    path = path or default_path()
    return path.with_name(path.name + ".pairing")


@contextlib.contextmanager
def pairing_lock(path: Path | None = None) -> Iterator[None]:
    """Hold while `pair` runs. Only one getUpdates consumer per bot token is
    allowed, so a daemon that is polling for button presses must step aside."""
    marker = _pairing_marker(path)
    marker.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    marker.touch()
    try:
        yield
    finally:
        try:
            marker.unlink()
        except FileNotFoundError:
            pass


def pairing_active(path: Path | None = None) -> bool:
    try:
        age = time.time() - _pairing_marker(path).stat().st_mtime
    except OSError:
        return False
    return age < PAIR_TIMEOUT_SECS + 30  # stale marker (crashed pairing) expires


# ---------------------------------------------------------------------------
# Requeue button
# ---------------------------------------------------------------------------


def _button(cfg: dict[str, Any], job_id: int, action: str, text: str) -> dict[str, str]:
    return {"text": text, "callback_data": f"{action}:{cfg['qur_id']}:{job_id}"}


def details_markup(cfg: dict[str, Any], job_id: int) -> dict[str, Any]:
    """What stays under a message once Requeue is spent: Details and Logs."""
    return {"inline_keyboard": [[_button(cfg, job_id, "sh", "ℹ️ Details"), _button(cfg, job_id, "lg", "📄 Logs")]]}


def job_markup(cfg: dict[str, Any], job_id: int) -> dict[str, Any]:
    """Buttons under a finished-job message: Requeue, Details, Logs."""
    return {"inline_keyboard": [
        [_button(cfg, job_id, "rq", "🔁 Requeue"), _button(cfg, job_id, "sh", "ℹ️ Details")],
        [_button(cfg, job_id, "lg", "📄 Logs")],
    ]}


def list_picker_markup(cfg: dict[str, Any], channels: list[int]) -> dict[str, Any]:
    """One button per existing channel, 4 per row; a press shows that channel's list."""
    btns = [{"text": f"ch {c}", "callback_data": f"lc:{cfg['qur_id']}:{c}"} for c in channels]
    return {"inline_keyboard": [btns[i:i + 4] for i in range(0, len(btns), 4)]}


def list_markup(cfg: dict[str, Any], chan: dict[str, Any]) -> dict[str, Any] | None:
    """Buttons under a channel list, one row per job in the same order as the
    text, every job with Details: running/frozen -> Logs + Cancel, queued ->
    Cancel, finished -> Logs + Requeue (None if the channel has no jobs)."""
    def btn(job: dict[str, Any], action: str, icon: str) -> dict[str, str]:
        return _button(cfg, job["id"], action, f"{icon} #{job['id']}")

    rows = [[btn(j, "sh", "ℹ️"), btn(j, "lg", "📄"), btn(j, "cn", "❌")] for j in (chan.get("frozen"), chan.get("running")) if j]
    queued = ((chan.get("priority_queue") or []) + chan["queue"])[:LIST_MAX_QUEUE_BUTTONS]
    rows += [[btn(j, "sh", "ℹ️"), btn(j, "cn", "❌")] for j in queued]
    rows += [[btn(j, "sh", "ℹ️"), btn(j, "lg", "📄"), btn(j, "rl", "🔁")] for j in chan["backlog"][:LIST_MAX_BACKLOG]]
    return {"inline_keyboard": rows} if rows else None


def without_button(markup: dict[str, Any] | None, data: str) -> dict[str, Any]:
    """`markup` minus the button whose callback_data is `data` (empty rows dropped)."""
    rows = [[b for b in row if b.get("callback_data") != data] for row in (markup or {}).get("inline_keyboard", [])]
    return {"inline_keyboard": [r for r in rows if r]}


def menu_markup() -> dict[str, Any]:
    """Persistent keyboard at the bottom of the chat (not tied to a job).
    Telegram keeps it until another reply keyboard replaces it; a press is
    sent as a plain message with the key's text."""
    return {"keyboard": [[{"text": MENU_STATUS}, {"text": MENU_LIST}]], "resize_keyboard": True, "is_persistent": True}


def parse_menu(text: Any) -> str | None:
    """"status", "list" or "menu" for a menu key / slash command, else None."""
    return MENU_ACTIONS.get(text.strip()) if isinstance(text, str) else None


def parse_callback(data: Any) -> tuple[str, str, int] | None:
    """(action, qur_id, job_id) from a button's callback_data, else None."""
    if not isinstance(data, str):
        return None
    parts = data.split(":")
    if len(parts) != 3 or parts[0] not in CALLBACK_ACTIONS or not parts[2].isdecimal():
        return None
    return parts[0], parts[1], int(parts[2])


def get_updates(cfg: dict[str, Any], offset: int | None, wait: int = POLL_SECS) -> list[dict[str, Any]]:
    """One long-poll (`wait` seconds; 0 = return at once) for button presses
    (inline buttons and menu keys). Raises NotifyError (409 = another process
    is polling this bot)."""
    params: dict[str, Any] = {"timeout": wait, "allowed_updates": ["callback_query", "message"]}
    if offset is not None:
        params["offset"] = offset
    return _api(cfg["token"], "getUpdates", params, timeout=wait + 10) or []


def answer_callback(cfg: dict[str, Any], callback_id: str, text: str) -> None:
    """Toast shown on the phone. Best-effort."""
    try:
        params = {"callback_query_id": callback_id}
        if text:
            params["text"] = text[:200]
        _api(cfg["token"], "answerCallbackQuery", params)
    except NotifyError:
        pass


def edit_markup(cfg: dict[str, Any], chat_id: int, message_id: int, markup: dict[str, Any]) -> None:
    """Replace the keyboard under a message (e.g. drop Requeue after use so it
    can't be pressed twice). Best-effort."""
    try:
        _api(cfg["token"], "editMessageReplyMarkup", {"chat_id": chat_id, "message_id": message_id, "reply_markup": markup})
    except NotifyError:
        pass


# ---------------------------------------------------------------------------
# job messages
# ---------------------------------------------------------------------------

_ICONS = {"done": "✅", "failed": "❌", "cancelled": "🚫", "interrupted": "⏹️", "skipped": "⏭️"}


def _fmt_secs(secs: float) -> str:
    if secs < 60:
        return f"{secs:.0f}s"
    minutes, secs_i = divmod(int(secs), 60)
    if minutes < 60:
        return f"{minutes}m{secs_i}s"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h{minutes}m"


def _duration(job: dict[str, Any]) -> str | None:
    if not (job.get("started_at") and job.get("finished_at")):
        return None
    secs = (datetime.fromisoformat(job["finished_at"]) - datetime.fromisoformat(job["started_at"])).total_seconds()
    return _fmt_secs(max(0.0, secs))


def _reason(note: str | None) -> str | None:
    """Coarse reason only. Raw notes can carry paths (`failed to start: ...`),
    and message content is visible to Telegram."""
    if not note:
        return None
    # "interrupted by daemon restart" is the note of rows written before the `interrupted` status existed
    if re.fullmatch(r"dependency \d+ (failed|removed)", note):  # only a job id: safe to show
        return note
    for known in ("timed out", "daemon stopped", "interrupted by daemon restart", "failed to start", "cancelled while queued"):
        if note.startswith(known):
            return known
    return None


def _display_quote(word: str) -> str:
    """One argv word quoted for display, same rule as cli.py's `_display_quote`
    (copied: cli imports this module). Plain words stay bare; a word whose only
    special character is an embedded `'` gets "double quotes" instead of
    shlex's `'"'"'` soup; anything else falls back to shlex.quote."""
    if not word:
        return "''"
    if shlex.quote(word) == word:
        return word
    if not any(c in word for c in '"$`\\\n'):
        return f'"{word}"'
    return shlex.quote(word)


def _display_join(argv: list[str]) -> str:
    return " ".join(_display_quote(w) for w in argv)


def _command(job: dict[str, Any]) -> str | None:
    """The command as typed (raw, not the resolved absolute-path form),
    collapsed to one line and capped."""
    try:
        text = " ".join(_display_join(json.loads(job["raw_cmd"])).split())
    except (KeyError, TypeError, ValueError):
        return None
    if len(text) > CMD_MAX_CHARS:
        text = text[: CMD_MAX_CHARS - 1] + "…"
    return text or None


def format_job(job: dict[str, Any], cfg: dict[str, Any]) -> str:
    """Message for a finished job: status, id, duration, the command as
    typed, machine, channel, exit code. No cwd, resolved paths or raw
    notes -- `qur show <id>` on the machine has those."""
    status = job["status"]
    label = "interrupted (daemon stopped)" if status == "interrupted" else status
    head = f"{_ICONS.get(status, '•')} {label} · job {job['id']}"
    dur = _duration(job)
    if dur:
        head += f" · {dur}"
    lines = [head]
    cmd = _command(job)
    if cmd:
        lines.append(f"$ {cmd}")
    parts = [f"ch {job['channel']}"]
    if job.get("exit_code") is not None:
        parts.append(f"exit {job['exit_code']}")
    if job.get("max_retries"):
        parts.append(f"attempt {job['attempt'] + 1}/{job['max_retries'] + 1}")
    reason = _reason(job.get("note"))
    if reason and not (status == "interrupted" and reason == "daemon stopped"):  # already in the head
        parts.append(reason)
    lines.append(f"{_who(cfg)} · " + " · ".join(parts))
    return "\n".join(lines)


def format_paused(job: dict[str, Any], cfg: dict[str, Any]) -> str:
    """Message for a channel that `--on-fail pause` just paused."""
    return (
        f"⏸ channel {job['channel']} paused after job {job['id']} failed\n"
        f"{_who(cfg)} · resume with: qur resume --channel {job['channel']}"
    )


def _local(ts: str | None) -> str:
    if not ts:
        return "-"
    return datetime.fromisoformat(ts).astimezone().strftime("%Y-%m-%d %H:%M:%S")


def _in_future(ts: str | None) -> bool:
    """True if the ISO timestamp is a moment that has not come yet."""
    if not ts:
        return False
    when = datetime.fromisoformat(ts)
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    return when > datetime.now(timezone.utc)


def _cap(text: str, limit: int = DETAIL_FIELD_MAX_CHARS) -> str:
    return text if len(text) <= limit else text[: limit - 1] + "…"


def format_details(job: dict[str, Any], cfg: dict[str, Any]) -> str:
    """Everything `qur show` prints, as Telegram HTML (parse_mode="HTML"):
    bold labels, monospace commands/paths, all dynamic text escaped. Sent only
    to paired chats, on request (Details button). Works for any job, running
    or queued included. Commands are shown in full up to DETAIL_CMD_MAX_CHARS;
    that cap is halved until the message fits Telegram's 4096 characters."""
    cap = DETAIL_CMD_MAX_CHARS
    while True:
        text = _format_details(job, cfg, cap)
        if len(text) <= TG_MAX_CHARS or cap <= 50:
            return text
        cap //= 2


def _format_details(job: dict[str, Any], cfg: dict[str, Any], cmd_cap: int) -> str:
    esc = html.escape
    field_cap = min(cmd_cap, DETAIL_FIELD_MAX_CHARS)

    def row(label: str, value: str, code: bool = False) -> str:
        return f"<b>{label}</b> " + (f"<code>{esc(value)}</code>" if code else esc(value))

    def cmd(key: str) -> str:
        try:
            return _cap(" ".join(_display_join(json.loads(job[key])).split()), cmd_cap)
        except (KeyError, TypeError, ValueError):
            return "-"

    status = job["status"]
    dur = _duration(job)
    if dur is None and job.get("started_at"):
        dur = _fmt_secs(max(0.0, (datetime.now(datetime.fromisoformat(job["started_at"]).tzinfo)
                                  - datetime.fromisoformat(job["started_at"])).total_seconds()))
    origin, parent = job.get("origin"), job.get("parent_id")
    if origin == "retry":
        origin_s = f"retry of #{parent} ({job.get('attempt')}/{job.get('max_retries')})"
    elif origin == "requeue":
        origin_s = f"requeue of #{parent}"
    else:
        origin_s = "new"
    lines = [
        f"{_ICONS.get(status, '•')} <b>Job #{job['id']}</b> · {esc(status)}",
        esc(_who(cfg)),
        "",
        row("channel", str(job["channel"])),
        row("raw command", cmd("raw_cmd"), code=True),
        row("resolved command", cmd("resolved_cmd"), code=True),
        row("cwd", _cap(job.get("cwd") or "-", field_cap), code=True),
        row("enqueued", _local(job.get("enqueued_at"))),
        row("started", _local(job.get("started_at"))),
        row("finished", _local(job.get("finished_at"))),
        row("duration", dur or "-"),
        row("exit code", str(job["exit_code"]) if job.get("exit_code") is not None else "-"),
        row("timeout", f"{job['timeout_secs']}s" if job.get("timeout_secs") else "none"),
        row("origin", origin_s),
    ]
    if job.get("max_retries"):
        lines.append(row("auto-retries", f"{job['attempt']} of {job['max_retries']} used"))
    if job.get("retry_delay_secs"):
        wait = _fmt_secs(job["retry_delay_secs"])
        lines.append(row("retry delay", wait + (", doubling every attempt" if job.get("retry_backoff") else "")))
    if job.get("on_fail") == "pause":
        lines.append(row("on fail", "pause the channel"))
    if job.get("not_before"):
        lines.append(row("not before", _local(job["not_before"])))
    if job.get("depends_on"):
        lines.append(row("depends on", _id_list(job["depends_on"])))
    if job.get("after_any"):
        lines.append(row("after any", _id_list(job["after_any"])))
    if job.get("waiting_on"):
        lines.append(row("waiting on", _id_list(job["waiting_on"])))
    env_lines = jobenv.describe(job.get("env_extra"))
    if env_lines:
        lines.append(row("environment", _cap(", ".join(env_lines), field_cap), code=True))
    lines.append(row("pid", str(job["pid"]) if job.get("pid") else "-"))
    lines.append(row("log", _cap(job.get("log_path") or "-", field_cap), code=True))
    if job.get("note"):
        lines.append(row("note", _cap(job["note"], field_cap)))
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# status / list / logs messages (Telegram HTML)
# ---------------------------------------------------------------------------


def _short_cmd(job: dict[str, Any], limit: int = LIST_CMD_MAX_CHARS) -> str:
    try:
        text = " ".join(_display_join(json.loads(job["raw_cmd"])).split())
    except (KeyError, TypeError, ValueError):
        return "-"
    return text if len(text) <= limit else text[: limit - 1] + "…"


def _since(ts: str | None) -> str:
    if not ts:
        return "-"
    then = datetime.fromisoformat(ts)
    return _fmt_secs(max(0.0, (datetime.now(then.tzinfo) - then).total_seconds()))


def _fit(header: str, lines: list[str]) -> str:
    """Join `header` + whole lines up to Telegram's limit; never cuts inside
    an HTML tag."""
    out, size = [header], len(header)
    for i, line in enumerate(lines):
        if size + len(line) + 1 > TG_MAX_CHARS - 40:
            out.append(f"… ({len(lines) - i} more lines)")
            break
        out.append(line)
        size += len(line) + 1
    return "\n".join(out)


def _id_list(ids: list[int]) -> str:
    """Job ids for display: "12", "12, 14"."""
    return ", ".join(str(i) for i in ids)


def _job_line(job: dict[str, Any], icon: str, extra: str = "", limit: int = LIST_CMD_MAX_CHARS) -> str:
    return f"{icon} #{job['id']} <code>{html.escape(_short_cmd(job, limit))}</code>{extra}"


def format_status(data: dict[str, Any], cfg: dict[str, Any]) -> str:
    """`qur status` as Telegram HTML: running job per channel, paused and
    frozen flags."""
    lines: list[str] = []
    for channel in sorted(data["channels"], key=int):
        chan = data["channels"][channel]
        flags = " · paused" if chan.get("paused") else ""
        lines.append(f"<b>channel {channel}</b>{flags}")
        frozen, running = chan.get("frozen"), chan.get("running")
        if frozen:
            lines.append(_job_line(frozen, "🧊", " frozen (--now)"))
        if running:
            pri = " (priority)" if running.get("is_priority") else ""
            lines.append(_job_line(running, "▶️", f"{pri} · running {_since(running.get('started_at'))}"))
        elif not frozen:
            lines.append("💤 idle")
    return _fit(f"<b>Status</b> · {html.escape(_who(cfg))}", lines if lines else ["(nothing has ever been queued)"])


def format_list(data: dict[str, Any], cfg: dict[str, Any], channel: int) -> str:
    """`qur list` for ONE channel as Telegram HTML: running job, queue and the
    recent finished jobs. Commands are shown in full (up to LIST_CMD_MAX_CHARS)
    and shortened only as far as needed to keep every job in the message."""
    chan = data["channels"][channel]
    entries: list[tuple[dict[str, Any], str, str]] = []
    if chan.get("frozen"):
        entries.append((chan["frozen"], "🧊", " frozen"))
    if chan.get("running"):
        entries.append((chan["running"], "▶️", f" · {_since(chan['running'].get('started_at'))}"))
    for job in (chan.get("priority_queue") or []) + chan["queue"]:
        waiting = f" · waiting on {_id_list(job['waiting_on'])}" if job.get("waiting_on") else ""
        if _in_future(job.get("not_before")):
            waiting += f" · starts {_local(job['not_before'])}"
        entries.append((job, "⏳", f" · queued {_since(job.get('enqueued_at'))}{waiting}"))
    for job in chan["backlog"][:LIST_TEXT_MAX_BACKLOG]:
        dur = _duration(job)
        exit_s = f" · exit {job['exit_code']}" if job.get("exit_code") is not None else ""
        entries.append((job, _ICONS.get(job["status"], "•"), (f" · {dur}" if dur else "") + exit_s))
    limit = max(LIST_CMD_MIN_CHARS, min(LIST_CMD_MAX_CHARS, (TG_MAX_CHARS - 300) // max(1, len(entries)) - 50))
    lines = ["⏸ paused"] if chan.get("paused") else []
    lines += [_job_line(job, icon, extra, limit) for job, icon, extra in entries]
    return _fit(f"<b>List · channel {channel}</b> · {html.escape(_who(cfg))}", lines or ["(idle, nothing yet)"])


_ANSI_RE = re.compile(r"\x1b\[[0-9;?]*[ -/]*[@-~]")
_CTRL_RE = re.compile(r"[\x00-\x08\x0b-\x1f\x7f]")


def read_log_tail(log_path: Any, lines: int = LOG_TAIL_LINES) -> str | None:
    """Last `lines` lines of a log file as clean text (colour codes and control
    characters dropped, \\r treated as a newline), None if it can't be read."""
    if not log_path:
        return None
    try:
        with open(log_path, "rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            f.seek(max(0, size - LOG_READ_BYTES))
            data = f.read()
    except OSError:
        return None
    text = data.decode(errors="replace").replace("\r\n", "\n").replace("\r", "\n")
    text = _CTRL_RE.sub("", _ANSI_RE.sub("", text))
    parts = text.rstrip("\n").split("\n")
    if size > LOG_READ_BYTES:
        parts = parts[1:]  # the first line is cut in the middle
    return "\n".join(parts[-lines:])


def format_log(job: dict[str, Any], tail: str | None, cfg: dict[str, Any]) -> str:
    """Log tail for one job as Telegram HTML (<pre>). Escaping can grow the
    text, so whole lines are dropped from the front until the message fits."""
    head = f"📄 <b>Log · job #{job['id']}</b> · {html.escape(job['status'])}\n{html.escape(_who(cfg))}\n"
    if tail is None:
        return head + "(no log file)"
    tail = tail.rstrip()
    if not tail:
        return head + "(log is empty)"
    cut = ""
    while True:
        body = f"{cut}<pre>{html.escape(tail)}</pre>"
        if len(head) + len(body) <= TG_MAX_CHARS:
            return head + body
        tail = tail.split("\n", 1)[1] if "\n" in tail else tail[len(tail) // 10 + 1:]
        cut = "… (earlier output cut)\n"
