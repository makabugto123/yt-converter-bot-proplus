#!/usr/bin/env python3
"""
YouTube Converter Telegram Bot (full script) — modified to support:
- per-user rate limits (max pending jobs & cooldown)
- persistent queue stored in SQLite (queued_tasks table)

Defaults:
- MAX_PENDING_PER_USER = 2
- ENQUEUE_COOLDOWN_SECONDS = 60
"""
import os
import re
import sqlite3
import tempfile
import threading
import time as _time
import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timedelta, time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

# ---------------- Configuration ----------------
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
DB_PATH = os.getenv("YT_CONVERTER_DB", "converter.db")
TIMEZONE = "Asia/Manila"
DAILY_CREDITS_FREE = 1000
DAILY_CREDITS_PREMIUM = 5000
COST_MP3 = 5
COST_MP4 = 10
SEARCH_RESULTS = 10
MAX_UPLOAD_BYTES = 1_900_000_000  # ~1.9 GB
TMP_DIR = Path(os.getenv("YT_CONVERTER_TMP", Path.cwd() / "tmp_downloads"))
TMP_DIR.mkdir(parents=True, exist_ok=True)

# Admin ID (change to your admin Telegram user id)
ADMIN = 73245133

# Accept any single http(s) link (we treat playlists as single-video via noplaylist)
URL_RE = re.compile(r"https?://\S+", re.I)

# --- NEW: per-user rate limit & persistence settings ---
MAX_PENDING_PER_USER = int(os.getenv("MAX_PENDING_PER_USER", "2"))  # max pending jobs per user per chat
ENQUEUE_COOLDOWN_SECONDS = int(os.getenv("ENQUEUE_COOLDOWN_SECONDS", "60"))  # seconds between enqueues per user
REHYDRATE_PLACEHOLDERS_ON_START = True  # if True, bot will send new placeholders for persisted tasks at startup

# YouTube search uses ytsearch via yt-dlp
# ---------------- Utilities ----------------
try:
    from zoneinfo import ZoneInfo
    from zoneinfo._common import ZoneInfoNotFoundError  # type: ignore
except Exception:
    ZoneInfo = None  # type: ignore
    class ZoneInfoNotFoundError(Exception):
        pass

def tznow() -> datetime:
    if ZoneInfo:
        try:
            return datetime.now(ZoneInfo(TIMEZONE))
        except ZoneInfoNotFoundError:
            pass
    return datetime.now()

def ensure_ffmpeg_available() -> bool:
    from shutil import which
    return which("ffmpeg") is not None and which("ffprobe") is not None

# ---------------- Database ----------------
SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    username TEXT,
    is_premium INTEGER NOT NULL DEFAULT 0,
    credits INTEGER NOT NULL DEFAULT 0,
    mp3_count INTEGER NOT NULL DEFAULT 0,
    mp4_count INTEGER NOT NULL DEFAULT 0,
    last_reset_date TEXT
);

CREATE TABLE IF NOT EXISTS queued_tasks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    chat_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    username TEXT,
    mode TEXT NOT NULL, -- 'mp3' or 'mp4'
    url TEXT NOT NULL,
    placeholder_message_id INTEGER, -- Telegram message id for the placeholder (nullable)
    enqueued_at TEXT NOT NULL, -- ISO timestamp
    status TEXT NOT NULL DEFAULT 'queued' -- queued, processing, done, failed
);
"""

def db() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH, check_same_thread=False)
    con.row_factory = sqlite3.Row
    return con

def init_db() -> None:
    with closing(db()) as con:
        con.executescript(SCHEMA)
        con.commit()

# ---------------- Database helpers for queue persistence (NEW) ----------------
def add_db_task(chat_id: int, user_id: int, username: str, mode: str, url: str, placeholder_message_id: Optional[int]) -> int:
    with closing(db()) as con:
        cur = con.execute(
            "INSERT INTO queued_tasks (chat_id, user_id, username, mode, url, placeholder_message_id, enqueued_at, status) VALUES (?, ?, ?, ?, ?, ?, ?, 'queued')",
            (chat_id, user_id, username, mode, url, placeholder_message_id, tznow().isoformat())
        )
        con.commit()
        return cur.lastrowid

def update_db_task_status(task_id: int, status: str, placeholder_message_id: Optional[int] = None) -> None:
    with closing(db()) as con:
        if placeholder_message_id is not None:
            con.execute("UPDATE queued_tasks SET status=?, placeholder_message_id=? WHERE id=?", (status, placeholder_message_id, task_id))
        else:
            con.execute("UPDATE queued_tasks SET status=? WHERE id=?", (status, task_id))
        con.commit()

def get_pending_tasks_from_db() -> List[sqlite3.Row]:
    with closing(db()) as con:
        rows = con.execute("SELECT * FROM queued_tasks WHERE status IN ('queued','processing') ORDER BY id ASC").fetchall()
        return rows

def count_user_pending_in_chat(chat_id: int, user_id: int) -> int:
    with closing(db()) as con:
        row = con.execute("SELECT COUNT(1) AS c FROM queued_tasks WHERE chat_id=? AND user_id=? AND status IN ('queued','processing')", (chat_id, user_id)).fetchone()
        return int(row["c"]) if row else 0

def last_enqueue_time_for_user(chat_id: int, user_id: int) -> Optional[datetime]:
    with closing(db()) as con:
        row = con.execute("SELECT enqueued_at FROM queued_tasks WHERE chat_id=? AND user_id=? ORDER BY enqueued_at DESC LIMIT 1", (chat_id, user_id)).fetchone()
        if not row:
            return None
        try:
            return datetime.fromisoformat(row["enqueued_at"])
        except Exception:
            return None

def remove_db_task(task_id: int) -> None:
    with closing(db()) as con:
        con.execute("DELETE FROM queued_tasks WHERE id=?", (task_id,))
        con.commit()

# ---------------- User DB / credits and other helpers (unchanged) ----------------
def get_user(user_id: int) -> Optional[sqlite3.Row]:
    with closing(db()) as con:
        cur = con.execute("SELECT * FROM users WHERE user_id=?", (user_id,))
        return cur.fetchone()

def register_user(update: Update) -> Tuple[bool, int]:
    u = update.effective_user
    assert u is not None
    with closing(db()) as con:
        cur = con.execute("SELECT 1, credits FROM users WHERE user_id=?", (u.id,))
        row = cur.fetchone()
        if row:
            return False, int(row["credits"])
        con.execute(
            "INSERT INTO users (user_id, first_name, last_name, username, is_premium, credits, last_reset_date) VALUES (?, ?, ?, ?, 0, ?, ?)",
            (u.id, u.first_name or "", u.last_name or "", u.username or "", DAILY_CREDITS_FREE, tznow().date().isoformat()),
        )
        con.commit()
    return True, DAILY_CREDITS_FREE

def maybe_reset_user_credits(user_id: int) -> None:
    today = tznow().date().isoformat()
    with closing(db()) as con:
        row = con.execute("SELECT is_premium, last_reset_date FROM users WHERE user_id=?", (user_id,)).fetchone()
        if not row:
            return
        if row["last_reset_date"] != today:
            new_credits = DAILY_CREDITS_PREMIUM if int(row["is_premium"]) else DAILY_CREDITS_FREE
            con.execute("UPDATE users SET credits=?, last_reset_date=? WHERE user_id=?", (new_credits, today, user_id))
            con.commit()

def ensure_credits_for(user_id: int, cost: int) -> bool:
    with closing(db()) as con:
        row = con.execute("SELECT credits FROM users WHERE user_id=?", (user_id,)).fetchone()
        return bool(row and int(row["credits"]) >= cost)

def add_conversion_and_deduct(user_id: int, mode: str, cost: int) -> None:
    with closing(db()) as con:
        if mode == "mp3":
            con.execute("UPDATE users SET credits = credits - ?, mp3_count = mp3_count + 1 WHERE user_id=?", (cost, user_id))
        else:
            con.execute("UPDATE users SET credits = credits - ?, mp4_count = mp4_count + 1 WHERE user_id=?", (cost, user_id))
        con.commit()

def get_profile(user_id: int) -> Tuple[int, int, int, str]:
    with closing(db()) as con:
        row = con.execute("SELECT credits, mp3_count, mp4_count, is_premium FROM users WHERE user_id=?", (user_id,)).fetchone()
        if not row:
            return 0, 0, 0, "Free"
        status = "Admin" if user_id == ADMIN else ("Premium" if row["is_premium"] else "Free")
        return (int(row["credits"]), int(row["mp3_count"]), int(row["mp4_count"]), status)

# ---------------- yt-dlp helpers (unchanged) ----------------
@dataclass
class SearchItem:
    idx: int
    title: str
    duration: str
    url: str

def yt_search(query: str, limit: int = 10) -> List[SearchItem]:
    """Faster search using extract_flat"""
    import yt_dlp
    ydl_opts = {"quiet": True, "noplaylist": True, "extract_flat": True}
    out: List[SearchItem] = []
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(f"ytsearch{limit}:{query}", download=False)
        entries = info.get("entries", []) if info else []
        for i, e in enumerate(entries, start=1):
            title = e.get("title") or "Untitled"
            url = e.get("url") or e.get("webpage_url") or ""
            if url and not url.startswith("http"):
                url = f"https://www.youtube.com/watch?v={url}"
            out.append(SearchItem(i, title, "", url))
    return out

def format_list(items: List[SearchItem]) -> str:
    return "\n".join(f"{i.idx}. {i.title}" for i in items)

def _download_sync(url: str, mode: str, tempdir: Path, progress_cb: Callable[[float], None]) -> Tuple[Path, Dict[str, Any]]:
    """Download using yt-dlp in a blocking thread; reports progress via progress_cb(percent)."""
    import yt_dlp
    def hook(d: Dict[str, Any]):
        try:
            if d.get("status") == "downloading":
                total = d.get("total_bytes") or d.get("total_bytes_estimate") or 0
                dl = d.get("downloaded_bytes") or 0
                pct = (dl / total * 100.0) if total else 0.0
                progress_cb(pct)
            elif d.get("status") == "finished":
                progress_cb(100.0)
        except Exception:
            pass

    outtmpl = str(tempdir / "%(title).200B.%(ext)s")

    cookiefile = os.getenv("YT_DLP_COOKIES")
    common_opts = {
        "outtmpl": outtmpl,
        "progress_hooks": [hook],
        "quiet": True,
        "noplaylist": True,
        # speed tweaks
        "concurrent_fragment_downloads": 4,
        "http_chunk_size": 10 * 1024 * 1024,
        "retries": 3,
        "socket_timeout": 20,
        **({"cookiefile": cookiefile} if cookiefile else {}),
    }

    if mode == "mp3":
        ydl_opts = {
            **common_opts,
            "format": "bestaudio[abr<=192]/bestaudio",
            "postprocessors": [
                {"key": "FFmpegExtractAudio", "preferredcodec": "mp3", "preferredquality": "128"}
            ],
            "postprocessor_args": ["-threads", "2"],
        }
    else:
        # cap video to 720p to speed up and avoid giant files
        ydl_opts = {
            **common_opts,
            "format": "bestvideo[height<=720][ext=mp4]+bestaudio[ext=m4a]/best[height<=720][ext=mp4]/best[height<=720]",
            "merge_output_format": "mp4",
            "postprocessors": [{"key": "FFmpegVideoConvertor", "preferedformat": "mp4"}],
            "postprocessor_args": ["-threads", "2"],
        }

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(url, download=True)
        fn = ydl.prepare_filename(info)
        if mode == "mp3" and not fn.endswith(".mp3"):
            from re import sub
            fn = sub(r"\.[^.]+$", ".mp3", fn)
        if mode == "mp4" and not fn.endswith(".mp4"):
            from re import sub
            fn = sub(r"\.[^.]+$", ".mp4", fn)
        file_path = Path(fn)

    assert file_path.exists()
    return file_path, info

# ---------------- Queue machinery (NEW + persistent) ----------------

def ordinal(n: int) -> str:
    if 10 <= (n % 100) <= 20:
        suf = "th"
    else:
        suf = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return f"{n}{suf}"

@dataclass
class ConvertTask:
    id: Optional[int]  # DB id if persisted, else None
    chat_id: int
    user_id: int
    mode: str  # "mp3" or "mp4"
    url: str
    username: str
    placeholder_message_id: Optional[int] = None  # message id of the "Please wait" placeholder

class ChatQueueManager:
    """
    Per-chat queue manager with persistent tasks saved to the queued_tasks table.
    """
    def __init__(self, app: Application):
        self.app = app
        self.queues: Dict[int, asyncio.Queue] = {}
        self.workers: Dict[int, asyncio.Task] = {}
        self._lock = asyncio.Lock()

    async def ensure_queue(self, chat_id: int) -> asyncio.Queue:
        async with self._lock:
            if chat_id not in self.queues:
                q = asyncio.Queue()
                self.queues[chat_id] = q
                # create worker task
                self.workers[chat_id] = asyncio.create_task(self._worker(chat_id))
            return self.queues[chat_id]

    async def _worker(self, chat_id: int):
        q = self.queues[chat_id]
        app = self.app
        bot = app.bot
        try:
            while True:
                task: ConvertTask = await q.get()
                try:
                    # mark processing in DB
                    if task.id:
                        update_db_task_status(task.id, "processing")
                    # edit placeholder to "Starting conversion..."
                    if task.placeholder_message_id:
                        try:
                            await bot.edit_message_text(chat_id=task.chat_id, message_id=task.placeholder_message_id,
                                                        text=f"Starting conversion for {task.username} — {task.mode.upper()}...")
                        except Exception:
                            pass
                    # actually run conversion (uses shared run_task function)
                    await run_task(app, task)
                    # mark done
                    if task.id:
                        update_db_task_status(task.id, "done")
                        # optional: remove finished tasks from DB if you prefer
                        remove_db_task(task.id)
                except Exception as e:
                    # mark failed
                    if task.id:
                        update_db_task_status(task.id, "failed")
                    try:
                        await bot.send_message(chat_id=task.chat_id, text=f"Error processing conversion for {task.username}: {e}")
                    except Exception:
                        pass
                finally:
                    q.task_done()
        except asyncio.CancelledError:
            # worker cancelled on shutdown
            pass

    async def queue_size(self, chat_id: int) -> int:
        q = self.queues.get(chat_id)
        return q.qsize() if q else 0

    async def load_pending_tasks_into_queues(self) -> None:
        """
        Load pending tasks from DB on startup and enqueue them to the per-chat queues.
        New placeholders are created (since old placeholder message IDs might not exist).
        """
        rows = get_pending_tasks_from_db()
        for r in rows:
            try:
                chat_id = int(r["chat_id"])
                user_id = int(r["user_id"])
                username = r["username"] or ""
                mode = r["mode"]
                url = r["url"]
                db_id = int(r["id"])
                # create placeholder in chat if configured
                placeholder_id = None
                if REHYDRATE_PLACEHOLDERS_ON_START:
                    try:
                        sent = await self.app.bot.send_message(chat_id=chat_id, text=f"Please wait...\nPosition : calculating... (restored queue)")
                        placeholder_id = sent.message_id
                        # update placeholder_message_id in DB
                        update_db_task_status(db_id, "queued", placeholder_message_id=placeholder_id)
                    except Exception:
                        placeholder_id = None
                task = ConvertTask(id=db_id, chat_id=chat_id, user_id=user_id, mode=mode, url=url, username=username, placeholder_message_id=placeholder_id)
                q = await self.ensure_queue(chat_id)
                # compute position from queue size +1
                pos = q.qsize() + 1
                if placeholder_id:
                    try:
                        await self.app.bot.edit_message_text(chat_id=chat_id, message_id=placeholder_id, text=f"Please wait...\nPosition : {ordinal(pos)}")
                    except Exception:
                        pass
                await q.put(task)
            except Exception:
                pass

# Placeholder for manager instance (will be set in main)
queue_mgr: Optional[ChatQueueManager] = None

# ---------------- Bot commands (original, lightly adapted) ----------------
async def start(update: Update, _: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "Welcome!\nSend /register to get daily credits.\nUsage:\n/mp3 <title or YouTube URL>\n/mp4 <title or YouTube URL>\n/check — show your status"
    )

async def register_cmd(update: Update, _: ContextTypes.DEFAULT_TYPE) -> None:
    created, credits = register_user(update)
    if created:
        await update.message.reply_text(f"Registered! You have {credits} coins today.")
    else:
        await update.message.reply_text("You're already registered.")

# Admin: checkuser
async def checkuser_cmd(update: Update, _: ContextTypes.DEFAULT_TYPE) -> None:
    u = update.effective_user
    if u.id != ADMIN:
        await update.message.reply_text("You are not authorized to use this command.")
        return

    args = (update.message.text or "").split(maxsplit=1)
    where = ""
    params: Tuple[Any, ...] = tuple()
    if len(args) > 1 and args[1].startswith("@"):
        where = "WHERE lower(username)=lower(?)"
        params = (args[1].lstrip("@"),)

    with closing(db()) as con:
        rows = con.execute(
            f"""
            SELECT username, mp3_count, mp4_count, credits, is_premium
            FROM users
            {where}
            ORDER BY user_id
            LIMIT 200
            """,
            params,
        ).fetchall()

    if not rows:
        await update.message.reply_text("No registered users found.")
        return

    W_USER, W_MP3, W_MP4, W_COINS, W_STATUS = 18, 5, 5, 7, 8

    def cut(s: str, w: int) -> str:
        s = s or ""
        return s if len(s) <= w else (s[: max(0, w - 1)] + "…")

    header = f"{'USER':<{W_USER}} | {'MP3':>{W_MP3}} | {'MP4':>{W_MP4}} | {'COINS':>{W_COINS}} | {'STATUS':<{W_STATUS}}"
    sep = "-" * len(header)
    lines = [header, sep]

    for r in rows:
        uname = f"@{r['username']}" if r['username'] else "(no_username)"
        uname = cut(uname, W_USER)
        status = "Premium" if int(r['is_premium']) else "Free"
        lines.append(f"{uname:<{W_USER}} | {int(r['mp3_count']):>{W_MP3}} | {int(r['mp4_count']):>{W_MP4}} | {int(r['credits']):>{W_COINS}} | {status:<{W_STATUS}}")

    text = "\n".join(lines)
    await update.message.reply_text(f"<pre>{text}</pre>", parse_mode="HTML")

async def check_cmd(update: Update, _: ContextTypes.DEFAULT_TYPE) -> None:
    u = update.effective_user
    maybe_reset_user_credits(u.id)
    credits, mp3c, mp4c, status = get_profile(u.id)
    name = (u.first_name or "User").strip()
    await update.message.reply_text(f"Hi {name}\nCoins: {credits}\nMP3 Converted : {mp3c}\nMP4 Converted : {mp4c}\nStatus : {status}")

def parse_args(text: str) -> str:
    parts = text.split(maxsplit=1)
    return parts[1].strip() if len(parts) > 1 else ""

# ---------------- Conversion runner (refactored for queue; unchanged core) ----------------
async def run_task(app: Application, task: ConvertTask) -> None:
    """
    Performs the full conversion logic for a queued task. This is a refactor of your original
    process_link logic so it can run outside the request handler. Covers credits re-check and deduction.
    """
    bot = app.bot
    chat_id = task.chat_id
    user_id = task.user_id
    mode = task.mode
    url = task.url
    username = task.username
    cost = COST_MP3 if mode == "mp3" else COST_MP4

    # Preflight: check user's credits at start of processing
    maybe_reset_user_credits(user_id)
    if not ensure_credits_for(user_id, cost):
        await bot.send_message(chat_id=chat_id, text=f"@{username} — Not enough coins to perform {mode.upper()} (cost {cost}). Job cancelled.")
        return

    # Gather meta
    import yt_dlp
    meta_opts = {"quiet": True, "noplaylist": True}
    cookiefile = os.getenv("YT_DLP_COOKIES")
    if cookiefile:
        meta_opts["cookiefile"] = cookiefile

    try:
        with yt_dlp.YoutubeDL(meta_opts) as ydl:
            meta = ydl.extract_info(url, download=False)
    except Exception as e:
        await bot.send_message(chat_id=chat_id, text=f"Failed to fetch metadata for {username}: {e}")
        return

    title = meta.get("title") or "Untitled"
    dur = int(meta.get("duration") or 0)
    m, s = divmod(dur, 60)
    duration = f"{m}:{s:02d}"

    # Duration policy: disallow >= 3 hours (10800s)
    MAX_SECONDS = 3 * 60 * 60
    if dur >= MAX_SECONDS:
        await bot.send_message(chat_id=chat_id, text=f"Sorry @{username}, this media is too long (≥ 3 hours). Maximum supported length is 2:59:59.")
        return

    link = meta.get("webpage_url") or url

    if mode == "mp3":
        resolution_text = "192 kbps"
        header = "Converting Music"
    else:
        h = meta.get("height"); w = meta.get("width")
        resolution_text = f"{w}x{h}" if w and h else "best available"
        header = "Converting Video"

    if not ensure_ffmpeg_available():
        await bot.send_message(chat_id=chat_id, text="FFmpeg/ffprobe not found on PATH.")
        return

    def _status_text(pct: float) -> str:
        blocks = max(0, min(5, int(pct // 20)))
        bar = "■" * blocks + "▢" * (5 - blocks)
        return (f"{header}\nTitle: {title}\nDuration: {duration}\nResolution: {resolution_text}\nType: {mode.upper()}\nLink: {link}\nStatus: {bar} ({int(pct)}%)")

    # If we have a placeholder message, edit it to the initial status; otherwise send a new one.
    status_msg_id = None
    try:
        if task.placeholder_message_id:
            try:
                await bot.edit_message_text(chat_id=chat_id, message_id=task.placeholder_message_id,
                                            text=_status_text(2))
                status_msg_id = task.placeholder_message_id
            except Exception:
                status_msg = await bot.send_message(chat_id=chat_id, text=_status_text(2))
                status_msg_id = status_msg.message_id
        else:
            status_msg = await bot.send_message(chat_id=chat_id, text=_status_text(2))
            status_msg_id = status_msg.message_id
    except Exception:
        # fallback: just send a simple message
        status_msg = await bot.send_message(chat_id=chat_id, text=f"{header}\nStarting...")
        status_msg_id = status_msg.message_id

    loop = asyncio.get_running_loop()
    progress_q: asyncio.Queue[float] = asyncio.Queue()

    def progress_cb(p: float) -> None:
        try:
            p = max(0.0, min(100.0, float(p)))
            loop.call_soon_threadsafe(progress_q.put_nowait, p)
        except Exception:
            pass

    job_dir = Path(tempfile.mkdtemp(prefix="yt_", dir=TMP_DIR))

    # Perform download in ThreadPool (like before) and concurrently update progress_msg
    with ThreadPoolExecutor(max_workers=1) as pool:
        fut = loop.run_in_executor(pool, _download_sync, url, mode, job_dir, progress_cb)

        async def editor():
            last_bucket = -1
            try:
                while True:
                    pct = float(await progress_q.get())
                    bucket = int(pct // 5)
                    if bucket != last_bucket:
                        last_bucket = bucket
                        try:
                            await bot.edit_message_text(chat_id=chat_id, message_id=status_msg_id, text=_status_text(pct))
                        except Exception:
                            pass
            except asyncio.CancelledError:
                return

        editor_task = asyncio.create_task(editor())
        try:
            file_path, _info = await fut
        finally:
            editor_task.cancel()
            try:
                await editor_task
            except Exception:
                pass

    # remove the status message
    try:
        await bot.delete_message(chat_id=chat_id, message_id=status_msg_id)
    except Exception:
        pass

    # Send file (respecting MAX_UPLOAD_BYTES)
    try:
        size = file_path.stat().st_size
        if size > MAX_UPLOAD_BYTES:
            await bot.send_message(chat_id=chat_id, text=f"File too large for Telegram upload ({size} bytes).")
        else:
            # send to chat (group or private)
            if mode == "mp3":
                with file_path.open("rb") as fh:
                    await bot.send_audio(chat_id=chat_id, audio=fh, title=file_path.stem, caption=file_path.stem)
            else:
                with file_path.open("rb") as fh:
                    await bot.send_video(chat_id=chat_id, video=fh, caption=file_path.stem, supports_streaming=True)
            # Deduct cost + increment counters
            add_conversion_and_deduct(user_id, mode, cost)
            # Inform chat that conversion finished (brief)
            try:
                await bot.send_message(chat_id=chat_id, text=f"Finished conversion for @{username} ({mode.upper()}).")
            except Exception:
                pass
    except Exception as e:
        try:
            await bot.send_message(chat_id=chat_id, text=f"Failed to send converted file for @{username}: {e}")
        except Exception:
            pass
    finally:
        # cleanup
        try:
            for p in job_dir.glob("*"):
                try:
                    p.unlink()
                except Exception:
                    pass
            job_dir.rmdir()
        except Exception:
            pass

# ---------------- Enqueue helpers (NEW + checks) ----------------
async def enqueue_conversion(update: Update, context: ContextTypes.DEFAULT_TYPE, mode: str) -> None:
    """
    Called by handlers to enqueue conversion requests per-chat. Replies immediately with
    "Please wait...\nPosition : X" placeholder. Enforces per-user pending & cooldown limits.
    """
    global queue_mgr
    if queue_mgr is None:
        # Shouldn't happen if main() initialized correctly
        queue_mgr = ChatQueueManager(context.application)

    u = update.effective_user
    chat = update.effective_chat
    chat_id = chat.id
    user_name = (u.username or u.full_name or "unknown").lstrip("@")
    args = parse_args(update.message.text or "")

    if not args:
        await update.message.reply_text(f"Usage: /{mode} <title or YouTube URL>")
        return

    cost = COST_MP3 if mode == "mp3" else COST_MP4
    maybe_reset_user_credits(u.id)
    if not ensure_credits_for(u.id, cost):
        await update.message.reply_text(f"Not enough coins. {mode.upper()} costs {cost}. Use /check.")
        return

    # Enforce per-user pending limit and cooldown
    current_pending = count_user_pending_in_chat(chat_id, u.id)
    if current_pending >= MAX_PENDING_PER_USER:
        await update.message.reply_text(f"You already have {current_pending} pending job(s) in this chat. Limit per user is {MAX_PENDING_PER_USER}.")
        return

    last_ts = last_enqueue_time_for_user(chat_id, u.id)
    if last_ts:
        elapsed = (tznow() - last_ts).total_seconds()
        if elapsed < ENQUEUE_COOLDOWN_SECONDS:
            await update.message.reply_text(f"You're sending jobs too quickly — please wait {int(ENQUEUE_COOLDOWN_SECONDS - elapsed)}s before enqueueing another job.")
            return

    # If URL -> enqueue directly
    if URL_RE.search(args):
        url = args
        q = await queue_mgr.ensure_queue(chat_id)
        position = q.qsize() + 1
        placeholder = await update.effective_chat.send_message(f"Please wait...\nPosition : {ordinal(position)}")
        # persist to DB
        db_id = add_db_task(chat_id, u.id, user_name, mode, url, placeholder.message_id)
        task = ConvertTask(id=db_id, chat_id=chat_id, user_id=u.id, mode=mode, url=url, username=user_name, placeholder_message_id=placeholder.message_id)
        await q.put(task)
        # update placeholder text with exact ordinal
        try:
            await placeholder.edit_text(f"Please wait...\nPosition : {ordinal(position)}")
        except Exception:
            pass
        return

    # Title search path — same as original but enqueue on selection
    searching_msg = await update.effective_chat.send_message("Searching… Please wait…")
    items = await asyncio.to_thread(yt_search, args, SEARCH_RESULTS)
    if not items:
        try:
            await searching_msg.delete()
        except Exception:
            pass
        await update.message.reply_text("No results found.")
        return

    # delete user's command message (cleanup)
    try:
        await update.message.delete()
    except Exception:
        pass

    list_text = format_list(items)
    list_msg = await update.effective_chat.send_message(list_text)
    try:
        await searching_msg.delete()
    except Exception:
        pass
    tip_msg = await update.effective_chat.send_message("Reply with a number (1–10) to choose.", reply_to_message_id=list_msg.message_id)

    context.user_data["pending"] = {
        "mode": mode,
        "items": [i.__dict__ for i in items],
        "list_msg_id": list_msg.message_id,
        "tip_msg_id": tip_msg.message_id,
        "cost": cost,
    }

# numeric reply handler now enqueues the selected URL
async def numeric_reply_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    pend = context.user_data.get("pending")
    if not pend:
        return
    if not update.message.reply_to_message or (update.message.reply_to_message.message_id != pend.get("list_msg_id")):
        return

    txt = (update.message.text or "").strip()
    if not txt.isdigit():
        return
    idx = int(txt)
    items = pend["items"]
    if not (1 <= idx <= len(items)):
        return

    # prepare a faux command message for enqueue to reuse logic
    url = items[idx - 1]["url"]
    mode = pend["mode"]

    # cleanup message(s)
    for mid in (update.message.message_id, pend.get("list_msg_id"), pend.get("tip_msg_id")):
        try:
            await update.effective_chat.delete_message(mid)
        except Exception:
            pass

    context.user_data.pop("pending", None)

    # simulate the message text so enqueue logic works uniformly
    # we temporarily set update.message.text to "/mp3 <url>" or "/mp4 <url>"
    orig_text = update.message.text
    update.message.text = f"/{mode} {url}"
    try:
        await enqueue_conversion(update, context, mode=mode)
    finally:
        update.message.text = orig_text

# mp_cmd now only enqueues (and checks registration)
async def mp_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE, mode: str) -> None:
    u = update.effective_user
    if not get_user(u.id):
        await update.message.reply_text("Please /register first.")
        return
    maybe_reset_user_credits(u.id)

    await enqueue_conversion(update, context, mode)

# Admin: set premium
async def set_prem_cmd(update: Update, _: ContextTypes.DEFAULT_TYPE) -> None:
    u = update.effective_user
    if u.id != ADMIN:
        await update.message.reply_text("You are not authorized to use this command.")
        return

    args = (update.message.text or "").split(maxsplit=1)
    if len(args) < 2 or not args[1].startswith("@"):
        await update.message.reply_text("Usage: /setprem @username")
        return

    target = args[1].lstrip("@")
    with closing(db()) as con:
        row = con.execute("SELECT user_id FROM users WHERE lower(username)=lower(?)", (target,)).fetchone()
        if not row:
            await update.message.reply_text("User not found. Ask them to /register first.")
            return
        con.execute("UPDATE users SET is_premium=1, credits=?, last_reset_date=? WHERE user_id=?", (DAILY_CREDITS_PREMIUM, tznow().date().isoformat(), int(row["user_id"])))
        con.commit()
    await update.message.reply_text(f"@{target} is now Premium with {DAILY_CREDITS_PREMIUM} coins.")

# Housekeeping: sweeper
def sweep_tmp_dir() -> None:
    try:
        for path in TMP_DIR.glob("**/*"):
            try:
                if path.is_file():
                    path.unlink(missing_ok=True)
                elif path.is_dir() and not any(path.iterdir()):
                    path.rmdir()
            except Exception:
                pass
    except Exception:
        pass

def start_sweeper_background(interval_seconds: int = 600) -> None:
    def _loop():
        while True:
            sweep_tmp_dir()
            _time.sleep(interval_seconds)
    t = threading.Thread(target=_loop, daemon=True)
    t.start()

async def daily_reset_all(_: Any) -> None:
    # Ensure daily credits reset for all users (used by JobQueue repeating)
    try:
        with closing(db()) as con:
            today = tznow().date().isoformat()
            # Reset all users' credits based on premium flag
            con.execute("UPDATE users SET credits = CASE WHEN is_premium THEN ? ELSE ? END, last_reset_date = ?",
                        (DAILY_CREDITS_PREMIUM, DAILY_CREDITS_FREE, today))
            con.commit()
    except Exception:
        pass

async def setup_jobs(app: Application) -> None:
    init_db()
    jq = getattr(app, "job_queue", None)
    if jq is None:
        print("[INFO] JobQueue not available. Using background thread sweeper.")
        start_sweeper_background(600)
    else:
        now = tznow()
        try:
            midnight = datetime.combine(now.date(), time(0,0,0, tzinfo=getattr(now, "tzinfo", None)))
            if now >= midnight:
                midnight = midnight + timedelta(days=1)
        except Exception:
            midnight = now + timedelta(seconds=5)

        jq.run_repeating(daily_reset_all, interval=timedelta(days=1), first=midnight, name="daily_reset")
        jq.run_repeating(lambda _: sweep_tmp_dir(), interval=timedelta(minutes=10), first=timedelta(minutes=10), name="tmp_sweeper")

    # After DB initialized, rehydrate any pending tasks into queue_mgr
    global queue_mgr
    if queue_mgr:
        await queue_mgr.load_pending_tasks_into_queues()

# ---------------- main ----------------
def main() -> None:
    global queue_mgr
    if not BOT_TOKEN:
        raise SystemExit("Set TELEGRAM_BOT_TOKEN env var")

    app = Application.builder().token(BOT_TOKEN).concurrent_updates(True).build()

    # initialize queue manager with the application instance
    queue_mgr = ChatQueueManager(app)

    # Handlers
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("register", register_cmd))
    app.add_handler(CommandHandler("check", check_cmd))
    app.add_handler(CommandHandler("setprem", set_prem_cmd))
    # mp3/mp4 use lambdas to pass the mode but now call mp_cmd which will enqueue
    app.add_handler(CommandHandler("mp3", lambda u, c: mp_cmd(u, c, "mp3")))
    app.add_handler(CommandHandler("mp4", lambda u, c: mp_cmd(u, c, "mp4")))
    app.add_handler(CommandHandler("checkuser", checkuser_cmd))

    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, numeric_reply_handler))

    app.post_init = setup_jobs  # will be awaited internally by PTB

    print("Bot is running. Press Ctrl+C to stop.")
    app.run_polling()

if __name__ == "__main__":
    main()