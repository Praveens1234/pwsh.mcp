#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║          GODMODE MCP SERVER  v4.0  —  AI-NATIVE EDITION                    ║
║                                                                              ║
║  Designed exclusively for AI agent control.                                  ║
║  Philosophy:                                                                 ║
║   • AI is the operator. Full trust. No human-style confirmation gates.       ║
║   • Every response teaches the AI what to do next.                           ║
║   • Every failure is forensically rich — cause, trace, suggestions, retry.   ║
║   • AI has persistent memory (scratch) + permanent store (mem).              ║
║   • Context-window efficient: structured JSON, no prose noise.               ║
║   • Tools are composable: batch, chain, watch, diff, bulk.                  ║
║   • Environment is self-describing: AI can discover everything.              ║
║   • Hardened: global exception handler, disk guard, session GC, rate limiter.║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

__version__ = "4.0.0"
__codename__ = "AI-Native"

# ── Conflict guard ─────────────────────────────────────────────────────────────
import os, sys
if os.path.basename(__file__).lower() in ("mcp.py", "mcp.pyw"):
    print("FATAL: Rename this file. 'mcp.py' conflicts with the mcp library.")
    sys.exit(1)

import asyncio, functools, hashlib, hmac, json, logging, platform, re
import shutil, signal, socket, sqlite3, threading, time, traceback, uuid
from collections import deque
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import fnmatch
import stat as stat_mod
import difflib
import psutil, uvicorn
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.middleware.cors import CORSMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

try:
    from mcp.server.fastmcp import FastMCP
except ImportError:
    print("FATAL: pip install mcp"); sys.exit(1)

try:
    import aiofiles; HAS_AIOFILES = True
except ImportError:
    HAS_AIOFILES = False

# ══════════════════════════════════════════════════════════════════════════════
# PLATFORM
# ══════════════════════════════════════════════════════════════════════════════

IS_WINDOWS  = platform.system() == "Windows"
LINUX_SHELL = os.environ.get("LINUX_SHELL", "bash")

if IS_WINDOWS:
    # On Windows: prefer PowerShell 7 (pwsh), fall back to Windows PowerShell 5
    SHELL_CMD = "pwsh" if shutil.which("pwsh") else "powershell"
else:
    # On Linux/macOS: default to bash; PS only if explicitly available
    SHELL_CMD = "pwsh" if shutil.which("pwsh") else LINUX_SHELL

# ══════════════════════════════════════════════════════════════════════════════
# CONFIG  (env-driven, minimal — AI doesn't need YAML ceremony)
# ══════════════════════════════════════════════════════════════════════════════

CFG = {
    "host":         os.environ.get("HOST", "0.0.0.0"),
    "port":         int(os.environ.get("PORT", 8000)),
    "api_key":      os.environ.get("API_KEY", ""),
    "require_auth": os.environ.get("REQUIRE_AUTH", "false").lower() == "true",
    "allowed_ips":  [x.strip() for x in os.environ.get("ALLOWED_IPS", "").split(",") if x.strip()],
    "data_dir":     os.environ.get("DATA_DIR", ""),
    "tls_cert":     os.environ.get("TLS_CERT", ""),
    "tls_key":      os.environ.get("TLS_KEY",  ""),
    "linux_shell":  LINUX_SHELL,
    # AI-native: no rate limiting by default — AI is trusted
    "rate_limit_enabled": os.environ.get("RATE_LIMIT", "false").lower() == "true",
    "max_jobs":     int(os.environ.get("MAX_JOBS", 200)),
    "job_ttl_h":    int(os.environ.get("JOB_TTL_H", 48)),
    "ledger_max":   int(os.environ.get("LEDGER_MAX", 2000)),
    "scratch_max":  int(os.environ.get("SCRATCH_MAX", 500)),
}

DATA_DIR    = Path(CFG["data_dir"] or Path(__file__).parent).resolve()
JOB_DIR     = DATA_DIR / "jobs"
SCRATCH_DIR = DATA_DIR / "scratch"
for d in (DATA_DIR, JOB_DIR, SCRATCH_DIR):
    d.mkdir(parents=True, exist_ok=True)

JOBS_FILE    = str(DATA_DIR / "jobs.json")
LEDGER_FILE  = str(DATA_DIR / "ledger.jsonl")
SCRATCH_FILE = str(DATA_DIR / "scratch.json")
LOG_FILE     = str(DATA_DIR / "server.log")
ERR_FILE     = str(DATA_DIR / "errors.jsonl")
MEM_FILE     = str(DATA_DIR / "memstore.db")    # permanent SQLite store

# ══════════════════════════════════════════════════════════════════════════════
# LOGGING
# ══════════════════════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        RotatingFileHandler(LOG_FILE, maxBytes=10*1024*1024, backupCount=3, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("GodMode")
START_TIME = time.time()

# ══════════════════════════════════════════════════════════════════════════════
# ─── AI-NATIVE RESPONSE BUILDER ──────────────────────────────────────────────
#
#  Every response is a JSON object with:
#   status       : "ok" | "error" | "partial"
#   data         : the actual result (AI reads this)
#   meta         : tool, duration_ms, ts, session_id context
#   ai_hint      : what AI should do next / key insights
#   on_error     : (only on error) cause, trace_id, suggestions[], retry_with
# ══════════════════════════════════════════════════════════════════════════════

def _ts() -> str:
    return datetime.utcnow().isoformat() + "Z"

def _dur(t0: float) -> float:
    return round((time.monotonic() - t0) * 1000, 2)

class R:
    @staticmethod
    def ok(data: Any, tool: str = "", t0: float = 0,
           hint: str = "", extra: Dict = None) -> str:
        obj: Dict[str, Any] = {
            "status": "ok",
            "data": data,
            "meta": {"tool": tool, "duration_ms": _dur(t0) if t0 else 0, "ts": _ts()},
        }
        if hint:
            obj["ai_hint"] = hint
        if extra:
            obj.update(extra)
        return json.dumps(obj, ensure_ascii=False, default=str)

    @staticmethod
    def error(message: str, tool: str = "", code: str = "ERROR",
              trace: str = "", suggestions: List[str] = None,
              retry_with: Dict = None, t0: float = 0,
              trace_id: str = "") -> str:
        tid = trace_id or uuid.uuid4().hex[:8]
        obj: Dict[str, Any] = {
            "status": "error",
            "error": {
                "code": code,
                "message": message,
                "trace_id": tid,
            },
            "meta": {"tool": tool, "duration_ms": _dur(t0) if t0 else 0, "ts": _ts()},
            "on_error": {
                "trace_id": tid,
                "suggestions": suggestions or [],
                "retry_with": retry_with or {},
            },
        }
        if trace:
            obj["error"]["stack"] = trace[-2000:]
        return json.dumps(obj, ensure_ascii=False, default=str)

    @staticmethod
    def partial(data: Any, tool: str = "", t0: float = 0,
                hint: str = "", truncated: bool = True) -> str:
        obj: Dict[str, Any] = {
            "status": "partial",
            "data": data,
            "truncated": truncated,
            "meta": {"tool": tool, "duration_ms": _dur(t0) if t0 else 0, "ts": _ts()},
        }
        if hint:
            obj["ai_hint"] = hint
        return json.dumps(obj, ensure_ascii=False, default=str)

# ══════════════════════════════════════════════════════════════════════════════
# ─── EXECUTION LEDGER  (AI's complete audit trail) ───────────────────────────
#  Every tool call is recorded. AI can query, filter, replay.
# ══════════════════════════════════════════════════════════════════════════════

class Ledger:
    """Append-only execution log, queryable by AI for forensics and investigation."""

    def __init__(self, path: str, max_entries: int = 2000):
        self.path        = path
        self.max_entries = max_entries
        self._lock: Optional[asyncio.Lock] = None
        self._mem: deque = deque(maxlen=max_entries)   # in-memory ring buffer
        self._load()

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    def _load(self):
        if os.path.exists(self.path):
            try:
                with open(self.path, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            try: self._mem.append(json.loads(line))
                            except: pass
            except Exception:
                pass

    async def record(self, tool: str, status: str, summary: str,
                     duration_ms: float = 0, trace_id: str = "",
                     tags: List[str] = None):
        entry = {
            "id":          uuid.uuid4().hex[:12],
            "ts":          _ts(),
            "tool":        tool,
            "status":      status,
            "summary":     summary[:300],
            "duration_ms": duration_ms,
            "trace_id":    trace_id or "",
            "tags":        tags or [],
        }
        self._mem.append(entry)
        async with self._get_lock():
            try:
                line = json.dumps(entry, ensure_ascii=False) + "\n"
                if HAS_AIOFILES:
                    async with aiofiles.open(self.path, "a", encoding="utf-8") as f:
                        await f.write(line)
                else:
                    def _w():
                        with open(self.path, "a", encoding="utf-8") as f:
                            f.write(line)
                    await asyncio.to_thread(_w)
            except Exception:
                pass

    def query(self, tool: str = "", status: str = "", tag: str = "",
              last_n: int = 50, search: str = "") -> List[Dict]:
        results = list(self._mem)
        if tool:   results = [e for e in results if e["tool"] == tool]
        if status: results = [e for e in results if e["status"] == status]
        if tag:    results = [e for e in results if tag in e.get("tags", [])]
        if search:
            lo = search.lower()
            results = [e for e in results if lo in e.get("summary", "").lower()]
        return results[-last_n:]

    def stats(self) -> Dict:
        entries = list(self._mem)
        by_tool: Dict[str, Dict] = {}
        for e in entries:
            t = e["tool"]
            if t not in by_tool:
                by_tool[t] = {"calls": 0, "ok": 0, "error": 0, "avg_ms": 0, "total_ms": 0}
            s = by_tool[t]
            s["calls"] += 1
            s["ok"]    += 1 if e["status"] == "ok" else 0
            s["error"] += 1 if e["status"] == "error" else 0
            s["total_ms"] += e.get("duration_ms", 0)
        for t, s in by_tool.items():
            s["avg_ms"] = round(s["total_ms"] / s["calls"], 1) if s["calls"] else 0
            del s["total_ms"]
        return {"total_entries": len(entries), "by_tool": by_tool}


ledger = Ledger(LEDGER_FILE, CFG["ledger_max"])

# ══════════════════════════════════════════════════════════════════════════════
# ─── AI SCRATCHPAD  (persistent key-value store for AI's own state) ──────────
# ══════════════════════════════════════════════════════════════════════════════

class Scratchpad:
    """
    AI Persistent Memory — Intelligent, Disciplined, Category-Aware.

    CATEGORY POLICY (enforced automatically by key prefix):
    ┌──────────────┬──────────────────┬─────────────────────────────────────────┐
    │ Prefix       │ Auto-TTL         │ Purpose                                 │
    ├──────────────┼──────────────────┼─────────────────────────────────────────┤
    │ session:     │ 2 hours          │ Active session IDs, per-session context  │
    │ cache:       │ 30 minutes       │ Computed values, query results, listings │
    │ tmp:         │ 10 minutes       │ Truly temporary, intermediate step data  │
    │ plan:        │ 12 hours         │ Task plans, step tracking, status        │
    │ job:         │ 4 hours          │ Background job IDs                       │
    │ retry:       │ 1 hour           │ Retry counters (auto-reset on success)   │
    │ result:      │ 24 hours         │ Task results, summaries                  │
    │ flag:        │ 6 hours          │ Boolean flags, signals                   │
    │ state:       │ 6 hours          │ Component state snapshots                │
    │ config:      │ none (permanent) │ User/system config that should persist   │
    │ perm:        │ none (permanent) │ Intentionally permanent data             │
    └──────────────┴──────────────────┴─────────────────────────────────────────┘

    Rules enforced:
    - Value size capped at 32KB per key (prevents context-window bombing)
    - Category key limits (prevent single namespace bloating)
    - Auto-TTL applied based on prefix even when ttl_minutes=0
    - Expired keys purged on every write and periodically
    - LRU eviction within category when limit hit
    """

    # Auto-TTL in minutes per prefix. 0 = permanent.
    CATEGORY_TTL: Dict[str, int] = {
        "session:": 120,
        "cache:":   30,
        "tmp:":     10,
        "plan:":    720,
        "job:":     240,
        "retry:":   60,
        "result:":  1440,
        "flag:":    360,
        "state:":   360,
        "config:":  0,
        "perm:":    0,
        "diff_baseline:": 120,
    }
    # Max keys per category prefix (None = no limit within global max)
    CATEGORY_LIMITS: Dict[str, int] = {
        "cache:":  50,
        "tmp:":    30,
        "retry:":  100,
        "session:": 20,
        "job:":    100,
    }
    VALUE_SIZE_CAP = 32 * 1024  # 32KB per value

    def __init__(self, path: str, max_keys: int = 500):
        self.path     = path
        self.max_keys = max_keys
        self._data: Dict[str, Any] = {}
        self._lock: Optional[asyncio.Lock] = None
        self._load()
        self._purge_expired_sync()   # clean on startup

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    def _get_auto_ttl(self, key: str) -> int:
        """Return auto-TTL minutes for key based on category prefix."""
        for prefix, ttl in self.CATEGORY_TTL.items():
            if key.startswith(prefix):
                return ttl
        return 0  # unknown prefix = permanent (AI chose deliberately)

    def _is_expired(self, entry: Dict) -> bool:
        exp = entry.get("expires_at")
        if not exp:
            return False
        try:
            return datetime.utcnow() > datetime.fromisoformat(exp.rstrip("Z"))
        except Exception:
            return False

    def _purge_expired_sync(self) -> int:
        """Synchronous expired-key purge — called on startup and periodically."""
        to_del = [k for k, v in self._data.items() if self._is_expired(v)]
        for k in to_del:
            del self._data[k]
        return len(to_del)

    async def _purge_expired(self) -> int:
        purged = self._purge_expired_sync()
        if purged:
            await self._flush_unlocked()
        return purged

    def _load(self):
        if os.path.exists(self.path):
            try:
                with open(self.path, "r", encoding="utf-8") as f:
                    self._data = json.load(f)
            except Exception:
                self._data = {}

    async def _flush_unlocked(self):
        """Write to disk — caller must hold lock or be in single-threaded context."""
        raw = json.dumps(self._data, ensure_ascii=False, indent=2, default=str)
        tmp = self.path + ".gmtmp"
        def _w():
            with open(tmp, "w", encoding="utf-8") as f: f.write(raw)
            os.replace(tmp, self.path)
        try:
            await asyncio.to_thread(_w)
        except Exception as e:
            logger.error(f"Scratchpad flush error: {e}")

    async def _flush(self):
        async with self._get_lock():
            await self._flush_unlocked()

    def _evict_category(self, prefix: str, keep: int):
        """LRU evict within a category to stay under keep limit."""
        cat_keys = sorted(
            [k for k in self._data if k.startswith(prefix)],
            key=lambda k: self._data[k].get("set_at", ""),
        )
        for k in cat_keys[:max(0, len(cat_keys) - keep + 1)]:
            del self._data[k]

    async def set(self, key: str, value: Any, ttl_minutes: int = 0) -> Dict:
        # Purge expired first
        self._purge_expired_sync()

        # Value size cap
        raw_val = json.dumps(value, default=str) if not isinstance(value, str) else value
        if len(raw_val.encode()) > self.VALUE_SIZE_CAP:
            # Truncate to cap and note it
            value = raw_val[:self.VALUE_SIZE_CAP] + "…[TRUNCATED:32KB_CAP]"

        # Determine TTL: explicit > auto-category > permanent
        auto_ttl = self._get_auto_ttl(key)
        effective_ttl = ttl_minutes if ttl_minutes > 0 else auto_ttl

        # Category limit enforcement
        for prefix, limit in self.CATEGORY_LIMITS.items():
            if key.startswith(prefix):
                cat_count = sum(1 for k in self._data if k.startswith(prefix) and k != key)
                if cat_count >= limit:
                    self._evict_category(prefix, limit)
                break

        # Global limit enforcement (LRU eviction of non-permanent keys)
        if len(self._data) >= self.max_keys and key not in self._data:
            # Prefer evicting tmp > cache > retry > session > others
            evict_order = ["tmp:", "cache:", "retry:", "session:", "flag:", "state:"]
            evicted = False
            for pfx in evict_order:
                victims = [k for k in self._data if k.startswith(pfx)]
                if victims:
                    oldest = min(victims, key=lambda k: self._data[k].get("set_at", ""))
                    del self._data[oldest]
                    evicted = True
                    break
            if not evicted:
                # Evict absolute oldest non-permanent
                non_perm = [k for k in self._data
                            if not (k.startswith("config:") or k.startswith("perm:"))]
                if non_perm:
                    oldest = min(non_perm, key=lambda k: self._data[k].get("set_at", ""))
                    del self._data[oldest]

        entry = {
            "value":      value,
            "set_at":     _ts(),
            "expires_at": (
                (datetime.utcnow() + timedelta(minutes=effective_ttl)).isoformat() + "Z"
                if effective_ttl > 0 else None
            ),
            "ttl_source": "explicit" if ttl_minutes > 0 else ("category" if auto_ttl > 0 else "permanent"),
            "ttl_minutes": effective_ttl or None,
        }
        self._data[key] = entry
        await self._flush()
        return {
            "key": key, "stored": True,
            "ttl_minutes": effective_ttl or None,
            "ttl_source":  entry["ttl_source"],
            "expires_at":  entry["expires_at"],
            "keys_total":  len(self._data),
        }

    def get(self, key: str) -> Tuple[bool, Any]:
        entry = self._data.get(key)
        if entry is None:
            return False, None
        if self._is_expired(entry):
            del self._data[key]
            # Schedule async flush — don't block sync get()
            try:
                loop = asyncio.get_running_loop()
                if loop.is_running():
                    loop.create_task(self._flush())
            except RuntimeError:
                pass
            return False, None
        return True, entry["value"]

    async def delete(self, key: str) -> bool:
        existed = key in self._data
        if existed:
            del self._data[key]
            await self._flush()
        return existed

    def list_keys(self, prefix: str = "", include_meta: bool = False) -> List[Any]:
        self._purge_expired_sync()
        keys = [k for k in self._data if k.startswith(prefix)] if prefix else list(self._data.keys())
        if include_meta:
            return [{
                "key":         k,
                "type":        type(self._data[k]["value"]).__name__,
                "set_at":      self._data[k].get("set_at"),
                "expires_at":  self._data[k].get("expires_at"),
                "ttl_source":  self._data[k].get("ttl_source", "unknown"),
                "ttl_minutes": self._data[k].get("ttl_minutes"),
                "size_chars":  len(json.dumps(self._data[k]["value"], default=str)),
            } for k in keys]
        return keys

    async def clear(self, prefix: str = "") -> int:
        if prefix:
            to_del = [k for k in self._data if k.startswith(prefix)]
        else:
            to_del = list(self._data.keys())
        for k in to_del:
            del self._data[k]
        if to_del:
            await self._flush()
        return len(to_del)

    def summary(self) -> Dict:
        """Compact category breakdown — AI can see memory usage without reading all values."""
        self._purge_expired_sync()
        categories: Dict[str, Dict] = {}
        unknown_count = 0
        total_size = 0
        for key, entry in self._data.items():
            cat = next((p for p in self.CATEGORY_TTL if key.startswith(p)), None)
            if not cat:
                unknown_count += 1; cat = "other:"
            if cat not in categories:
                categories[cat] = {"count": 0, "size_chars": 0, "permanent": 0}
            categories[cat]["count"] += 1
            sz = len(json.dumps(entry.get("value", ""), default=str))
            categories[cat]["size_chars"] += sz
            total_size += sz
            if not entry.get("expires_at"):
                categories[cat]["permanent"] += 1
        return {
            "total_keys":   len(self._data),
            "total_size_chars": total_size,
            "max_keys":     self.max_keys,
            "pct_full":     round(len(self._data) / self.max_keys * 100, 1),
            "categories":   categories,
        }


scratch = Scratchpad(SCRATCH_FILE, CFG["scratch_max"])

# ══════════════════════════════════════════════════════════════════════════════
# ─── MEMSTORE  — Permanent Structured Storage (SQLite-backed) ────────────────
#
#  Scratch    = working memory. Has TTL. Gets purged. Like RAM.
#  MemStore   = permanent knowledge base. No TTL. Survives forever. Like HDD.
#
#  Use MemStore for:
#    • User preferences, configs, credentials references
#    • Project knowledge: architecture, notes, decisions
#    • Task history and completed work summaries
#    • Any information that should survive across all sessions permanently
#    • Plans that outlast a single session
#    • Reference data: API endpoints, server configs, file maps
#
#  Structure:
#    namespace  — logical grouping ('projects', 'config', 'notes', 'tasks', ...)
#    key        — unique name within namespace
#    value      — any JSON value (string, dict, list, number)
#    tags       — comma-separated labels for cross-namespace search
#    notes      — human-readable description of what/why this record exists
# ══════════════════════════════════════════════════════════════════════════════

class MemStore:
    """
    Permanent SQLite-backed key-value store for AI agents.
    Zero TTL — data lives until explicitly deleted.
    Supports namespaces, tags, full-text search, and structured JSON values.
    Thread-safe via WAL mode + per-operation connections.
    """

    SCHEMA = """
    CREATE TABLE IF NOT EXISTS mem (
        namespace   TEXT    NOT NULL,
        key         TEXT    NOT NULL,
        value       TEXT    NOT NULL,          -- JSON-serialised
        value_type  TEXT    NOT NULL DEFAULT 'auto',
        tags        TEXT    NOT NULL DEFAULT '',  -- comma-separated
        notes       TEXT    NOT NULL DEFAULT '',
        created_at  TEXT    NOT NULL,
        updated_at  TEXT    NOT NULL,
        access_count INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (namespace, key)
    );
    CREATE INDEX IF NOT EXISTS idx_mem_tags      ON mem(tags);
    CREATE INDEX IF NOT EXISTS idx_mem_ns        ON mem(namespace);
    CREATE INDEX IF NOT EXISTS idx_mem_updated   ON mem(updated_at);
    PRAGMA journal_mode=WAL;
    PRAGMA synchronous=NORMAL;
    """

    def __init__(self, path: str):
        self.path = path
        self._lock = threading.Lock()
        self._init_db()

    def _conn(self) -> sqlite3.Connection:
        c = sqlite3.connect(self.path, timeout=10, check_same_thread=False)
        c.row_factory = sqlite3.Row
        c.execute("PRAGMA journal_mode=WAL")
        c.execute("PRAGMA synchronous=NORMAL")
        return c

    def _init_db(self):
        with self._lock:
            c = self._conn()
            c.executescript(self.SCHEMA)
            c.commit(); c.close()

    # ── sync helpers (called via asyncio.to_thread) ───────────────────────────

    def _set_sync(self, namespace: str, key: str, value: Any,
                  tags: str = "", notes: str = "") -> Dict:
        now = _ts()
        val_json = json.dumps(value, ensure_ascii=False, default=str)
        if len(val_json.encode()) > 512 * 1024:   # 512KB hard cap
            raise ValueError(f"Value too large: {len(val_json.encode())} bytes. Max 512KB.")
        val_type = type(value).__name__ if not isinstance(value, str) else "str"
        with self._lock:
            c = self._conn()
            existing = c.execute(
                "SELECT created_at FROM mem WHERE namespace=? AND key=?",
                (namespace, key)
            ).fetchone()
            if existing:
                c.execute(
                    "UPDATE mem SET value=?, value_type=?, tags=?, notes=?, updated_at=? "
                    "WHERE namespace=? AND key=?",
                    (val_json, val_type, tags, notes, now, namespace, key)
                )
                action = "updated"
            else:
                c.execute(
                    "INSERT INTO mem (namespace,key,value,value_type,tags,notes,created_at,updated_at) "
                    "VALUES (?,?,?,?,?,?,?,?)",
                    (namespace, key, val_json, val_type, tags, notes, now, now)
                )
                action = "created"
            c.commit(); c.close()
        return {"namespace": namespace, "key": key, "action": action,
                "tags": tags, "size_bytes": len(val_json.encode()), "updated_at": now}

    def _get_sync(self, namespace: str, key: str) -> Optional[Dict]:
        with self._lock:
            c = self._conn()
            row = c.execute(
                "SELECT * FROM mem WHERE namespace=? AND key=?", (namespace, key)
            ).fetchone()
            if not row:
                c.close(); return None
            c.execute(
                "UPDATE mem SET access_count=access_count+1 WHERE namespace=? AND key=?",
                (namespace, key)
            )
            c.commit(); c.close()
        return dict(row)

    def _delete_sync(self, namespace: str, key: str = "") -> int:
        with self._lock:
            c = self._conn()
            if key:
                cur = c.execute("DELETE FROM mem WHERE namespace=? AND key=?", (namespace, key))
            else:
                cur = c.execute("DELETE FROM mem WHERE namespace=?", (namespace,))
            deleted = cur.rowcount
            c.commit(); c.close()
        return deleted

    def _list_sync(self, namespace: str = "", key_prefix: str = "",
                   tags: str = "", limit: int = 200) -> List[Dict]:
        with self._lock:
            c = self._conn()
            conds, params = [], []
            if namespace:
                conds.append("namespace=?"); params.append(namespace)
            if key_prefix:
                conds.append("key LIKE ?"); params.append(key_prefix + "%")
            if tags:
                for tag in tags.split(","):
                    t = tag.strip()
                    if t:
                        conds.append("(',' || tags || ',' LIKE ?)"); params.append(f"%,{t},%")
            where  = f"WHERE {' AND '.join(conds)}" if conds else ""
            params.append(limit)
            rows = c.execute(
                f"SELECT namespace,key,value_type,tags,notes,created_at,updated_at,access_count "
                f"FROM mem {where} ORDER BY updated_at DESC LIMIT ?", params
            ).fetchall()
            c.close()
        return [dict(r) for r in rows]

    def _search_sync(self, query: str, namespace: str = "", limit: int = 50) -> List[Dict]:
        """Full-text search in keys, values, tags, notes."""
        q = f"%{query.lower()}%"
        with self._lock:
            c = self._conn()
            conds = ["(LOWER(key) LIKE ? OR LOWER(value) LIKE ? OR LOWER(tags) LIKE ? OR LOWER(notes) LIKE ?)"]
            params: list = [q, q, q, q]
            if namespace:
                conds.append("namespace=?"); params.append(namespace)
            params.append(limit)
            rows = c.execute(
                f"SELECT namespace,key,value_type,tags,notes,created_at,updated_at "
                f"FROM mem WHERE {' AND '.join(conds)} ORDER BY updated_at DESC LIMIT ?", params
            ).fetchall()
            c.close()
        return [dict(r) for r in rows]

    def _namespaces_sync(self) -> List[Dict]:
        with self._lock:
            c = self._conn()
            rows = c.execute(
                "SELECT namespace, COUNT(*) as count, MAX(updated_at) as last_updated "
                "FROM mem GROUP BY namespace ORDER BY last_updated DESC"
            ).fetchall()
            c.close()
        return [dict(r) for r in rows]

    def _stats_sync(self) -> Dict:
        with self._lock:
            c = self._conn()
            total   = c.execute("SELECT COUNT(*) FROM mem").fetchone()[0]
            ns_cnt  = c.execute("SELECT COUNT(DISTINCT namespace) FROM mem").fetchone()[0]
            db_size = os.path.getsize(self.path) if os.path.exists(self.path) else 0
            c.close()
        return {"total_records": total, "namespaces": ns_cnt,
                "db_size_bytes": db_size, "path": self.path}

    # ── async wrappers ────────────────────────────────────────────────────────

    async def set(self, namespace: str, key: str, value: Any,
                  tags: str = "", notes: str = "") -> Dict:
        return await asyncio.to_thread(self._set_sync, namespace, key, value, tags, notes)

    async def get(self, namespace: str, key: str) -> Optional[Dict]:
        return await asyncio.to_thread(self._get_sync, namespace, key)

    async def delete(self, namespace: str, key: str = "") -> int:
        return await asyncio.to_thread(self._delete_sync, namespace, key)

    async def list(self, namespace: str = "", key_prefix: str = "",
                   tags: str = "", limit: int = 200) -> List[Dict]:
        return await asyncio.to_thread(self._list_sync, namespace, key_prefix, tags, limit)

    async def search(self, query: str, namespace: str = "", limit: int = 50) -> List[Dict]:
        return await asyncio.to_thread(self._search_sync, query, namespace, limit)

    async def namespaces(self) -> List[Dict]:
        return await asyncio.to_thread(self._namespaces_sync)

    async def stats(self) -> Dict:
        return await asyncio.to_thread(self._stats_sync)


mem = MemStore(MEM_FILE)

# ══════════════════════════════════════════════════════════════════════════════
# ─── ERROR FORENSICS STORE ───────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

class ErrorStore:
    """Stores rich error records. AI can query to investigate failures."""
    def __init__(self, path: str, max_mem: int = 200):
        self.path = path
        self._mem: deque = deque(maxlen=max_mem)
        self._lock: Optional[asyncio.Lock] = None

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    async def record(self, tool: str, error: str, stack: str,
                     inputs: Dict = None, trace_id: str = ""):
        entry = {
            "trace_id":  trace_id or uuid.uuid4().hex[:8],
            "ts":        _ts(),
            "tool":      tool,
            "error":     error[:500],
            "stack":     stack[-3000:],
            "inputs":    {k: str(v)[:200] for k, v in (inputs or {}).items()},
        }
        self._mem.append(entry)
        async with self._get_lock():
            try:
                line = json.dumps(entry, ensure_ascii=False) + "\n"
                if HAS_AIOFILES:
                    async with aiofiles.open(self.path, "a", encoding="utf-8") as f:
                        await f.write(line)
                else:
                    await asyncio.to_thread(
                        lambda: open(self.path, "a", encoding="utf-8").write(line)
                    )
            except Exception:
                pass

    def recent(self, n: int = 20, tool: str = "") -> List[Dict]:
        entries = list(self._mem)
        if tool:
            entries = [e for e in entries if e["tool"] == tool]
        return entries[-n:]


err_store = ErrorStore(ERR_FILE)

# ══════════════════════════════════════════════════════════════════════════════
# ─── GLOBAL SHIELD  (AI-native: rich failure returns, no blocking) ────────────
# ══════════════════════════════════════════════════════════════════════════════

# Circuit breaker — still useful so AI knows a tool is broken
_CB: Dict[str, Dict] = {}
_CB_THRESHOLD = 8       # higher than human version — AI retries are intentional
_CB_COOLDOWN  = 30      # seconds — faster recovery
_cb_alock: Optional[asyncio.Lock] = None

def _get_cb_lock() -> asyncio.Lock:
    global _cb_alock
    if _cb_alock is None:
        _cb_alock = asyncio.Lock()
    return _cb_alock

async def _cb_open(name: str) -> bool:
    async with _get_cb_lock():
        s = _CB.get(name, {"f": 0, "t": 0.0, "open": False})
        if s["open"] and time.monotonic() - s["t"] >= _CB_COOLDOWN:
            s["open"] = False; s["f"] = 0; _CB[name] = s
        return s.get("open", False)

async def _cb_fail(name: str):
    async with _get_cb_lock():
        s = _CB.get(name, {"f": 0, "t": 0.0, "open": False})
        s["f"] += 1; s["t"] = time.monotonic()
        if s["f"] >= _CB_THRESHOLD:
            s["open"] = True
            logger.warning(f"Circuit OPEN: '{name}'")
        _CB[name] = s

async def _cb_ok(name: str):
    async with _get_cb_lock():
        if name in _CB:
            _CB[name]["f"] = 0; _CB[name]["open"] = False


def shield(func):
    """
    AI-native shield:
    - On success: records to ledger
    - On failure: full forensics (stack, trace_id, suggestions) returned AS DATA
      so AI can read, reason, and retry — not crash, not block
    - Circuit breaker: prevents tool hammering when consistently broken
    - Never throws — always returns a parseable JSON string
    """
    is_async = asyncio.iscoroutinefunction(func)

    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        name = func.__name__
        t0   = time.monotonic()
        trace_id = uuid.uuid4().hex[:8]

        if await _cb_open(name):
            msg = f"Tool '{name}' circuit is open (too many recent failures). Cooldown: {_CB_COOLDOWN}s."
            await ledger.record(name, "circuit_open", msg, _dur(t0), trace_id)
            return R.error(msg, tool=name, code="CIRCUIT_OPEN",
                           suggestions=[
                               f"Wait {_CB_COOLDOWN}s and retry.",
                               "Call ledger_query() to see recent failures for this tool.",
                               "Call error_inspect() to see full error details.",
                           ])

        try:
            result = await func(*args, **kwargs) if is_async else await asyncio.to_thread(func, *args, **kwargs)
            await _cb_ok(name)

            # Extract status from result for ledger
            try:
                parsed = json.loads(result) if isinstance(result, str) else result
                status = parsed.get("status", "ok")
                summary = str(parsed.get("data", ""))[:200]
            except Exception:
                status = "ok"; summary = str(result)[:200]

            await ledger.record(name, status, summary, _dur(t0), trace_id)
            return result

        except Exception as exc:
            await _cb_fail(name)
            tb  = traceback.format_exc()
            err = str(exc)

            # Record to error store with input context
            inputs = {}
            if args:   inputs["args"]   = str(args)[:300]
            if kwargs: inputs["kwargs"] = str(kwargs)[:300]
            await err_store.record(name, err, tb, inputs, trace_id)
            await ledger.record(name, "error", err[:200], _dur(t0), trace_id, tags=["exception"])

            logger.error(f"EXCEPTION [{trace_id}] in {name}: {err}")

            return R.error(
                message=err,
                tool=name,
                code="EXCEPTION",
                trace=tb,
                trace_id=trace_id,
                suggestions=[
                    f"Call error_inspect(trace_id='{trace_id}') for full stack.",
                    f"Call ledger_query(tool='{name}', status='error') for history.",
                    "Check inputs for type mismatches or missing values.",
                    "Retry with corrected parameters.",
                ],
                retry_with={"same_tool": name, "trace_id": trace_id},
            )
    return wrapper


# ══════════════════════════════════════════════════════════════════════════════
# ─── SHELL RUNNER (internal, used by many tools) ─────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

async def _run(cmd: str, shell: str = None, timeout: int = 120,
               env_extra: Dict = None, cwd: str = None) -> Dict:
    """
    Core execution engine. Returns structured dict:
      exit_code, stdout, stderr, truncated, duration_ms, cmd_used
    """
    sh  = shell or SHELL_CMD
    t0  = time.monotonic()
    env = os.environ.copy()
    if env_extra:
        env.update(env_extra)

    # Resolve working directory
    work_dir: Optional[str] = None
    if cwd and os.path.isdir(cwd):
        work_dir = cwd
    elif cwd:
        logger.warning(f"_run: cwd '{cwd}' not found, ignoring")

    # PowerShell wrapper — ensures UTF-8 output encoding
    if sh in ("pwsh", "powershell"):
        wrapped   = "$OutputEncoding = [Console]::OutputEncoding = [System.Text.Encoding]::UTF8; " + cmd
        exec_args = [sh, "-NonInteractive", "-Command", wrapped]
    else:
        exec_args = [sh, "-c", cmd]

    stdout_buf = bytearray()
    stderr_buf = bytearray()
    MAX        = 4 * 1024 * 1024  # 4MB per stream

    try:
        proc = await asyncio.create_subprocess_exec(
            *exec_args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
            cwd=work_dir,
        )

        async def _pump(stream, buf):
            while True:
                chunk = await stream.read(16384)
                if not chunk: break
                buf.extend(chunk)
                if len(buf) > MAX:
                    buf.extend(b"\n[TRUNCATED: exceeded 4MB]")
                    try: proc.kill()
                    except: pass
                    break

        try:
            await asyncio.wait_for(
                asyncio.gather(_pump(proc.stdout, stdout_buf), _pump(proc.stderr, stderr_buf)),
                timeout=float(timeout),
            )
            await proc.wait()
        except asyncio.TimeoutError:
            try: proc.kill()
            except: pass
            return {
                "exit_code": -1, "timed_out": True,
                "stdout": _clean(stdout_buf), "stderr": _clean(stderr_buf),
                "truncated": True, "duration_ms": _dur(t0), "cmd_used": sh,
                "ai_hint": f"Command timed out after {timeout}s. Use job_start() for long-running tasks.",
            }

        return {
            "exit_code":   proc.returncode,
            "stdout":      _clean(stdout_buf),
            "stderr":      _clean(stderr_buf) or None,
            "truncated":   b"TRUNCATED" in stdout_buf,
            "timed_out":   False,
            "duration_ms": _dur(t0),
            "cmd_used":    sh,
        }
    except FileNotFoundError:
        return {
            "exit_code": -127, "error": f"Shell '{sh}' not found in PATH.",
            "stdout": "", "stderr": "", "ai_hint":
            f"Install {sh} or set LINUX_SHELL env var to an available shell.",
        }


def _clean(buf: bytearray) -> str:
    txt = buf.decode("utf-8", errors="replace")
    return re.sub(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])", "", txt)


def _ps_arg(s: str) -> str:
    return s.replace("'", "''").replace("`", "``").replace("$", "`$")


def _validate_pid(s: str) -> Tuple[bool, Optional[int], str]:
    try:
        p = int(s)
        if p <= 0:  return False, None, "PID must be > 0"
        if p < 10:  return False, None, f"PID {p} is in system-reserved range (<10)"
        if p == os.getpid(): return False, None, "Cannot target the MCP server's own PID"
        return True, p, ""
    except (ValueError, TypeError):
        return False, None, f"'{s}' is not a valid integer PID"

# ══════════════════════════════════════════════════════════════════════════════
# ─── JOB MANAGER ─────────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

class Job:
    TERMINAL = {"completed", "failed", "stopped", "timed_out", "lost", "error"}

    def __init__(self, jid, cmd, pid=None, log="", status="running",
                 start=None, end=None, exit_code=None, label="", tags=None):
        self.id        = jid
        self.cmd       = cmd
        self.pid       = pid
        self.log       = log
        self.status    = status
        self.start     = start or _ts()
        self.end       = end
        self.exit_code = exit_code
        self.label     = label
        self.tags      = tags or []
        self.process   = None

    @property
    def is_terminal(self): return self.status in self.TERMINAL

    def to_dict(self):
        return dict(id=self.id, cmd=self.cmd[:500], pid=self.pid, log=self.log,
                    status=self.status, start=self.start, end=self.end,
                    exit_code=self.exit_code, label=self.label, tags=self.tags)

    @classmethod
    def from_dict(cls, d):
        j = cls(d["id"], d.get("cmd",""), d.get("pid"), d.get("log",""),
                d.get("status","unknown"), d.get("start"), d.get("end"),
                d.get("exit_code"), d.get("label",""), d.get("tags",[]))
        return j


class JobManager:
    def __init__(self):
        self.jobs: Dict[str, Job] = {}
        self._lock: Optional[asyncio.Lock] = None
        self._sem:  Optional[asyncio.Semaphore] = None
        self._tasks: Dict[str, asyncio.Task] = {}
        self._load()

    def _lk(self) -> asyncio.Lock:
        if self._lock is None: self._lock = asyncio.Lock()
        return self._lock

    def _sm(self) -> asyncio.Semaphore:
        if self._sem is None: self._sem = asyncio.Semaphore(50)  # AI runs many jobs
        return self._sem

    def _load(self):
        if not os.path.exists(JOBS_FILE): return
        try:
            with open(JOBS_FILE) as f:
                data = json.load(f)
            for d in data.values():
                j = Job.from_dict(d)
                if not j.is_terminal:
                    alive = j.pid and psutil.pid_exists(j.pid)
                    j.status = "running" if alive else "lost"
                    if not alive: j.end = _ts()
                self.jobs[j.id] = j
        except Exception as e:
            logger.error(f"Job load error: {e}")

    async def _save(self):
        async with self._lk():
            snap = {j.id: j.to_dict() for j in self.jobs.values()}
        raw = json.dumps(snap, ensure_ascii=False, indent=2, default=str)
        try:
            tmp = JOBS_FILE + ".tmp"
            if HAS_AIOFILES:
                async with aiofiles.open(tmp, "w", encoding="utf-8") as f:
                    await f.write(raw)
            else:
                await asyncio.to_thread(lambda: open(tmp, "w").write(raw))
            await asyncio.to_thread(os.replace, tmp, JOBS_FILE)
        except Exception as e:
            logger.error(f"Job save error: {e}")

    async def start(self, cmd: str, label: str = "", tags: List[str] = None,
                    timeout: int = 0, shell: str = "") -> "Job":
        async with self._sm():
            jid      = uuid.uuid4().hex[:10]
            log_path = str(JOB_DIR / f"{jid}.log")
            tags     = tags or []
            sh       = shell or SHELL_CMD

            fh = await asyncio.to_thread(lambda: open(log_path, "w", encoding="utf-8"))
            try:
                if sh in ("pwsh", "powershell"):
                    exec_args = [sh, "-NonInteractive", "-Command", cmd]
                else:
                    exec_args = [sh, "-c", cmd]

                proc = await asyncio.create_subprocess_exec(
                    *exec_args,
                    stdout=fh, stderr=asyncio.subprocess.STDOUT,
                    start_new_session=True,
                )
                job = Job(jid, cmd, proc.pid, log_path, label=label, tags=tags)
                job.process = proc
                async with self._lk():
                    self.jobs[jid] = job
                await self._save()

                task = asyncio.create_task(self._monitor(job, fh, timeout), name=f"jm-{jid}")
                self._tasks[jid] = task
                return job
            except Exception as e:
                try: await asyncio.to_thread(fh.close)
                except: pass
                raise e

    async def _monitor(self, job: Job, fh, timeout: int):
        try:
            if timeout > 0:
                try:
                    await asyncio.wait_for(job.process.wait(), float(timeout))
                except asyncio.TimeoutError:
                    try: job.process.kill()
                    except: pass
                    job.status = "timed_out"; job.exit_code = -1
            else:
                await job.process.wait()

            if job.status not in {"timed_out", "stopped"}:
                job.exit_code = job.process.returncode
                job.status    = "completed" if job.exit_code == 0 else "failed"
        except asyncio.CancelledError:
            job.status = "stopped"
        except Exception as e:
            job.status = "error"
            logger.error(f"Monitor error job {job.id}: {e}")
        finally:
            job.end = _ts()
            try: await asyncio.to_thread(fh.close)
            except: pass
            self._tasks.pop(job.id, None)
            await self._save()

    async def stop(self, jid: str) -> bool:
        async with self._lk():
            job = self.jobs.get(jid)
        if not job or job.is_terminal: return False
        if job.process:
            try:
                job.process.terminate()
                try: await asyncio.wait_for(job.process.wait(), 3.0)
                except asyncio.TimeoutError:
                    try: job.process.kill()
                    except: pass
            except ProcessLookupError: pass
        elif job.pid:
            try: os.kill(job.pid, signal.SIGTERM)
            except OSError: pass
        job.status = "stopped"; job.end = _ts()
        await self._save()
        return True

    async def wait_for(self, jid: str, timeout_s: int = 120) -> Optional[Job]:
        async with self._lk():
            job = self.jobs.get(jid)
        if not job: return None
        deadline = time.monotonic() + timeout_s
        while not job.is_terminal:
            if time.monotonic() >= deadline: return job
            await asyncio.sleep(0.15)
        return job

    def read_log(self, jid: str, tail: int = 0, head: int = 0,
                 grep: str = "") -> Tuple[str, bool]:
        """Returns (content, truncated)"""
        job = self.jobs.get(jid)
        if not job: return f"[Job '{jid}' not found]", False
        if not os.path.exists(job.log): return "[Log file missing]", False
        try:
            with open(job.log, "r", encoding="utf-8", errors="replace") as f:
                lines = f.readlines()
            if grep:
                lo = grep.lower()
                lines = [l for l in lines if lo in l.lower()]
            if head > 0: lines = lines[:head]
            if tail > 0: lines = lines[-tail:]
            content   = "".join(lines)
            truncated = len(content) > 200_000
            if truncated: content = content[:200_000] + "\n[TRUNCATED]"
            return content, truncated
        except Exception as e:
            return f"[Read error: {e}]", False

    async def purge(self, status: str = "", older_h: int = 0) -> int:
        cutoff = (datetime.utcnow() - timedelta(hours=older_h)) if older_h > 0 else None
        removed = []
        async with self._lk():
            for job in list(self.jobs.values()):
                if not job.is_terminal: continue
                if status and job.status != status: continue
                if cutoff and job.end:
                    try:
                        if datetime.fromisoformat(job.end.rstrip("Z")) >= cutoff: continue
                    except Exception: pass
                del self.jobs[job.id]
                removed.append(job)
        for j in removed:
            try: await asyncio.to_thread(os.remove, j.log)
            except: pass
        if removed: await self._save()
        return len(removed)

    def list(self, status: str = "", tag: str = "", last_n: int = 100) -> List[Dict]:
        jobs = list(self.jobs.values())
        if status: jobs = [j for j in jobs if j.status == status]
        if tag:    jobs = [j for j in jobs if tag in j.tags]
        jobs.sort(key=lambda j: j.start, reverse=True)
        return [j.to_dict() for j in jobs[:last_n]]


jm = JobManager()

# ══════════════════════════════════════════════════════════════════════════════
# ─── SESSION MANAGER ─────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

class PShellSession:
    def __init__(self, sid: str):
        self.id       = sid
        self.process  = None
        self.out_q:   Optional[asyncio.Queue] = None
        self.err_q:   Optional[asyncio.Queue] = None
        self._lock:   Optional[asyncio.Lock]  = None
        self.created  = _ts()
        self.cmd_count = 0

    @property
    def alive(self) -> bool:
        return self.process is not None and self.process.returncode is None

    async def start(self):
        self.out_q = asyncio.Queue()
        self.err_q = asyncio.Queue()
        self._lock = asyncio.Lock()
        env = os.environ.copy(); env["LANG"] = "en_US.UTF-8"
        self.process = await asyncio.create_subprocess_exec(
            SHELL_CMD, "-NoExit", "-Command", "-",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
        )
        asyncio.create_task(self._pump(self.process.stdout, self.out_q), name=f"so-{self.id}")
        asyncio.create_task(self._pump(self.process.stderr, self.err_q), name=f"se-{self.id}")
        await self._raw("$OutputEncoding = [Console]::OutputEncoding = [System.Text.Encoding]::UTF8")
        await asyncio.sleep(0.2)
        await self._drain()

    async def _pump(self, stream, q):
        try:
            while True:
                line = await stream.readline()
                if not line: break
                await q.put(line.decode("utf-8", errors="replace"))
        except Exception: pass

    async def _drain(self) -> str:
        out = []
        while not self.out_q.empty(): out.append(await self.out_q.get())
        while not self.err_q.empty(): out.append(f"[ERR]{await self.err_q.get()}")
        return "".join(out)

    async def _raw(self, cmd: str):
        if not self.alive: raise RuntimeError(f"Session {self.id} is dead.")
        self.process.stdin.write((cmd + "\n").encode("utf-8"))
        await self.process.stdin.drain()

    async def exec(self, cmd: str, timeout_ms: int = 8000) -> Tuple[str, bool]:
        """Atomic exec with sentinel detection. Returns (output, timed_out)."""
        async with self._lock:
            sentinel = f"__GMEND_{uuid.uuid4().hex[:8]}__"
            await self._raw(cmd)
            await self._raw(f"Write-Output '{sentinel}'")
            self.cmd_count += 1

            out = []; deadline = time.monotonic() + timeout_ms / 1000
            while time.monotonic() < deadline:
                await asyncio.sleep(0.03)
                while not self.out_q.empty():
                    line = await self.out_q.get()
                    if sentinel in line:
                        return _clean_str("".join(out)), False
                    out.append(line)
                while not self.err_q.empty():
                    out.append(f"[STDERR]{await self.err_q.get()}")
            return _clean_str("".join(out)) + "\n[WARN:sentinel_timeout]", True

    async def kill(self):
        if self.process:
            try:
                self.process.terminate()
                await asyncio.wait_for(self.process.wait(), 3.0)
            except Exception:
                try: self.process.kill()
                except: pass


def _clean_str(s: str) -> str:
    return re.sub(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])", "", s)


class SessionManager:
    def __init__(self):
        self.sessions: Dict[str, PShellSession] = {}
        self._lock: Optional[asyncio.Lock] = None

    def _lk(self):
        if self._lock is None: self._lock = asyncio.Lock()
        return self._lock

    async def create(self) -> PShellSession:
        sid = uuid.uuid4().hex[:10]
        s   = PShellSession(sid)
        await s.start()
        async with self._lk():
            self.sessions[sid] = s
        return s

    async def get(self, sid: str) -> Optional[PShellSession]:
        async with self._lk():
            return self.sessions.get(sid)

    async def kill(self, sid: str) -> bool:
        async with self._lk():
            s = self.sessions.pop(sid, None)
        if s: await s.kill(); return True
        return False

    def list_all(self) -> List[Dict]:
        return [{"id": s.id, "alive": s.alive, "created": s.created,
                 "cmd_count": s.cmd_count} for s in self.sessions.values()]


sm = SessionManager()

# ══════════════════════════════════════════════════════════════════════════════
# AUTH MIDDLEWARE
# ══════════════════════════════════════════════════════════════════════════════

class AuthMiddleware(BaseHTTPMiddleware):
    def __init__(self, app):
        super().__init__(app)
        self._key_hash = hashlib.sha256(CFG["api_key"].encode()).hexdigest() if CFG["api_key"] else ""
        self._ips      = set(CFG["allowed_ips"])

    async def dispatch(self, req: Request, call_next):
        if req.url.path in ("/health",): return await call_next(req)
        ip = req.client.host if req.client else "?"
        if self._ips and ip not in self._ips:
            return JSONResponse({"error": "IP not in allowlist", "ip": ip}, status_code=403)
        if CFG["api_key"] and CFG["require_auth"]:
            auth  = req.headers.get("Authorization", "")
            token = auth[7:] if auth.startswith("Bearer ") else req.headers.get("X-API-Key", "")
            if not hmac.compare_digest(
                hashlib.sha256(token.encode()).hexdigest(), self._key_hash
            ):
                return JSONResponse({"error": "Unauthorized. Use Bearer token or X-API-Key."}, status_code=401)
        return await call_next(req)

# ══════════════════════════════════════════════════════════════════════════════
# MCP
# ══════════════════════════════════════════════════════════════════════════════

mcp = FastMCP("GodMode-AI", host="0.0.0.0")

# ─────────────────────────────────────────────────────────────────────────────
#  GROUP 0 — AI ORIENTATION  (first calls AI should make)
# ─────────────────────────────────────────────────────────────────────────────

@mcp.tool()
def ai_init() -> str:
    """
    ★ FIRST CALL every session. Returns environment snapshot + memory state.
    Compact by design — gives orientation without flooding context window.
    """
    t0 = time.monotonic()
    mem  = psutil.virtual_memory()
    disk = psutil.disk_usage("/" if not IS_WINDOWS else "C:\\")
    proc = psutil.Process(os.getpid())

    load = getattr(os, "getloadavg", lambda: None)()
    if load is None:
        p    = psutil.cpu_percent(interval=0.1)
        load = (p, p, p)

    running_jobs  = [j.to_dict() for j in jm.jobs.values() if not j.is_terminal]
    recent_errors = err_store.recent(3)   # 3 not 5 — context economy
    scratch_smry  = scratch.summary()

    tool_map = {
        "orientation":   ["ai_init", "env_snapshot", "tool_guide", "probe"],
        "shell":         ["shell_exec", "shell_exec_linux", "batch_exec",
                          "chain_exec", "smart_exec", "script_run"],
        "jobs":          ["job_start", "job_list", "job_wait", "job_logs",
                          "job_stop", "job_cleanup"],
        "sessions":      ["session_create", "session_exec", "session_send",
                          "session_read", "session_list", "session_kill"],
        "filesystem":    ["fs_read", "fs_write", "fs_delete", "fs_list", "fs_info",
                          "fs_move", "fs_copy", "fs_search", "fs_tree", "fs_glob",
                          "fs_bulk_create", "fs_bulk_delete", "fs_bulk_move", "fs_bulk_copy"],
        "system":        ["sys_info", "process_list", "process_kill", "kill_by_name",
                          "service_ctrl", "env_get", "env_set", "log_read"],
        "network":       ["net_dns", "net_connect", "net_inspect", "http_request"],
        "ai_memory":     ["scratch_summary", "scratch_set", "scratch_get",
                          "scratch_list", "scratch_delete", "scratch_gc", "scratch_clear"],
        "permanent_store": ["mem_namespaces", "mem_set", "mem_get", "mem_update",
                            "mem_delete", "mem_list", "mem_search"],
        "data":          ["jsonl_append", "diff_exec"],
        "investigation": ["ledger_query", "ledger_stats", "error_inspect", "watch_until"],
        "server":        ["ping", "server_status"],
    }

    data = {
        "server": {
            "version": __version__, "codename": __codename__,
            "uptime_s": round(time.time() - START_TIME, 1),
            "data_dir": str(DATA_DIR), "pid": os.getpid(),
        },
        "platform": {
            "os":           platform.system(),
            "os_version":   platform.version()[:80],   # truncate long win version strings
            "hostname":     socket.gethostname(),
            "arch":         platform.machine(),
            "python":       platform.python_version(),
            "shell_ps":     SHELL_CMD,
            "shell_linux":  CFG["linux_shell"],
        },
        "resources": {
            "cpu_cores":         psutil.cpu_count(),
            "cpu_pct":           round(load[0], 1),
            "ram_total_gb":      round(mem.total / 1024**3, 2),
            "ram_free_gb":       round(mem.available / 1024**3, 2),
            "ram_pct":           mem.percent,
            "disk_free_gb":      round(disk.free / 1024**3, 2),
            "disk_pct":          disk.percent,
            "server_ram_mb":     round(proc.memory_info().rss / 1024**2, 1),
        },
        "state": {
            "running_jobs":      len(running_jobs),
            "running_jobs_list": running_jobs,
            "active_sessions":   len(sm.sessions),
            "scratch_summary":   scratch_smry,
            "mem_store":         mem._stats_sync(),  # sync OK here — not in hot path
        },
        "recent_errors":   recent_errors,
        "tool_map":        tool_map,
        "open_circuits":   {
            n: {"failures": s["f"]}
            for n, s in _CB.items() if s.get("open")
        },
        "startup_checklist": [
            "1. Read platform + resources above",
            "2. Check state.running_jobs_list — resume any interrupted jobs",
            "3. Check state.scratch_summary — working memory from last session",
            "4. Check state.mem_store — how much permanent knowledge is stored",
            "5. mem_namespaces() — see what namespaces/topics are in permanent store",
            "6. Check recent_errors — understand any prior failures",
            "7. Proceed with user task",
        ],
    }
    return R.ok(data, tool="ai_init", t0=t0,
                hint="Session ready. See startup_checklist. "
                     "scratch_summary() for memory details. tool_guide() for usage patterns.")


@mcp.tool()
def tool_guide() -> str:
    """
    Complete AI usage guide: philosophy, memory architecture, tool selection,
    patterns, discipline rules, anti-patterns. Read once per session.
    """
    guide = {
        "philosophy": [
            "Parse data field from every response. On error: read on_error.suggestions[].",
            "CONTEXT ECONOMY: prefer compact outputs. Use filter params, output_mode=minimal.",
            "PARALLEL FIRST: batch_exec() for 2+ independent commands. NEVER loop shell_exec().",
            "BACKGROUND JOBS: job_start() for anything >5s. NEVER block on long shell_exec().",
            "MEMORY TWO-LAYER: scratch=working RAM (TTL), mem=permanent HDD (forever).",
            "VERIFY BEFORE REPORT: confirm change happened — never assume success.",
            "ANTI-LOOP: retried 3x without change? STOP and report the blocker.",
        ],

        "memory_architecture": {
            "SCRATCH (working memory)": {
                "backend":  "JSON file, auto-loaded on start",
                "ttl":      "auto by prefix — see scratch_discipline below",
                "analogy":  "Agent RAM — fast, temporary, auto-purged by GC",
                "use_for":  "retry counters, job IDs, step status, current plan, session ids",
                "tools":    ["scratch_set", "scratch_get", "scratch_summary", "scratch_gc"],
            },
            "MEM (permanent store)": {
                "backend":  "SQLite WAL — survives restarts, never auto-deleted",
                "ttl":      "NONE — lives until mem_delete() is called",
                "analogy":  "Agent HDD — structured, permanent, searchable",
                "use_for":  "configs, project knowledge, decisions, completed work, reference data",
                "tools":    ["mem_set", "mem_get", "mem_search", "mem_namespaces", "mem_list"],
            },
            "DECISION_RULE": [
                "Needed across sessions? → mem_set(namespace, key, value)",
                "Temporary task state (job ID, retry count)? → scratch_set(prefix:key)",
                "Ongoing project plan? → mem_set('projects', 'plan_name', ...)",
                "Current session context? → scratch_set('plan:task_name', ...)",
                "User config or preference? → mem_set('config', 'key', value)",
            ],
        },

        "scratch_discipline": {
            "rule":     "Every scratch key MUST have correct category prefix.",
            "prefixes": {
                "session:": "2h   — active session IDs",
                "cache:":   "30m  — computed listings, query results",
                "tmp:":     "10m  — intermediate step data, throw-away",
                "plan:":    "12h  — task plans, step tracking",
                "job:":     "4h   — background job IDs",
                "retry:":   "1h   — retry counters",
                "result:":  "24h  — final task output/summary",
                "flag:":    "6h   — boolean signals",
                "state:":   "6h   — component state",
                "config:":  "perm — user/system config",
                "perm:":    "perm — intentional permanent data",
            },
            "lifecycle": [
                "START  → scratch_set('plan:task',  {steps:[...], status:'started'})",
                "JOB    → scratch_set('job:label',  job_id)  ← immediately after job_start()",
                "RETRY  → increment retry: counter; change strategy at count>=3",
                "DONE   → scratch_set('result:task', summary); scratch_gc()",
            ],
        },

        "mem_namespaces_guide": {
            "suggested_namespaces": {
                "config":    "Server configs, API keys references, user preferences",
                "projects":  "Architecture docs, design decisions, project plans",
                "tasks":     "Completed task summaries, phase notes",
                "notes":     "Freeform knowledge, observations, trading notes",
                "servers":   "Server IPs, endpoints, deployment info",
                "agents":    "Other agents info, capabilities, endpoints",
                "trading":   "Trading configs, strategies, edge notes",
            },
            "usage": [
                "mem_namespaces()          → see all namespaces + record counts",
                "mem_list('projects')      → all project records",
                "mem_get('config', 'key')  → retrieve specific config",
                "mem_search('gold')        → find all records mentioning gold",
                "mem_set('notes', 'key', value, tags='important,xauusd')",
            ],
        },

        "context_window_rules": {
            "heavy_tools": {
                "process_list":  "Use name_filter=, output_mode='minimal'. top_n<=20.",
                "env_get":       "ALWAYS use name= or filter_prefix=. NEVER bare env_get().",
                "net_inspect":   "Use scope='interfaces' or 'sockets', not 'all'.",
                "fs_list":       "Use max_depth=1. Use fs_tree() for recursive overview.",
                "job_logs":      "Use tail=100. Never tail=0 on large log.",
                "fs_search":     "Always set pattern AND max_results.",
            },
            "prefer_compact": [
                "scratch_summary()            over scratch_list() for orientation",
                "mem_namespaces()             over mem_list() for first look",
                "fs_tree(path, max_depth=3)   over fs_list(recursive=True)",
                "fs_glob('**/*.py')           over fs_search for filename patterns",
                "process_list(output_mode='minimal') over full list",
                "env_get(filter_prefix='APP_') over env_get() for all vars",
            ],
        },

        "tool_selection": {
            "permanent_storage":     "mem_set(ns, key, value) → persists forever",
            "find_stored_info":      "mem_search('query') → full-text all records",
            "knowledge_overview":    "mem_namespaces() + mem_list(ns)",
            "run_command":           "shell_exec (PS) | shell_exec_linux (bash)",
            "run_many_commands":     "batch_exec (parallel JSON array)",
            "run_pipeline":          "chain_exec (sequential, PREV_OUTPUT env var)",
            "run_unknown_duration":  "smart_exec (auto-routes: short=inline, long=background)",
            "run_background":        "job_start → scratch_set('job:label', id) → job_wait",
            "run_stateful_shell":    "session_create → session_exec (stateful cd/venv)",
            "find_files_by_name":    "fs_glob('**/*.ext')",
            "find_files_by_content": "fs_search(pattern, content_pattern)",
            "explore_structure":     "fs_tree(path, max_depth=3)",
            "create_many_files":     "fs_bulk_create (JSON array, one call)",
            "delete_many":           "fs_bulk_delete (JSON array, one call)",
            "move_many":             "fs_bulk_move ([{from, to}] array)",
            "http_call":             "http_request(url, method, body)",
            "wait_for_condition":    "watch_until(cmd, pattern)",
            "compare_before_after":  "diff_exec(cmd, baseline_key)",
            "kill_by_pid":           "process_kill(pid)",
            "kill_by_name":          "kill_by_name(pattern, dry_run=True first)",
            "structured_log":        "jsonl_append(path, record_json)",
        },

        "patterns": {
            "session_start": [
                "1. ai_init()          → environment + memory snapshot",
                "2. mem_namespaces()   → see what permanent knowledge exists",
                "3. scratch_summary()  → check working memory from last session",
                "4. tool_guide()       → refresh discipline rules (optional)",
            ],
            "save_project_knowledge": [
                "mem_set('projects', 'my_project_plan', plan_dict, tags='active,phase1')",
                "mem_set('config',   'server_endpoint', url, notes='Main API server')",
                "mem_set('notes',    'important_finding', text, tags='important,gold')",
            ],
            "get_installed_apps": [
                "1. scratch_get('cache:installed_apps') — check cache first",
                "   IF found: use cached. SKIP steps 2-3.",
                "2. shell_exec('Get-ItemProperty HKLM:\\...Uninstall\\* | Select DisplayName | ConvertTo-Json')",
                "3. scratch_set('cache:installed_apps', result)  ← 30m TTL auto-applied",
            ],
            "install_software": [
                "1. probe()                   — check if already installed",
                "2. job_start(cmd, label='install_X')  — background",
                "3. scratch_set('job:install_X', job_id)",
                "4. job_wait(job_id, timeout=120)",
                "5. probe()                   — verify install succeeded",
            ],
            "run_long_task": [
                "1. mem_set('tasks', 'task_plan', {steps:[...], status:'started'})  ← permanent plan",
                "2. scratch_set('plan:task', {steps:[...], current:0})              ← working state",
                "3. job_start(cmd, label='task') → scratch_set('job:task', job_id)",
                "4. job_wait(job_id, timeout=300)",
                "5. job_logs(job_id, tail=200)",
                "6. mem_set('tasks', 'task_result', summary)  ← permanent result",
                "7. scratch_gc()  ← clean expired working memory",
            ],
            "investigate_failure": [
                "1. ledger_query(status='error', last_n=5)",
                "2. error_inspect(trace_id='abc123')",
                "3. job_logs(job_id, tail=50) — if it was a job",
                "4. ledger_stats()            — which tools fail most",
            ],
        },

        "anti_patterns": {
            "FORBIDDEN_loop_shell":     "for cmd in cmds: shell_exec(cmd) — USE batch_exec()",
            "FORBIDDEN_blind_sleep":    "shell_exec('Start-Sleep 10')   — USE watch_until()/job_wait()",
            "FORBIDDEN_env_dump":       "env_get() bare              — USE filter_prefix= or name=",
            "FORBIDDEN_full_log":       "job_logs(tail=0)            — USE tail=100 or tail=200",
            "FORBIDDEN_lost_job":       "job_start() without scratch_set('job:label', id)",
            "FORBIDDEN_no_plan":        "execute without scratch_set('plan:task', ...) or mem_set",
            "FORBIDDEN_full_fs":        "fs_list(recursive=True)     — USE fs_tree() or fs_glob()",
            "FORBIDDEN_scratch_dump":   "scratch_list() bare         — USE scratch_summary()",
            "FORBIDDEN_perm_cache":     "scratch_set('apps', data) no prefix — USE cache:apps",
            "FORBIDDEN_mem_as_scratch": "mem_set() for retry counters — USE scratch_set(retry:...)",
            "FORBIDDEN_scratch_as_mem": "scratch_set('perm:config',...) — USE mem_set('config',...)",
            "FORBIDDEN_infinite_retry": "same command 4+ times — change strategy at 3 attempts",
        },
    }
    return R.ok(guide, tool="tool_guide",
                hint="Guide loaded. Key rules: TWO-LAYER MEMORY + CONTEXT ECONOMY + PARALLEL FIRST.")


@mcp.tool()
@shield
async def env_snapshot() -> str:
    """
    Lightweight mid-task state check: CPU, RAM, disk, top processes, listening ports.
    Capped output — context window efficient. Faster than ai_init().
    """
    t0   = time.monotonic()
    mem  = psutil.virtual_memory()
    disk = psutil.disk_usage("/" if not IS_WINDOWS else "C:\\")

    top_procs = sorted(
        [{"pid": p.pid, "name": p.info["name"],
          "cpu": round(p.info["cpu_percent"] or 0, 1),
          "ram_mb": round((p.info["memory_info"].rss if p.info["memory_info"] else 0) / 1024**2, 1)}
         for p in psutil.process_iter(["name", "cpu_percent", "memory_info"])
         if (p.info.get("cpu_percent") or 0) > 0.0],   # only non-idle processes
        key=lambda x: x["cpu"], reverse=True
    )[:8]   # cap at 8 — not 10

    # Only listening ports — much smaller than all connections
    listening = []
    for c in psutil.net_connections(kind="inet"):
        if c.status == "LISTEN":
            listening.append({"port": c.laddr.port, "pid": c.pid})

    data = {
        "ram_pct":        mem.percent,
        "ram_free_gb":    round(mem.available / 1024**3, 2),
        "disk_pct":       disk.percent,
        "disk_free_gb":   round(disk.free / 1024**3, 2),
        "cpu_pct":        psutil.cpu_percent(interval=0.1),
        "top_procs":      top_procs,
        "listening_ports": listening[:20],   # cap at 20
        "running_jobs":   sum(1 for j in jm.jobs.values() if not j.is_terminal),
        "active_sessions": len(sm.sessions),
        "scratch_pct_full": scratch.summary()["pct_full"],
    }
    return R.ok(data, tool="env_snapshot", t0=t0,
                hint="Quick state check done. "
                     "scratch_summary() for memory. process_list(name_filter=) for specific process.")


# ─────────────────────────────────────────────────────────────────────────────
#  GROUP 1 — SHELL EXECUTION
# ─────────────────────────────────────────────────────────────────────────────

@mcp.tool()
@shield
async def shell_exec(command: str, timeout: int = 60, cwd: str = "",
                     env_vars: str = "") -> str:
    """
    Execute a PowerShell command. Returns exit_code, stdout, stderr.
    - cwd: working directory (empty = server dir)
    - env_vars: 'KEY=VAL,KEY2=VAL2' extra env vars
    Use for PowerShell-specific commands (Get-*, registry, Windows services etc.)
    Use shell_exec_linux() for bash/native Linux commands.
    """
    t0 = time.monotonic()
    extra_env = {}
    if env_vars:
        for pair in env_vars.split(","):
            if "=" in pair:
                k, _, v = pair.partition("=")
                extra_env[k.strip()] = v.strip()

    result = await _run(command, shell=SHELL_CMD, timeout=timeout,
                        env_extra=extra_env or None, cwd=cwd or None)
    result["tool"] = "shell_exec"

    hint = ""
    if result.get("exit_code", 0) != 0:
        hint = (f"Non-zero exit ({result['exit_code']}). Check stderr. "
                "If permission error: try running with elevated privileges. "
                "If syntax error: review PS syntax. Use shell_exec_linux() for bash.")
    return R.ok(result, tool="shell_exec", t0=t0, hint=hint)


@mcp.tool()
@shield
async def shell_exec_linux(command: str, timeout: int = 60, cwd: str = "",
                            shell: str = "") -> str:
    """
    Execute a native Linux/bash command. Returns exit_code, stdout, stderr.
    - shell: override shell (default: bash). Options: bash, sh, zsh, fish
    - cwd: working directory
    Use for native Linux tools: apt, systemctl, curl, grep, awk, python3 etc.
    On Windows, this still attempts to run through pwsh.
    """
    t0  = time.monotonic()
    sh  = shell or CFG["linux_shell"]
    result = await _run(command, shell=sh, timeout=timeout, cwd=cwd or None)
    result["tool"] = "shell_exec_linux"
    hint = ""
    if result.get("exit_code", 0) != 0:
        hint = f"Exit {result['exit_code']}. Check stderr. Tool not found? Use probe() to check installed tools."
    return R.ok(result, tool="shell_exec_linux", t0=t0, hint=hint)


@mcp.tool()
@shield
async def batch_exec(commands: str, timeout_each: int = 30,
                     stop_on_failure: bool = False, shell: str = "") -> str:
    """
    ★ Run multiple commands in parallel. Massive context-window saver.
    commands: JSON array string: '["cmd1", "cmd2", "cmd3"]'
    Returns all results with per-command status.
    stop_on_failure: If True, stop after first non-zero exit.
    shell: 'ps' for PowerShell (default) or 'bash' for Linux shell.
    Example: batch_exec('["Get-Date","$PSVersionTable","Get-Location"]')
    """
    t0 = time.monotonic()
    try:
        cmds = json.loads(commands)
        if not isinstance(cmds, list): raise ValueError("Must be JSON array")
    except Exception as e:
        return R.error(f"commands must be valid JSON array: {e}",
                       tool="batch_exec",
                       suggestions=["Example: '[\"cmd1\", \"cmd2\"]'"],
                       retry_with={"commands": '["echo hello", "pwd"]'})

    sh = CFG["linux_shell"] if shell == "bash" else SHELL_CMD

    if stop_on_failure:
        # Sequential — needed so we can abort on first failure
        results = []
        for i, cmd in enumerate(cmds):
            r = await _run(cmd, shell=sh, timeout=timeout_each)
            r["index"] = i; r["cmd"] = cmd
            results.append(r)
            if r.get("exit_code", 0) != 0:
                results.append({"index": "STOPPED", "reason": f"stop_on_failure at index {i}",
                                 "exit_code": r.get("exit_code")})
                break
    else:
        # Truly parallel — all commands fire at once via asyncio.gather
        async def _run_indexed(i: int, cmd: str):
            r = await _run(cmd, shell=sh, timeout=timeout_each)
            r["index"] = i; r["cmd"] = cmd
            return r
        results = list(await asyncio.gather(*[_run_indexed(i, cmd) for i, cmd in enumerate(cmds)]))

    summary = {
        "total": len(cmds),
        "executed": len([r for r in results if "exit_code" in r]),
        "success":  len([r for r in results if r.get("exit_code") == 0]),
        "failed":   len([r for r in results if isinstance(r.get("exit_code"), int) and r["exit_code"] != 0]),
        "mode": "sequential" if stop_on_failure else "parallel",
    }
    return R.ok({"summary": summary, "results": results}, tool="batch_exec", t0=t0,
                hint=f"{summary['executed']} commands run in {summary['mode']} mode. "
                     f"{summary['failed']} failed.")


@mcp.tool()
@shield
async def chain_exec(commands: str, timeout_each: int = 30,
                     abort_on_error: bool = True) -> str:
    """
    Run commands in sequence, piping stdout of each as input context to the next.
    commands: JSON array. Each command sees prior command's output via $PREV_OUTPUT env var.
    abort_on_error: Stop chain if any command fails (default True).
    Use for: build pipelines, multi-step transforms, sequential setup steps.
    """
    t0 = time.monotonic()
    try:
        cmds = json.loads(commands)
        if not isinstance(cmds, list): raise ValueError
    except Exception:
        return R.error("commands must be JSON array string.",
                       suggestions=["batch_exec() for parallel. chain_exec() for sequential with context."])

    steps = []; prev_out = ""
    for i, cmd in enumerate(cmds):
        r = await _run(cmd, shell=SHELL_CMD, timeout=timeout_each,
                       env_extra={"PREV_OUTPUT": prev_out[:4000]})
        r["step"] = i; r["cmd"] = cmd
        steps.append(r)
        prev_out = r.get("stdout", "")
        if abort_on_error and r.get("exit_code", 0) != 0:
            steps.append({"step": "ABORTED", "reason": f"Non-zero exit at step {i}",
                          "exit_code": r.get("exit_code")})
            break

    return R.ok({
        "steps_planned": len(cmds),
        "steps_completed": len([s for s in steps if "exit_code" in s]),
        "final_output": prev_out[:5000],
        "steps": steps,
    }, tool="chain_exec", t0=t0)


@mcp.tool()
@shield
async def script_run(path: str, args: str = "", timeout: int = 120,
                     shell: str = "") -> str:
    """
    Execute a script file by path. Supports .ps1 (PowerShell) and .sh/.py etc. (Linux).
    - args: space-separated arguments
    - shell: override (ps/bash/python3 etc.)
    Safer than embedding file content in shell_exec.
    """
    t0 = time.monotonic()
    if not os.path.exists(path):
        return R.error(f"Script not found: {path}", tool="script_run",
                       suggestions=["Use fs_list() to find the correct path.",
                                    "Use fs_write() to create the script first."])

    ext    = Path(path).suffix.lower()
    sh     = shell or (SHELL_CMD if ext == ".ps1" else CFG["linux_shell"])
    flag   = "-File" if sh in ("pwsh", "powershell") else ""

    cmd_parts = [sh]
    if flag: cmd_parts.append(flag)
    cmd_parts.append(path)
    if args.strip(): cmd_parts.extend(args.split())

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd_parts, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        try:
            out, err = await asyncio.wait_for(proc.communicate(), float(timeout))
        except asyncio.TimeoutError:
            try: proc.kill()
            except: pass
            return R.error(f"Script timed out after {timeout}s.", tool="script_run",
                           suggestions=["Use job_start() to run script as background job."])

        return R.ok({
            "exit_code": proc.returncode,
            "stdout":    _clean_str(out.decode("utf-8", errors="replace")),
            "stderr":    _clean_str(err.decode("utf-8", errors="replace")) or None,
            "script":    path,
        }, tool="script_run", t0=t0)
    except FileNotFoundError:
        return R.error(f"Shell '{sh}' not found.", tool="script_run")


# ─────────────────────────────────────────────────────────────────────────────
#  GROUP 2 — JOB MANAGEMENT
# ─────────────────────────────────────────────────────────────────────────────

@mcp.tool()
@shield
async def job_start(command: str, label: str = "", tags: str = "",
                    timeout: int = 0, shell: str = "") -> str:
    """
    Start a long-running background job. Returns job_id immediately.
    - label: Human-readable name (e.g., 'install_deps', 'build_project')
    - tags: Comma-separated tags for grouping (e.g., 'phase1,setup')
    - timeout: Auto-kill after N seconds (0 = never)
    - shell: 'bash' for Linux shell, '' for PowerShell (default)
    After starting: use job_wait(job_id) to block for completion,
    or job_logs(job_id) to tail output, or job_list() to monitor.
    """
    t0     = time.monotonic()
    tag_l  = [t.strip() for t in tags.split(",") if t.strip()]
    sh     = CFG["linux_shell"] if shell == "bash" else (shell or SHELL_CMD)
    job    = await jm.start(command, label=label, tags=tag_l, timeout=timeout, shell=sh)
    return R.ok({
        "job_id":  job.id, "pid": job.pid, "label": label, "tags": tag_l,
        "log":     job.log, "status": job.status, "shell": sh,
    }, tool="job_start", t0=t0,
    hint=f"Job {job.id} started (PID {job.pid}) via {sh}. "
         f"job_wait('{job.id}') to wait, job_logs('{job.id}') to stream.")


@mcp.tool()
@shield
async def smart_exec(command: str, wait_s: int = 5, timeout: int = 0) -> str:
    """
    ★ Smart dispatch: runs command and waits up to wait_s seconds.
    If done → returns full output.
    If still running → promotes to background, returns job_id.
    Best default for unknown-duration commands.
    """
    t0  = time.monotonic()
    job = await jm.start(command, label="smart_exec", timeout=timeout)
    deadline = time.monotonic() + wait_s
    while not job.is_terminal and time.monotonic() < deadline:
        await asyncio.sleep(0.1)
    if job.is_terminal:
        out, trunc = jm.read_log(job.id)
        return R.ok({
            "mode": "immediate", "job_id": job.id,
            "status": job.status, "exit_code": job.exit_code,
            "output": _clean_str(out), "truncated": trunc,
        }, tool="smart_exec", t0=t0,
        hint="" if job.exit_code == 0 else f"Exit {job.exit_code}. Check stderr in output.")
    return R.ok({
        "mode": "background", "job_id": job.id, "pid": job.pid, "status": "running",
    }, tool="smart_exec", t0=t0,
    hint=f"Still running. job_wait('{job.id}') to block, or job_logs('{job.id}') to poll.")


@mcp.tool()
@shield
async def job_list(status: str = "", tag: str = "", last_n: int = 50) -> str:
    """
    List jobs with optional filter.
    - status: running | completed | failed | stopped | timed_out | all
    - tag: filter by tag
    - last_n: max results (newest first)
    """
    t0   = time.monotonic()
    jobs = jm.list(status=status, tag=tag, last_n=last_n)
    counts: Dict[str, int] = {}
    for j in jm.jobs.values():
        counts[j.status] = counts.get(j.status, 0) + 1
    return R.ok({"counts": counts, "filter": {"status": status, "tag": tag},
                 "results": jobs}, tool="job_list", t0=t0)


@mcp.tool()
@shield
async def job_wait(job_id: str, timeout: int = 120) -> str:
    """
    Block until a job reaches terminal state. Returns final output.
    - timeout: max seconds to wait (default 120)
    Returns full log output when done. Use after job_start() for sequential workflows.
    """
    t0  = time.monotonic()
    job = await jm.wait_for(job_id, timeout)
    if not job:
        return R.error(f"Job '{job_id}' not found.", tool="job_wait",
                       suggestions=["Call job_list() to see valid job IDs."])
    out, trunc = jm.read_log(job_id)
    hint = ""
    if job.status == "running":
        hint = f"Timeout hit ({timeout}s). Job still running. job_logs('{job_id}') to tail."
    elif job.exit_code and job.exit_code != 0:
        hint = f"Job failed (exit {job.exit_code}). Read output for errors. error_inspect() for details."
    return R.ok({
        "job_id": job_id, "status": job.status,
        "exit_code": job.exit_code, "label": job.label,
        "started": job.start, "ended": job.end,
        "duration_s": round((_dur(t0) - _dur(t0)) / 1000, 1),
        "output": _clean_str(out), "truncated": trunc,
    }, tool="job_wait", t0=t0, hint=hint)


@mcp.tool()
@shield
async def job_logs(job_id: str, tail: int = 100, head: int = 0,
                   grep: str = "") -> str:
    """
    Read job output log.
    - tail: last N lines (default 100; 0 = all)
    - head: first N lines (overrides tail)
    - grep: case-insensitive line filter
    Tip: tail=0 for full log, tail=50 for recent output, grep='error' to find errors.
    """
    t0  = time.monotonic()
    job = jm.jobs.get(job_id)
    if not job:
        return R.error(f"Job '{job_id}' not found.", tool="job_logs",
                       suggestions=["job_list() to see valid IDs."])
    out, trunc = jm.read_log(job_id, tail=tail, head=head, grep=grep)
    return R.ok({
        "job_id": job_id, "status": job.status,
        "exit_code": job.exit_code, "log_path": job.log,
        "output": out, "truncated": trunc,
    }, tool="job_logs", t0=t0,
    hint="Job still running." if not job.is_terminal else f"Job {job.status}.")


@mcp.tool()
@shield
async def job_stop(job_id: str) -> str:
    """Stop a running job (SIGTERM then SIGKILL)."""
    t0 = time.monotonic()
    ok = await jm.stop(job_id)
    if ok:
        return R.ok({"stopped": True, "job_id": job_id}, tool="job_stop", t0=t0)
    return R.error(f"Job '{job_id}' not found or already terminal.", tool="job_stop",
                   suggestions=["job_list() to see current job states."])


@mcp.tool()
@shield
async def job_cleanup(status: str = "completed", older_than_hours: int = 1) -> str:
    """
    Remove finished jobs and their log files.
    - status: completed | failed | stopped | timed_out | all
    - older_than_hours: only jobs that ended > N hours ago (0 = all matching)
    """
    t0  = time.monotonic()
    sf  = None if status == "all" else status
    cnt = await jm.purge(status=sf, older_h=older_than_hours)
    return R.ok({"removed": cnt, "filter": {"status": status, "older_than_hours": older_than_hours}},
                tool="job_cleanup", t0=t0)


# ─────────────────────────────────────────────────────────────────────────────
#  GROUP 3 — SESSIONS
# ─────────────────────────────────────────────────────────────────────────────

@mcp.tool()
@shield
async def session_create() -> str:
    """
    Create a persistent interactive PowerShell session.
    Returns session_id. Use session_exec() for all commands in that session.
    Best for: stateful work (cd into dir, activate venv, set vars, chain commands).
    """
    t0 = time.monotonic()
    s  = await sm.create()
    return R.ok({"session_id": s.id, "created": s.created},
                tool="session_create", t0=t0,
                hint=f"Session '{s.id}' ready. Use session_exec('{s.id}', 'your-command').")


@mcp.tool()
@shield
async def session_exec(session_id: str, command: str, timeout_ms: int = 8000) -> str:
    """
    ★ Run a command in a persistent session and get output reliably.
    Uses sentinel detection — output is always complete, never cut off.
    State persists: cd, $vars, venv activations carry over between calls.
    - timeout_ms: max wait in milliseconds (default 8000)
    """
    t0 = time.monotonic()
    s  = await sm.get(session_id)
    if not s:
        return R.error(f"Session '{session_id}' not found.", tool="session_exec",
                       suggestions=["session_create() to start a new session.",
                                    "session_list() to see active sessions."])
    if not s.alive:
        return R.error(f"Session '{session_id}' is dead (process exited).", tool="session_exec",
                       suggestions=["session_kill() to clean up.",
                                    "session_create() for a new session."])
    out, timed_out = await s.exec(command, timeout_ms=timeout_ms)
    hint = ""
    if timed_out:
        hint = f"Sentinel timeout ({timeout_ms}ms). Output may be partial. Increase timeout_ms or use job_start() instead."
    return R.ok({"session_id": session_id, "output": out, "cmd_count": s.cmd_count,
                 "timed_out": timed_out}, tool="session_exec", t0=t0, hint=hint)


@mcp.tool()
@shield
async def session_send(session_id: str, command: str) -> str:
    """Fire-and-forget: send command, don't wait. Follow with session_read().
    Prefer session_exec() for normal use."""
    t0 = time.monotonic()
    s  = await sm.get(session_id)
    if not s: return R.error(f"Session '{session_id}' not found.", tool="session_send")
    await s._raw(command)
    return R.ok({"sent": True, "hint": "Use session_read() after delay, or session_exec() for reliable output."},
                tool="session_send", t0=t0)


@mcp.tool()
@shield
async def session_read(session_id: str, wait_ms: int = 400) -> str:
    """Read buffered output from session. wait_ms controls how long to wait for output."""
    t0 = time.monotonic()
    s  = await sm.get(session_id)
    if not s: return R.error(f"Session '{session_id}' not found.", tool="session_read")
    await asyncio.sleep(wait_ms / 1000)
    out = await s._drain()
    return R.ok({"session_id": session_id, "output": out}, tool="session_read", t0=t0)


@mcp.tool()
@shield
async def session_list() -> str:
    """List all active sessions with status, creation time, command count."""
    t0       = time.monotonic()
    sessions = sm.list_all()
    return R.ok({"count": len(sessions), "sessions": sessions}, tool="session_list", t0=t0)


@mcp.tool()
@shield
async def session_kill(session_id: str) -> str:
    """Terminate a session and free resources."""
    t0 = time.monotonic()
    ok = await sm.kill(session_id)
    if ok: return R.ok({"killed": True, "session_id": session_id}, tool="session_kill", t0=t0)
    return R.error(f"Session '{session_id}' not found.", tool="session_kill")


# ─────────────────────────────────────────────────────────────────────────────
#  GROUP 4 — FILE SYSTEM
# ─────────────────────────────────────────────────────────────────────────────

@mcp.tool()
@shield
async def fs_read(path: str, offset: int = 0, length: int = 100_000,
                  tail: int = 0, encoding: str = "utf-8") -> str:
    """Read file content. offset+length for pagination. tail for last N lines."""
    t0 = time.monotonic()
    if not os.path.exists(path):
        return R.error(f"File not found: {path}", tool="fs_read",
                       suggestions=["fs_list(parent_dir) to see available files.",
                                    "probe() to search for the file."])

    def _read():
        sz = os.path.getsize(path)
        if tail > 0:
            with open(path, "r", encoding=encoding, errors="replace") as f:
                lines = f.readlines()
            content = "".join(lines[-tail:])
        else:
            with open(path, "r", encoding=encoding, errors="replace") as f:
                f.seek(offset); content = f.read(length)
        return {"path": path, "total_bytes": sz,
                "content": content, "tail_mode": tail > 0,
                "offset": offset if not tail else -1,
                "has_more": (offset + length < sz) if not tail else False}

    result = await asyncio.to_thread(_read)
    return R.ok(result, tool="fs_read", t0=t0)


@mcp.tool()
@shield
async def fs_write(path: str, content: str, mode: str = "w") -> str:
    """Write or append to file. mode: 'w' overwrite (atomic) | 'a' append.
    Creates parent directories automatically.
    Overwrites use temp-file + rename — safe against partial-write corruption."""
    t0 = time.monotonic()
    if mode not in ("w", "a"):
        return R.error("mode must be 'w' or 'a'.", tool="fs_write")
    if not _disk_ok(50):
        return R.error("Disk space critically low (<50MB free). Write aborted.",
                       tool="fs_write",
                       suggestions=["Free disk space.", "Use fs_delete() to remove old files.",
                                    "Check disk with env_snapshot()"])

    def _write():
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        if mode == "w":
            # Atomic: write to .tmp then rename — prevents partial-write corruption
            tmp = path + ".gmtmp"
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(content)
            os.replace(tmp, path)
        else:
            with open(path, "a", encoding="utf-8") as f:
                f.write(content)
        return os.path.getsize(path)

    sz = await asyncio.to_thread(_write)
    return R.ok({"path": path, "bytes_written": len(content.encode()), "total_size": sz,
                 "mode": mode, "atomic": mode == "w"}, tool="fs_write", t0=t0)


@mcp.tool()
@shield
async def fs_delete(path: str) -> str:
    """Delete file or directory tree. No confirmation needed — AI is trusted."""
    t0 = time.monotonic()
    if not os.path.exists(path):
        return R.error(f"Path not found: {path}", tool="fs_delete")

    def _del():
        is_file = os.path.isfile(path)
        sz = os.path.getsize(path) if is_file else None
        if is_file: os.remove(path)
        else: shutil.rmtree(path)
        return is_file, sz

    is_file, sz = await asyncio.to_thread(_del)
    return R.ok({"deleted": path, "type": "file" if is_file else "directory",
                 "size_bytes": sz}, tool="fs_delete", t0=t0)


@mcp.tool()
@shield
async def fs_list(path: str, recursive: bool = False,
                  pattern: str = "") -> str:
    """List directory. recursive=True for subtree. pattern: glob filter (e.g. '*.py')"""
    t0 = time.monotonic()
    if not os.path.exists(path):
        return R.error(f"Path not found: {path}", tool="fs_list",
                       suggestions=["fs_info(path) to check if path exists.",
                                    "probe() to find files by name."])

    def _list():
        items = []
        if recursive:
            for root, dirs, files in os.walk(path):
                dirs[:] = [d for d in dirs if not d.startswith(".")]
                for name in dirs + files:
                    full = os.path.join(root, name)
                    if pattern and not fnmatch.fnmatch(name, pattern): continue
                    try:
                        st = os.stat(full)
                        items.append({"path": full,
                                      "rel": os.path.relpath(full, path),
                                      "type": "dir" if os.path.isdir(full) else "file",
                                      "size": st.st_size if os.path.isfile(full) else None,
                                      "mtime": datetime.fromtimestamp(st.st_mtime).isoformat()})
                    except OSError: pass
        else:
            for name in os.listdir(path):
                if pattern:
                                if not fnmatch.fnmatch(name, pattern): continue
                full = os.path.join(path, name)
                try:
                    st = os.stat(full)
                    items.append({"name": name,
                                  "type": "dir" if os.path.isdir(full) else "file",
                                  "size": st.st_size if os.path.isfile(full) else None,
                                  "mtime": datetime.fromtimestamp(st.st_mtime).isoformat()})
                except OSError: pass
        return items

    items = await asyncio.to_thread(_list)
    return R.ok({"path": path, "count": len(items), "items": items}, tool="fs_list", t0=t0)


@mcp.tool()
@shield
async def fs_info(path: str) -> str:
    """Get metadata about a path: exists, type, size, permissions, owner."""
    t0 = time.monotonic()
    exists = os.path.exists(path)
    if not exists:
        return R.ok({"path": path, "exists": False}, tool="fs_info", t0=t0,
                    hint="Path does not exist. Use fs_write() to create it or fs_list() to explore.")
    try:
        st = os.stat(path)
        info = {
            "path": path, "exists": True,
            "type": "file" if os.path.isfile(path) else ("dir" if os.path.isdir(path) else "other"),
            "size_bytes": st.st_size,
            "size_human": f"{st.st_size / 1024:.1f}KB" if st.st_size < 1024*1024 else f"{st.st_size/1024**2:.1f}MB",
            "created":    datetime.fromtimestamp(st.st_ctime).isoformat(),
            "modified":   datetime.fromtimestamp(st.st_mtime).isoformat(),
            "permissions": oct(stat_mod.S_IMODE(st.st_mode)),
            "is_symlink": os.path.islink(path),
        }
        if not IS_WINDOWS:
            info["uid"] = st.st_uid; info["gid"] = st.st_gid
        return R.ok(info, tool="fs_info", t0=t0)
    except Exception as e:
        return R.error(str(e), tool="fs_info")


@mcp.tool()
@shield
async def fs_move(source: str, destination: str, overwrite: bool = False) -> str:
    """Move or rename a file/directory."""
    t0 = time.monotonic()
    if not os.path.exists(source):
        return R.error(f"Source not found: {source}", tool="fs_move")
    if os.path.exists(destination) and not overwrite:
        return R.error(f"Destination exists: '{destination}'. Set overwrite=True.",
                       tool="fs_move", retry_with={"overwrite": True})
    def _mv():
        Path(destination).parent.mkdir(parents=True, exist_ok=True)
        shutil.move(source, destination)
    await asyncio.to_thread(_mv)
    return R.ok({"moved": True, "from": source, "to": destination}, tool="fs_move", t0=t0)


@mcp.tool()
@shield
async def fs_copy(source: str, destination: str, overwrite: bool = False) -> str:
    """Copy a file or directory tree."""
    t0 = time.monotonic()
    if not os.path.exists(source):
        return R.error(f"Source not found: {source}", tool="fs_copy")
    if os.path.exists(destination) and not overwrite:
        return R.error(f"Destination exists. Set overwrite=True.",
                       tool="fs_copy", retry_with={"overwrite": True})
    def _cp():
        Path(destination).parent.mkdir(parents=True, exist_ok=True)
        if os.path.isdir(source):
            if os.path.exists(destination): shutil.rmtree(destination)
            shutil.copytree(source, destination)
        else:
            shutil.copy2(source, destination)
    await asyncio.to_thread(_cp)
    return R.ok({"copied": True, "from": source, "to": destination}, tool="fs_copy", t0=t0)


@mcp.tool()
@shield
async def fs_bulk_create(items: str) -> str:
    """
    ★ Create multiple files and/or directories in ONE call.
    items: JSON array of objects. Each object:
      {"path": "/some/file.txt", "content": "text here"}   → creates file
      {"path": "/some/dir/",     "type": "dir"}             → creates directory
      {"path": "/some/file.py",  "content": "...", "mode": "a"} → append mode
    Parent directories created automatically.
    Returns per-item results. Much faster than calling fs_write() in a loop.
    Example: fs_bulk_create('[{"path":"/tmp/a.txt","content":"hello"},{"path":"/tmp/logs/","type":"dir"}]')
    """
    t0 = time.monotonic()
    try:
        ops = json.loads(items)
        if not isinstance(ops, list): raise ValueError("Must be JSON array")
    except Exception as e:
        return R.error(f"items must be valid JSON array: {e}", tool="fs_bulk_create",
                       suggestions=["Each item: {\"path\": \"...\", \"content\": \"...\"} for file or {\"path\": \"dir/\", \"type\": \"dir\"} for directory"])

    results = []
    for op in ops:
        path = op.get("path", "")
        if not path:
            results.append({"path": path, "ok": False, "error": "path is required"})
            continue
        try:
            is_dir = op.get("type") == "dir" or path.endswith("/")
            if is_dir:
                Path(path).mkdir(parents=True, exist_ok=True)
                results.append({"path": path, "ok": True, "type": "dir"})
            else:
                content = op.get("content", "")
                mode    = op.get("mode", "w")
                Path(path).parent.mkdir(parents=True, exist_ok=True)
                if mode == "w":
                    tmp = path + ".gmtmp"
                    with open(tmp, "w", encoding="utf-8") as f: f.write(content)
                    os.replace(tmp, path)
                else:
                    with open(path, "a", encoding="utf-8") as f: f.write(content)
                results.append({"path": path, "ok": True, "type": "file",
                                 "bytes": len(content.encode())})
        except Exception as e:
            results.append({"path": path, "ok": False, "error": str(e)})

    ok_count  = sum(1 for r in results if r["ok"])
    return R.ok({
        "total": len(results), "success": ok_count, "failed": len(results) - ok_count,
        "results": results,
    }, tool="fs_bulk_create", t0=t0,
    hint=f"{ok_count}/{len(results)} items created." +
         (" Check results for errors." if ok_count < len(results) else ""))


@mcp.tool()
@shield
async def fs_bulk_delete(paths: str, ignore_missing: bool = True) -> str:
    """
    Delete multiple files and/or directories in ONE call.
    paths: JSON array of path strings: '["/tmp/a.txt", "/tmp/old_dir", "/tmp/b.log"]'
    ignore_missing: if True (default), missing paths are noted but don't fail.
    Returns per-path results with type and size.
    """
    t0 = time.monotonic()
    try:
        path_list = json.loads(paths)
        if not isinstance(path_list, list): raise ValueError
    except Exception as e:
        return R.error(f"paths must be JSON array of strings: {e}", tool="fs_bulk_delete")

    def _del_all():
        results = []
        for p in path_list:
            if not os.path.exists(p):
                if ignore_missing:
                    results.append({"path": p, "ok": True, "note": "already absent"})
                else:
                    results.append({"path": p, "ok": False, "error": "not found"})
                continue
            try:
                is_file = os.path.isfile(p)
                sz      = os.path.getsize(p) if is_file else None
                if is_file: os.remove(p)
                else:       shutil.rmtree(p)
                results.append({"path": p, "ok": True,
                                 "type": "file" if is_file else "dir", "size_bytes": sz})
            except Exception as e:
                results.append({"path": p, "ok": False, "error": str(e)})
        return results

    results  = await asyncio.to_thread(_del_all)
    ok_count = sum(1 for r in results if r["ok"])
    return R.ok({
        "total": len(results), "deleted": ok_count, "failed": len(results) - ok_count,
        "results": results,
    }, tool="fs_bulk_delete", t0=t0)


@mcp.tool()
@shield
async def fs_bulk_move(pairs: str, overwrite: bool = False) -> str:
    """
    Move/rename multiple files or directories in ONE call.
    pairs: JSON array of {from, to} objects:
      '[{"from": "/tmp/old.txt", "to": "/data/new.txt"}, ...]'
    overwrite: allow overwriting existing destinations (default False).
    """
    t0 = time.monotonic()
    try:
        ops = json.loads(pairs)
        if not isinstance(ops, list): raise ValueError
    except Exception as e:
        return R.error(f"pairs must be JSON array: {e}", tool="fs_bulk_move",
                       suggestions=["Format: '[{\"from\": \"src\", \"to\": \"dst\"}]'"])

    def _move_all():
        results = []
        for op in ops:
            src = op.get("from", "")
            dst = op.get("to", "")
            if not src or not dst:
                results.append({"from": src, "to": dst, "ok": False, "error": "from/to required"})
                continue
            if not os.path.exists(src):
                results.append({"from": src, "to": dst, "ok": False, "error": "source not found"})
                continue
            if os.path.exists(dst) and not overwrite:
                results.append({"from": src, "to": dst, "ok": False,
                                 "error": f"destination exists, set overwrite=True"})
                continue
            try:
                Path(dst).parent.mkdir(parents=True, exist_ok=True)
                shutil.move(src, dst)
                results.append({"from": src, "to": dst, "ok": True})
            except Exception as e:
                results.append({"from": src, "to": dst, "ok": False, "error": str(e)})
        return results

    results  = await asyncio.to_thread(_move_all)
    ok_count = sum(1 for r in results if r["ok"])
    return R.ok({
        "total": len(results), "moved": ok_count, "failed": len(results) - ok_count,
        "results": results,
    }, tool="fs_bulk_move", t0=t0)


@mcp.tool()
@shield
async def fs_bulk_copy(pairs: str, overwrite: bool = False) -> str:
    """
    Copy multiple files or directory trees in ONE call.
    pairs: JSON array of {from, to} objects:
      '[{"from": "/src/config.json", "to": "/dst/config.json"}, ...]'
    Directory trees are copied recursively.
    """
    t0 = time.monotonic()
    try:
        ops = json.loads(pairs)
        if not isinstance(ops, list): raise ValueError
    except Exception as e:
        return R.error(f"pairs must be JSON array: {e}", tool="fs_bulk_copy")

    def _copy_all():
        results = []
        for op in ops:
            src = op.get("from", "")
            dst = op.get("to", "")
            if not src or not dst:
                results.append({"from": src, "to": dst, "ok": False, "error": "from/to required"})
                continue
            if not os.path.exists(src):
                results.append({"from": src, "to": dst, "ok": False, "error": "source not found"})
                continue
            if os.path.exists(dst) and not overwrite:
                results.append({"from": src, "to": dst, "ok": False,
                                 "error": "destination exists, set overwrite=True"})
                continue
            try:
                Path(dst).parent.mkdir(parents=True, exist_ok=True)
                if os.path.isdir(src):
                    if os.path.exists(dst): shutil.rmtree(dst)
                    shutil.copytree(src, dst)
                    results.append({"from": src, "to": dst, "ok": True, "type": "dir"})
                else:
                    shutil.copy2(src, dst)
                    results.append({"from": src, "to": dst, "ok": True, "type": "file"})
            except Exception as e:
                results.append({"from": src, "to": dst, "ok": False, "error": str(e)})
        return results

    results  = await asyncio.to_thread(_copy_all)
    ok_count = sum(1 for r in results if r["ok"])
    return R.ok({
        "total": len(results), "copied": ok_count, "failed": len(results) - ok_count,
        "results": results,
    }, tool="fs_bulk_copy", t0=t0)


@mcp.tool()
@shield
async def fs_tree(path: str, max_depth: int = 3, max_items: int = 200,
                   show_size: bool = False) -> str:
    """
    ★ Compact directory tree view — like 'tree' command output.
    Context-efficient: shows structure without listing full metadata per item.
    - max_depth: how deep to recurse (default 3, max 8)
    - max_items: cap total items shown (default 200)
    - show_size: include file sizes (slightly larger output)
    Much more context-efficient than fs_list(recursive=True) for exploring structure.
    """
    t0        = time.monotonic()
    max_depth = min(max_depth, 8)

    def _build_tree(base: str, depth: int, prefix: str, items_left: list) -> List[str]:
        if depth > max_depth or items_left[0] <= 0:
            return []
        lines = []
        try:
            entries = sorted(os.scandir(base), key=lambda e: (not e.is_dir(), e.name.lower()))
        except PermissionError:
            return [f"{prefix}  [permission denied]"]
        for i, entry in enumerate(entries):
            if items_left[0] <= 0:
                lines.append(f"{prefix}  … (truncated at {max_items} items)")
                break
            is_last  = (i == len(entries) - 1)
            connector = "└── " if is_last else "├── "
            ext_pfx   = "    " if is_last else "│   "
            name = entry.name
            if entry.is_dir(follow_symlinks=False):
                lines.append(f"{prefix}{connector}{name}/")
                items_left[0] -= 1
                lines.extend(_build_tree(entry.path, depth + 1, prefix + ext_pfx, items_left))
            else:
                sz = ""
                if show_size:
                    try: sz = f"  [{entry.stat().st_size:,}B]"
                    except: sz = ""
                lines.append(f"{prefix}{connector}{name}{sz}")
                items_left[0] -= 1
        return lines

    if not os.path.exists(path):
        return R.error(f"Path not found: {path}", tool="fs_tree")

    counter   = [max_items]
    tree_lines = [path + "/"]
    tree_lines.extend(await asyncio.to_thread(_build_tree, path, 1, "", counter))
    truncated  = counter[0] <= 0

    return R.ok({
        "path":      path,
        "tree":      "\n".join(tree_lines),
        "truncated": truncated,
        "max_depth": max_depth,
    }, tool="fs_tree", t0=t0,
    hint=f"Tree rendered. {'Truncated at max_items=' + str(max_items) + '.' if truncated else ''} "
         "Use max_depth= to control recursion depth.")


@mcp.tool()
@shield
async def fs_glob(pattern: str, base_path: str = ".", max_results: int = 200) -> str:
    """
    Find files using glob pattern matching.
    pattern: glob pattern e.g. '**/*.py', '*.log', 'src/**/*.json', 'test_*.txt'
    base_path: directory to search from (default: current dir)
    max_results: cap results (default 200)
    Faster and more precise than fs_search() when you know the filename pattern.
    Example: fs_glob('**/*.py', base_path='C:/myproject')
    """
    t0 = time.monotonic()
    if not os.path.exists(base_path):
        return R.error(f"base_path not found: {base_path}", tool="fs_glob")

    def _glob():
        base = Path(base_path)
        try:
            matches = []
            for p in base.glob(pattern):
                if len(matches) >= max_results: break
                try:
                    st = p.stat()
                    matches.append({
                        "path":     str(p),
                        "rel":      str(p.relative_to(base)),
                        "type":     "dir" if p.is_dir() else "file",
                        "size_bytes": st.st_size if p.is_file() else None,
                    })
                except OSError:
                    matches.append({"path": str(p), "rel": str(p.relative_to(base))})
            return matches
        except Exception as e:
            return {"error": str(e)}

    result = await asyncio.to_thread(_glob)
    if isinstance(result, dict) and "error" in result:
        return R.error(result["error"], tool="fs_glob",
                       suggestions=["Check glob pattern syntax. Example: '**/*.py'"])

    truncated = len(result) >= max_results
    return R.ok({
        "pattern":   pattern,
        "base_path": base_path,
        "count":     len(result),
        "truncated": truncated,
        "matches":   result,
    }, tool="fs_glob", t0=t0,
    hint=f"{len(result)} matches." + (" Results capped. Narrow pattern or increase max_results." if truncated else ""))


@mcp.tool()
@shield
async def fs_search(path: str, pattern: str, file_pattern: str = "",
                    max_results: int = 500, case_sensitive: bool = False) -> str:
    """
    Regex search inside files recursively.
    - file_pattern: only search files matching this glob (e.g. '*.py')
    - max_results: cap results (default 500)
    Returns: file path, line number, matching line content.
    """
    t0 = time.monotonic()
    def _search():
        flags = 0 if case_sensitive else re.IGNORECASE
        try: rx = re.compile(pattern, flags)
        except re.error as e: return {"error": f"Bad regex: {e}", "results": []}
        matches = []
        for root, dirs, files in os.walk(path):
            dirs[:] = [d for d in dirs if not d.startswith(".")]
            for fname in files:
                if file_pattern and not fnmatch.fnmatch(fname, file_pattern): continue
                fpath = os.path.join(root, fname)
                try:
                    with open(fpath, "r", encoding="utf-8", errors="ignore") as f:
                        for i, line in enumerate(f, 1):
                            if rx.search(line):
                                matches.append({"file": fpath, "line": i,
                                                "text": line.rstrip()[:400]})
                                if len(matches) >= max_results:
                                    return {"results": matches, "truncated": True, "count": len(matches)}
                except OSError: pass
        return {"results": matches, "truncated": False, "count": len(matches)}

    result = await asyncio.to_thread(_search)
    if "error" in result:
        return R.error(result["error"], tool="fs_search")
    return R.ok(result, tool="fs_search", t0=t0,
                hint=f"{result['count']} matches found." + (" Results truncated." if result["truncated"] else ""))


# ─────────────────────────────────────────────────────────────────────────────
#  GROUP 5 — SYSTEM
# ─────────────────────────────────────────────────────────────────────────────

@mcp.tool()
@shield
async def sys_info() -> str:
    """Full system resource snapshot: CPU, RAM, disk, boot time."""
    t0 = time.monotonic()
    def _get():
        mem  = psutil.virtual_memory()
        disk = psutil.disk_usage("/" if not IS_WINDOWS else "C:\\")
        load = getattr(os, "getloadavg", lambda: None)()
        if load is None:
            p = psutil.cpu_percent(interval=0.2); load = (p, p, p)
        return {
            "os": platform.system(), "hostname": socket.gethostname(),
            "cpu": {"cores": psutil.cpu_count(), "load_1m_5m_15m": [round(x,2) for x in load]},
            "ram": {"total_gb": round(mem.total/1024**3,2),
                    "free_gb": round(mem.available/1024**3,2), "pct": mem.percent},
            "disk": {"total_gb": round(disk.total/1024**3,2),
                     "free_gb": round(disk.free/1024**3,2), "pct": disk.percent},
            "boot": datetime.fromtimestamp(psutil.boot_time()).isoformat(),
        }
    return R.ok(await asyncio.to_thread(_get), tool="sys_info", t0=t0)


@mcp.tool()
@shield
async def process_list(sort_by: str = "cpu", top_n: int = 15,
                        name_filter: str = "", output_mode: str = "normal") -> str:
    """
    List running processes.
    - sort_by: cpu | ram | pid | name
    - top_n: max results (default 15, max 50)
    - name_filter: substring match on process name
    - output_mode: 'minimal' (pid+name+cpu only) | 'normal' | 'full' (with cmdline)
    Context tip: use name_filter to find specific process, not full list.
    """
    t0     = time.monotonic()
    top_n  = min(top_n, 50)
    fields = ["pid", "name", "cpu_percent", "memory_info", "status"]
    if output_mode == "full":
        fields.append("cmdline")

    def _list():
        procs = []
        for p in psutil.process_iter(fields):
            try:
                info = p.info
                if name_filter and name_filter.lower() not in (info["name"] or "").lower():
                    continue
                entry: Dict[str, Any] = {
                    "pid":    info["pid"],
                    "name":   info["name"],
                    "cpu":    round(info["cpu_percent"] or 0, 1),
                }
                if output_mode != "minimal":
                    entry["ram_mb"]  = round((info["memory_info"].rss if info["memory_info"] else 0) / 1024**2, 1)
                    entry["status"]  = info["status"]
                if output_mode == "full" and "cmdline" in info:
                    entry["cmd"] = " ".join((info["cmdline"] or [])[:6])
                procs.append(entry)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
        key_map = {"cpu": "cpu", "ram": "ram_mb", "pid": "pid", "name": "name"}
        procs.sort(key=lambda x: x.get(key_map.get(sort_by, "cpu"), 0), reverse=(sort_by != "name"))
        return procs[:top_n]

    return R.ok({"processes": await asyncio.to_thread(_list)},
                tool="process_list", t0=t0,
                hint="Use name_filter= to find specific process. output_mode='minimal' for smallest response.")


@mcp.tool()
@shield
async def process_kill(pid: str, signal_type: str = "TERM") -> str:
    """
    Kill a process by PID.
    - signal_type: TERM (graceful, default) | KILL (force) | INT
    Will not kill PID <10 or the server's own PID.
    """
    t0 = time.monotonic()
    ok, pid_int, msg = _validate_pid(pid)
    if not ok: return R.error(msg, tool="process_kill",
                              suggestions=["Use process_list() to find valid PIDs."])
    sig_map = {"TERM": signal.SIGTERM, "KILL": signal.SIGKILL, "INT": signal.SIGINT}
    sig = sig_map.get(signal_type.upper(), signal.SIGTERM)
    try:
        os.kill(pid_int, sig)
        return R.ok({"killed": True, "pid": pid_int, "signal": signal_type}, tool="process_kill", t0=t0)
    except ProcessLookupError:
        return R.error(f"PID {pid_int} not found.", tool="process_kill")
    except PermissionError:
        return R.error(f"Permission denied killing PID {pid_int}.",
                       tool="process_kill",
                       suggestions=["Try with elevated privileges.",
                                    "Use shell_exec('Stop-Process -Id X -Force') on Windows."])


@mcp.tool()
@shield
async def service_ctrl(action: str, name: str) -> str:
    """
    Control system services.
    - action: status | start | stop | restart
    Uses Get-Service on Windows, systemctl on Linux (via pwsh).
    """
    t0    = time.monotonic()
    VALID = {"status", "start", "stop", "restart"}
    if action not in VALID:
        return R.error(f"action must be: {', '.join(sorted(VALID))}",
                       tool="service_ctrl")
    CMDLETS = {"status": "Get-Service", "start": "Start-Service",
               "stop": "Stop-Service", "restart": "Restart-Service"}
    safe = _ps_arg(name)
    if action == "status":
        cmd = f"{CMDLETS[action]} -Name '{safe}' -ErrorAction SilentlyContinue | Select-Object Name,Status,DisplayName,StartType | ConvertTo-Json"
    else:
        if IS_WINDOWS:
            cmd = f"{CMDLETS[action]} -Name '{safe}' -PassThru -ErrorAction Stop | Select-Object Name,Status | ConvertTo-Json"
        else:
            cmd = f"systemctl {action} '{safe}' && systemctl status '{safe}' --no-pager"
    result = await _run(cmd, shell=SHELL_CMD if IS_WINDOWS else CFG["linux_shell"], timeout=30)
    return R.ok({"action": action, "service": name, "result": result},
                tool="service_ctrl", t0=t0)


@mcp.tool()
@shield
async def env_get(name: str = "", filter_prefix: str = "") -> str:
    """
    Get environment variables. CONTEXT-AWARE output:
    - name='VAR'           → single variable value (minimal output)
    - filter_prefix='APP_' → only vars starting with prefix
    - name='' no prefix    → summary only (count + first 30 names), NOT full dump
    Use filter_prefix to get a subset. Never call with no args to dump all — too large.
    """
    t0 = time.monotonic()
    if name:
        v = os.environ.get(name)
        return R.ok({"name": name, "value": v, "found": v is not None}, tool="env_get", t0=t0)
    env = dict(os.environ)
    if filter_prefix:
        env = {k: v for k, v in env.items() if k.startswith(filter_prefix)}
        return R.ok({"vars": env, "count": len(env), "filter": filter_prefix}, tool="env_get", t0=t0)
    # No filter — return summary, not full dump (context protection)
    names = sorted(env.keys())
    return R.ok({
        "count":     len(names),
        "preview":   names[:40],
        "note":      "Full dump suppressed to protect context window. Use filter_prefix= or name= for values.",
        "hint_usage": "env_get(filter_prefix='APP_') or env_get(name='PATH')",
    }, tool="env_get", t0=t0,
    hint="Use filter_prefix='PREFIX_' to get a subset of env vars.")


@mcp.tool()
@shield
async def env_set(name: str, value: str) -> str:
    """Set an environment variable for the server process."""
    t0 = time.monotonic()
    PROTECTED = {"API_KEY", "PATH", "LD_PRELOAD", "LD_LIBRARY_PATH"}
    if name.upper() in PROTECTED:
        return R.error(f"'{name}' is protected.", tool="env_set",
                       suggestions=["Use a different variable name."])
    os.environ[name] = value
    return R.ok({"set": True, "name": name, "value": value}, tool="env_set", t0=t0)


@mcp.tool()
@shield
async def log_read(path: str = "", lines: int = 100) -> str:
    """Read tail of a log file. Default: server log. lines capped at 5000."""
    t0    = time.monotonic()
    lines = min(lines, 5000)
    path  = path or LOG_FILE
    if not os.path.exists(path):
        return R.error(f"Log not found: {path}", tool="log_read",
                       suggestions=[f"Server log is at: {LOG_FILE}",
                                    f"Error log is at: {ERR_FILE}"])
    def _tail():
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            return "".join(f.readlines()[-lines:])
    content = await asyncio.to_thread(_tail)
    return R.ok({"path": path, "lines": lines, "content": content}, tool="log_read", t0=t0)


# ─────────────────────────────────────────────────────────────────────────────
#  GROUP 6 — NETWORK
# ─────────────────────────────────────────────────────────────────────────────

@mcp.tool()
@shield
async def net_dns(hostname: str) -> str:
    """DNS resolution. Returns all A/AAAA records."""
    t0 = time.monotonic()
    def _resolve():
        try:
            r = socket.getaddrinfo(hostname, None)
            return {"hostname": hostname, "addresses": sorted({x[4][0] for x in r}), "resolved": True}
        except socket.gaierror as e:
            return {"hostname": hostname, "resolved": False, "error": str(e)}
    return R.ok(await asyncio.to_thread(_resolve), tool="net_dns", t0=t0)


@mcp.tool()
@shield
async def net_connect(target: str, port: int, timeout_s: float = 5.0) -> str:
    """TCP connectivity test. Returns latency if connected."""
    t0 = time.monotonic()
    if not (1 <= port <= 65535):
        return R.error("port must be 1–65535", tool="net_connect")
    try:
        t_conn = time.monotonic()
        r, w   = await asyncio.wait_for(asyncio.open_connection(target, port), timeout_s)
        lat    = round((time.monotonic() - t_conn) * 1000, 2)
        w.close()
        try: await w.wait_closed()
        except: pass
        return R.ok({"target": target, "port": port, "connected": True,
                     "latency_ms": lat}, tool="net_connect", t0=t0)
    except asyncio.TimeoutError:
        return R.ok({"target": target, "port": port, "connected": False,
                     "error": "timeout"}, tool="net_connect", t0=t0)
    except ConnectionRefusedError:
        return R.ok({"target": target, "port": port, "connected": False,
                     "error": "refused"}, tool="net_connect", t0=t0)
    except OSError as e:
        return R.ok({"target": target, "port": port, "connected": False,
                     "error": str(e)}, tool="net_connect", t0=t0)


@mcp.tool()
@shield
async def net_inspect(scope: str = "all") -> str:
    """Network overview. scope: interfaces | routes | sockets | all"""
    t0 = time.monotonic()
    VALID = {"interfaces", "routes", "sockets", "all"}
    if scope not in VALID:
        return R.error(f"scope must be: {', '.join(sorted(VALID))}", tool="net_inspect")

    def _get():
        result: Dict[str, Any] = {}
        if scope in ("interfaces", "all"):
            result["interfaces"] = [
                {"name": nic, "addrs": [a.address for a in addrs]}
                for nic, addrs in psutil.net_if_addrs().items()
            ]
        if scope in ("sockets", "all"):
            conns = []
            for c in psutil.net_connections(kind="inet"):
                conns.append({
                    "proto": "tcp" if c.type == socket.SOCK_STREAM else "udp",
                    "local": f"{c.laddr.ip}:{c.laddr.port}" if c.laddr else "",
                    "remote": f"{c.raddr.ip}:{c.raddr.port}" if c.raddr else "",
                    "status": c.status, "pid": c.pid,
                })
            result["connections"] = conns
        return result

    data = await asyncio.to_thread(_get)
    if scope in ("routes", "all"):
        r = await _run("ip route" if not IS_WINDOWS else "Get-NetRoute | Select-Object DestinationPrefix,NextHop | ConvertTo-Json")
        data["routes"] = r.get("stdout", "")
    # Cap connections to prevent context flooding
    if "connections" in data:
        all_conns = data["connections"]
        data["connections"] = all_conns[:100]
        data["connections_total"] = len(all_conns)
        if len(all_conns) > 100:
            data["connections_note"] = f"Showing 100/{len(all_conns)}. Use scope='sockets' with process filter for full list."
    return R.ok(data, tool="net_inspect", t0=t0,
                hint="Use scope='interfaces' or 'sockets' instead of 'all' to reduce output size.")


@mcp.tool()
@shield
async def http_request(url: str, method: str = "GET", body: str = "",
                       headers: str = "", timeout_s: float = 30.0,
                       follow_redirects: bool = True) -> str:
    """
    ★ Make HTTP requests — GET, POST, PUT, PATCH, DELETE.
    - url: full URL including scheme (https://...)
    - method: GET | POST | PUT | PATCH | DELETE | HEAD
    - body: request body string (JSON, form data, etc.)
    - headers: JSON string {'Content-Type': 'application/json', 'X-Key': 'val'}
    - timeout_s: request timeout in seconds
    Returns: status_code, headers, body (first 50KB), latency_ms.
    Use for: API testing, health checks, downloading config, webhook testing.
    """
    import urllib.request, urllib.error
    t0 = time.monotonic()
    VALID_METHODS = {"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD"}
    method = method.upper()
    if method not in VALID_METHODS:
        return R.error(f"method must be one of: {', '.join(sorted(VALID_METHODS))}", tool="http_request")

    # Parse extra headers
    extra_headers: Dict[str, str] = {}
    if headers.strip():
        try:
            extra_headers = json.loads(headers)
        except Exception as e:
            return R.error(f"headers must be valid JSON object: {e}", tool="http_request",
                           suggestions=["Example: '{\"Content-Type\": \"application/json\"}'"])

    def _do_request():
        data = body.encode("utf-8") if body else None
        req = urllib.request.Request(url, data=data, method=method)
        req.add_header("User-Agent", f"GodMode-MCP/{__version__}")
        if body and "Content-Type" not in extra_headers:
            # Auto-detect JSON body
            try:
                json.loads(body)
                req.add_header("Content-Type", "application/json")
            except Exception:
                req.add_header("Content-Type", "text/plain")
        for k, v in extra_headers.items():
            req.add_header(k, str(v))

        try:
            ctx = None
            if not follow_redirects:
                import urllib.request as ur
                ctx = ur.build_opener(ur.HTTPRedirectHandler())
            opener = urllib.request.build_opener() if follow_redirects else ctx
            with opener.open(req, timeout=timeout_s) as resp:
                raw = resp.read(51200)  # 50KB cap
                resp_body = raw.decode("utf-8", errors="replace")
                resp_headers = dict(resp.headers)
                return {
                    "status_code":    resp.status,
                    "url_final":      resp.url,
                    "headers":        resp_headers,
                    "body":           resp_body,
                    "body_truncated": len(raw) >= 51200,
                    "latency_ms":     round((time.monotonic() - t0) * 1000, 2),
                }
        except urllib.error.HTTPError as e:
            raw = e.read(10240)
            return {
                "status_code": e.code,
                "error":       str(e.reason),
                "body":        raw.decode("utf-8", errors="replace"),
                "latency_ms":  round((time.monotonic() - t0) * 1000, 2),
            }
        except urllib.error.URLError as e:
            return {"status_code": 0, "error": str(e.reason),
                    "latency_ms": round((time.monotonic() - t0) * 1000, 2)}

    result = await asyncio.to_thread(_do_request)
    status = result.get("status_code", 0)
    hint   = ""
    if status == 0:
        hint = "Connection failed. net_dns() / net_connect() to check reachability."
    elif status >= 400:
        hint = f"HTTP {status}. Read body for error details. Check URL, auth headers, and request body."
    return R.ok(result, tool="http_request", t0=t0, hint=hint)


@mcp.tool()
@shield
async def kill_by_name(name_pattern: str, signal_type: str = "TERM",
                       dry_run: bool = False) -> str:
    """
    Kill all processes whose name matches a pattern (case-insensitive substring).
    - name_pattern: substring to match against process name (e.g. 'python', 'node', 'chrome')
    - signal_type: TERM (graceful) | KILL (force) | INT
    - dry_run: if True, only list what WOULD be killed — no actual kill
    Will never kill the MCP server itself or PID < 10.
    """
    t0 = time.monotonic()
    sig_map = {"TERM": signal.SIGTERM, "KILL": signal.SIGKILL, "INT": signal.SIGINT}
    sig = sig_map.get(signal_type.upper(), signal.SIGTERM)
    own_pid = os.getpid()
    lo = name_pattern.lower()

    candidates = []
    for p in psutil.process_iter(["pid", "name", "cmdline", "status"]):
        try:
            name = (p.info["name"] or "").lower()
            if lo in name and p.pid >= 10 and p.pid != own_pid:
                candidates.append({
                    "pid":    p.pid,
                    "name":   p.info["name"],
                    "status": p.info["status"],
                    "cmd":    " ".join((p.info["cmdline"] or [])[:4])[:100],
                })
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass

    if not candidates:
        return R.ok({"killed": 0, "dry_run": dry_run, "pattern": name_pattern,
                     "message": "No matching processes found."},
                    tool="kill_by_name", t0=t0)

    if dry_run:
        return R.ok({"killed": 0, "dry_run": True, "would_kill": candidates,
                     "pattern": name_pattern},
                    tool="kill_by_name", t0=t0,
                    hint=f"dry_run=True: {len(candidates)} processes would be killed. "
                         "Set dry_run=False to actually kill them.")

    killed, failed = [], []
    for proc in candidates:
        try:
            os.kill(proc["pid"], sig)
            killed.append(proc)
        except (ProcessLookupError, PermissionError) as e:
            proc["error"] = str(e); failed.append(proc)

    return R.ok({
        "killed":  len(killed), "failed": len(failed),
        "signal":  signal_type, "pattern": name_pattern,
        "killed_list": killed, "failed_list": failed,
    }, tool="kill_by_name", t0=t0,
    hint=f"{len(killed)} processes killed." + (f" {len(failed)} failed (check permissions)." if failed else ""))


@mcp.tool()
@shield
async def jsonl_append(path: str, record: str) -> str:
    """
    Append a JSON record as a new line to a JSONL file (newline-delimited JSON).
    Creates file if it doesn't exist.
    record: JSON object string — e.g. '{"event":"step_done","ts":"...","data":{...}}'
    Use for: structured logging, audit trails, streaming results, append-only event stores.
    JSONL is ideal for AI agent logs — each line is a parseable record.
    """
    t0 = time.monotonic()
    try:
        parsed = json.loads(record)
        if not isinstance(parsed, dict):
            raise ValueError("record must be a JSON object (dict), not array/scalar")
    except (json.JSONDecodeError, ValueError) as e:
        return R.error(f"record must be valid JSON object: {e}", tool="jsonl_append",
                       suggestions=["Example: '{\"event\": \"step_done\", \"step\": 3}'"])

    # Auto-inject ts if not present
    if "ts" not in parsed:
        parsed["ts"] = _ts()

    line = json.dumps(parsed, ensure_ascii=False) + "\n"

    def _write():
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, "a", encoding="utf-8") as f:
            f.write(line)
        return os.path.getsize(path)

    total_size = await asyncio.to_thread(_write)
    return R.ok({
        "path":         path,
        "appended":     True,
        "record":       parsed,
        "file_size_b":  total_size,
    }, tool="jsonl_append", t0=t0)


# ─────────────────────────────────────────────────────────────────────────────
#  GROUP 6b — PERMANENT MEMORY (MemStore — SQLite, survives forever)
#
#  Scratch  = working memory. Has TTL. Think: agent's RAM.
#  MemStore = permanent knowledge base. No TTL. Think: agent's HDD.
#
#  Use for: configs, project knowledge, decisions, task history, reference data,
#           plans that outlast a session, credentials notes, architecture docs.
# ─────────────────────────────────────────────────────────────────────────────

@mcp.tool()
@shield
async def mem_set(namespace: str, key: str, value: str,
                  tags: str = "", notes: str = "") -> str:
    """
    ★ Store a value PERMANENTLY in the knowledge base (SQLite — survives server restarts forever).
    namespace: logical grouping — e.g. 'projects', 'config', 'tasks', 'notes', 'servers'
    key:       unique name within namespace — e.g. 'trading_bot_config', 'step1_notes'
    value:     any string or JSON (auto-parsed). Max 512KB.
    tags:      comma-separated labels — e.g. 'important,phase1,gold-trading'
    notes:     human description of what/why — helps AI find it later via mem_search()

    WHEN TO USE mem_set() vs scratch_set():
      mem_set()    → permanent info (configs, decisions, plans, reference data)
      scratch_set()→ temporary working state (retry counters, job IDs, step status)

    Examples:
      mem_set('config',   'mt5_settings',     '{...}',  tags='trading,mt5')
      mem_set('projects', 'qlm_architecture', '...',    notes='Core design doc for QLM project')
      mem_set('tasks',    'phase1_summary',   '...',    tags='completed,phase1')
      mem_set('notes',    'gold_trading_edge', '...',   tags='trading,xauusd,important')
    """
    t0 = time.monotonic()
    if not namespace.strip() or not key.strip():
        return R.error("namespace and key are required.", tool="mem_set")
    try:
        parsed = json.loads(value)
    except Exception:
        parsed = value
    result = await mem.set(namespace.strip(), key.strip(), parsed, tags, notes)
    return R.ok(result, tool="mem_set", t0=t0,
                hint=f"Permanently stored as {namespace}/{key}. "
                     f"Retrieve with mem_get('{namespace}', '{key}'). "
                     f"Find with mem_search('{key.split('_')[0]}').")


@mcp.tool()
@shield
async def mem_get(namespace: str, key: str) -> str:
    """
    Retrieve a permanent record by namespace + key.
    Returns value, tags, notes, created_at, updated_at, access_count.
    """
    t0  = time.monotonic()
    row = await mem.get(namespace.strip(), key.strip())
    if not row:
        return R.ok({
            "found": False, "namespace": namespace, "key": key,
        }, tool="mem_get", t0=t0,
        hint=f"Key '{key}' not in namespace '{namespace}'. "
             f"mem_list('{namespace}') to see keys. mem_search('{key}') to search all namespaces.")
    try:
        value = json.loads(row["value"])
    except Exception:
        value = row["value"]
    return R.ok({
        "found":        True,
        "namespace":    row["namespace"],
        "key":          row["key"],
        "value":        value,
        "value_type":   row["value_type"],
        "tags":         row["tags"],
        "notes":        row["notes"],
        "created_at":   row["created_at"],
        "updated_at":   row["updated_at"],
        "access_count": row["access_count"],
    }, tool="mem_get", t0=t0)


@mcp.tool()
@shield
async def mem_update(namespace: str, key: str, value: str = "",
                     tags: str = "__keep__", notes: str = "__keep__") -> str:
    """
    Update an existing record's value, tags, and/or notes without full overwrite.
    Pass value='' to keep existing value (only update tags/notes).
    Pass tags='__keep__' to keep existing tags (only update value/notes).
    Pass notes='__keep__' to keep existing notes.
    Useful for: adding tags, updating notes, patching a sub-field.
    """
    t0  = time.monotonic()
    row = await mem.get(namespace, key)
    if not row:
        return R.error(f"Record '{namespace}/{key}' not found.",
                       tool="mem_update",
                       suggestions=[f"mem_set('{namespace}', '{key}', ...) to create it first."])
    new_val   = value  if value        else row["value"]
    new_tags  = tags   if tags   != "__keep__" else row["tags"]
    new_notes = notes  if notes  != "__keep__" else row["notes"]
    try:
        parsed = json.loads(new_val)
    except Exception:
        parsed = new_val
    result = await mem.set(namespace, key, parsed, new_tags, new_notes)
    result["action"] = "updated"
    return R.ok(result, tool="mem_update", t0=t0)


@mcp.tool()
@shield
async def mem_delete(namespace: str, key: str = "") -> str:
    """
    Delete a record or an entire namespace.
    key='' deletes ALL records in the namespace (use carefully!).
    Returns count of deleted records.
    """
    t0 = time.monotonic()
    if not namespace.strip():
        return R.error("namespace is required.", tool="mem_delete")
    deleted = await mem.delete(namespace.strip(), key.strip())
    scope   = f"{namespace}/{key}" if key else f"{namespace}/* (entire namespace)"
    return R.ok({
        "deleted": deleted, "scope": scope,
        "warning": "This is permanent — records cannot be recovered." if deleted > 0 else None,
    }, tool="mem_delete", t0=t0,
    hint=f"{deleted} record(s) permanently deleted." if deleted else f"Nothing found to delete at '{scope}'.")


@mcp.tool()
@shield
async def mem_list(namespace: str = "", key_prefix: str = "",
                   tags: str = "", limit: int = 100) -> str:
    """
    List permanent records — with optional filters.
    namespace:  filter to one namespace (empty = all namespaces)
    key_prefix: only keys starting with this prefix
    tags:       comma-separated tags to filter by (e.g. 'important,phase1')
    limit:      max results (default 100, max 500)
    Returns: key, value_type, tags, notes, created_at, updated_at (NOT full values — use mem_get for value).
    Context-efficient: shows metadata only, not full values.
    """
    t0    = time.monotonic()
    limit = min(limit, 500)
    rows  = await mem.list(namespace.strip(), key_prefix.strip(), tags.strip(), limit)
    return R.ok({
        "count":     len(rows),
        "namespace": namespace or "(all)",
        "filters":   {"key_prefix": key_prefix, "tags": tags},
        "records":   rows,
        "note":      "Values not shown. Use mem_get(namespace, key) for full value.",
    }, tool="mem_list", t0=t0,
    hint=f"{len(rows)} records found. mem_get(namespace, key) to read value. "
         f"mem_search(query) to find by content.")


@mcp.tool()
@shield
async def mem_search(query: str, namespace: str = "", limit: int = 30) -> str:
    """
    ★ Full-text search across ALL permanent records.
    Searches in: key names, JSON values, tags, and notes.
    namespace: restrict to one namespace (empty = search all)
    limit: max results (default 30)

    Use this to find records when you don't know the exact namespace/key.
    Example: mem_search('gold trading') — finds any record mentioning gold trading
    Example: mem_search('phase1', namespace='tasks') — all phase1 tasks
    """
    t0   = time.monotonic()
    if not query.strip():
        return R.error("query is required.", tool="mem_search")
    rows = await mem.search(query.strip(), namespace.strip(), limit)
    return R.ok({
        "query":     query,
        "namespace": namespace or "(all)",
        "count":     len(rows),
        "results":   rows,
        "note":      "Use mem_get(namespace, key) to retrieve full value.",
    }, tool="mem_search", t0=t0,
    hint=f"{len(rows)} matches for '{query}'. mem_get(namespace, key) to read value.")


@mcp.tool()
@shield
async def mem_namespaces() -> str:
    """
    ★ List all namespaces in permanent store with record counts.
    Quick overview of what knowledge is stored.
    Call this at session start to understand available permanent data.
    """
    t0   = time.monotonic()
    rows = await mem.namespaces()
    st   = await mem.stats()
    return R.ok({
        "namespaces":    rows,
        "total_records": st["total_records"],
        "db_size_kb":    round(st["db_size_bytes"] / 1024, 1),
    }, tool="mem_namespaces", t0=t0,
    hint="Use mem_list(namespace='...') to see keys. mem_search('query') to find specific records.")


# ─────────────────────────────────────────────────────────────────────────────
#  GROUP 7 — AI MEMORY / SCRATCHPAD
# ─────────────────────────────────────────────────────────────────────────────

@mcp.tool()
@shield
async def scratch_set(key: str, value: str, ttl_minutes: int = 0) -> str:
    """
    ★ Persist a value to AI scratchpad (survives restarts).
    key: MUST use category prefix (see categories below). No prefix = permanent.
    value: any string or JSON. Capped at 32KB. JSON auto-parsed and stored structured.
    ttl_minutes: override auto-TTL (0 = use category default).

    CATEGORY PREFIXES (auto-TTL applied):
      session: → 2h   | Active session IDs
      cache:   → 30m  | Listings, computed values (apps list, etc.)
      tmp:     → 10m  | Intermediate step data
      plan:    → 12h  | Task plans and step tracking
      job:     → 4h   | Background job IDs
      retry:   → 1h   | Retry counters
      result:  → 24h  | Task results and summaries
      flag:    → 6h   | Boolean signals
      state:   → 6h   | Component state
      config:  → perm | Config that should survive forever
      perm:    → perm | Intentionally permanent data

    AI DISCIPLINE: Do NOT store trivial/transient data permanently.
    Use cache: for listings. Use tmp: for intermediate. Use result: for outputs.
    """
    t0 = time.monotonic()
    try:
        parsed = json.loads(value)
    except Exception:
        parsed = value
    result = await scratch.set(key, parsed, ttl_minutes)
    return R.ok(result, tool="scratch_set", t0=t0,
                hint=f"Stored under '{result['ttl_source']}' TTL policy. "
                     f"Expires: {result['expires_at'] or 'never'}.")


@mcp.tool()
@shield
async def scratch_get(key: str) -> str:
    """Retrieve a value from scratchpad by key. Returns found=false if expired or missing."""
    t0      = time.monotonic()
    ok, val = scratch.get(key)
    if not ok:
        return R.ok({"key": key, "found": False, "value": None}, tool="scratch_get", t0=t0,
                    hint=f"Key '{key}' not found or expired. scratch_summary() to see all active keys.")
    return R.ok({"key": key, "found": True, "value": val}, tool="scratch_get", t0=t0)


@mcp.tool()
@shield
async def scratch_summary() -> str:
    """
    ★ PREFERRED over scratch_list for orientation.
    Returns compact category breakdown: count, size, permanent count per category.
    Use this to understand memory state WITHOUT reading all values.
    Context-window efficient — single small response shows full memory picture.
    """
    t0  = time.monotonic()
    s   = scratch.summary()
    return R.ok(s, tool="scratch_summary", t0=t0,
                hint=f"Memory {s['pct_full']}% full ({s['total_keys']}/{s['max_keys']} keys). "
                     "scratch_list(prefix='plan:') to see specific category keys.")


@mcp.tool()
@shield
async def scratch_list(prefix: str = "", include_meta: bool = False) -> str:
    """
    List scratchpad keys, optionally filtered by prefix.
    include_meta=True: shows ttl, set_at, size_chars per key.
    For overview: use scratch_summary() — much smaller response.
    For specific category: scratch_list(prefix='plan:')
    """
    t0   = time.monotonic()
    keys = scratch.list_keys(prefix, include_meta)
    return R.ok({"count": len(keys), "keys": keys, "prefix": prefix},
                tool="scratch_list", t0=t0,
                hint="Use scratch_summary() for full memory overview. "
                     "Use prefix= to filter to specific category.")


@mcp.tool()
@shield
async def scratch_delete(key: str) -> str:
    """Delete a key from scratchpad."""
    t0 = time.monotonic()
    ok = await scratch.delete(key)
    return R.ok({"deleted": ok, "key": key}, tool="scratch_delete", t0=t0)


@mcp.tool()
@shield
async def scratch_gc() -> str:
    """
    ★ Garbage collect: purge ALL expired keys from scratchpad.
    Run this after completing a task phase to keep memory clean.
    Also run if scratch_summary() shows pct_full > 70%.
    Returns count of keys purged and new memory state.
    """
    t0     = time.monotonic()
    purged = await scratch._purge_expired()
    s      = scratch.summary()
    return R.ok({
        "purged": purged,
        "remaining": s["total_keys"],
        "pct_full": s["pct_full"],
        "categories": s["categories"],
    }, tool="scratch_gc", t0=t0,
    hint=f"Purged {purged} expired keys. Memory now {s['pct_full']}% full.")


@mcp.tool()
@shield
async def scratch_clear(prefix: str = "") -> str:
    """
    Clear scratchpad keys by prefix. Empty prefix = clears ALL (use carefully!).
    Prefer scratch_gc() for routine cleanup — it only removes expired keys.
    Use this for: clearing a specific task's namespace after completion.
    Example: scratch_clear(prefix='retry:') clears all retry counters.
    """
    t0  = time.monotonic()
    cnt = await scratch.clear(prefix)
    return R.ok({"cleared": cnt, "prefix": prefix or "(all)"},
                tool="scratch_clear", t0=t0,
                hint="scratch_gc() is safer — only removes expired keys.")


# ─────────────────────────────────────────────────────────────────────────────
#  GROUP 8 — INVESTIGATION & FORENSICS
# ─────────────────────────────────────────────────────────────────────────────

@mcp.tool()
@shield
async def ledger_query(tool: str = "", status: str = "", tag: str = "",
                        search: str = "", last_n: int = 50) -> str:
    """
    ★ Query the execution ledger — your complete action history.
    - tool: filter by tool name (e.g. 'shell_exec')
    - status: ok | error | circuit_open | partial
    - tag: filter by tag
    - search: text search in summary
    - last_n: max results
    Use to: understand what happened before, find failing patterns, replay context.
    """
    t0      = time.monotonic()
    entries = ledger.query(tool=tool, status=status, tag=tag, last_n=last_n, search=search)
    return R.ok({"count": len(entries), "filter": {"tool": tool, "status": status,
                 "tag": tag, "search": search}, "entries": entries},
                tool="ledger_query", t0=t0,
                hint="See ledger_stats() for per-tool success rates.")


@mcp.tool()
@shield
async def ledger_stats() -> str:
    """
    Aggregated stats per tool: call count, success/error rate, avg duration.
    Use to identify: which tools fail most, slowest tools, usage patterns.
    """
    t0    = time.monotonic()
    stats = ledger.stats()
    return R.ok(stats, tool="ledger_stats", t0=t0)


@mcp.tool()
@shield
async def error_inspect(trace_id: str = "", tool: str = "",
                         last_n: int = 10) -> str:
    """
    ★ Full forensic error detail.
    - trace_id: get specific error by trace ID (from on_error.trace_id)
    - tool: get recent errors for a specific tool
    - last_n: if no trace_id, return last N errors
    Returns: full stack trace, inputs that caused the error, timestamp.
    """
    t0 = time.monotonic()
    if trace_id:
        errors = err_store.recent(200)
        matched = [e for e in errors if e.get("trace_id") == trace_id]
        if not matched:
            return R.error(f"trace_id '{trace_id}' not found.",
                           tool="error_inspect",
                           suggestions=["error_inspect(last_n=20) to see recent errors.",
                                        "ledger_query(status='error') to find trace IDs."])
        return R.ok({"error": matched[0]}, tool="error_inspect", t0=t0)
    errors = err_store.recent(last_n, tool=tool)
    return R.ok({"count": len(errors), "filter_tool": tool, "errors": errors},
                tool="error_inspect", t0=t0)


@mcp.tool()
@shield
async def probe(target: str = "full") -> str:
    """
    ★ Safe environment probe — inspects without changing anything.
    target: full | tools | network | storage | processes | python | node
    Use before acting: understand what's available, what's installed, what's running.
    """
    t0 = time.monotonic()
    result: Dict[str, Any] = {}

    if target in ("full", "tools"):
        # Check what tools are installed
        tools_to_check = ["python3", "python", "node", "npm", "git", "docker",
                          "curl", "wget", "jq", "make", "gcc", "pip", "pip3",
                          "pwsh", "bash", "zsh", "systemctl", "service"]
        result["installed_tools"] = {
            t: shutil.which(t) or "NOT FOUND"
            for t in tools_to_check
        }

    if target in ("full", "python"):
        r = await _run("python3 -c \"import sys; import pkg_resources; pkgs=[(d.project_name,d.version) for d in pkg_resources.working_set]; print(sys.version); print(len(pkgs))\"",
                       shell=CFG["linux_shell"], timeout=10)
        result["python"] = {"version": r.get("stdout",""), "error": r.get("stderr")}

    if target in ("full", "storage"):
        partitions = []
        for p in psutil.disk_partitions():
            try:
                u = psutil.disk_usage(p.mountpoint)
                partitions.append({"mount": p.mountpoint, "device": p.device,
                                   "total_gb": round(u.total/1024**3,2),
                                   "free_gb": round(u.free/1024**3,2), "pct": u.percent})
            except Exception: pass
        result["storage"] = partitions

    if target in ("full", "network"):
        result["network"] = {
            "hostname": socket.gethostname(),
            "local_ip": get_local_ip(),
            "interfaces": list(psutil.net_if_addrs().keys()),
        }

    if target in ("full", "processes"):
        result["top_cpu"] = sorted([
            {"pid": p.pid, "name": p.info["name"],
             "cpu": round(p.info["cpu_percent"] or 0, 1)}
            for p in psutil.process_iter(["name","cpu_percent"])
        ], key=lambda x: x["cpu"], reverse=True)[:5]

    return R.ok(result, tool="probe", t0=t0,
                hint="Use this data to plan what commands to run. "
                     "Check installed_tools before running tool-specific commands.")


@mcp.tool()
@shield
async def diff_exec(command: str, baseline_key: str = "",
                    timeout: int = 30) -> str:
    """
    Run a command and diff its output against a stored baseline.
    If baseline_key is empty: stores result as baseline in scratchpad.
    If baseline_key exists: runs command again and shows diff.
    Use for: verify a change actually happened, compare before/after state.
    Example: diff_exec('Get-Service | ConvertTo-Json', 'services_baseline')
    """
    t0  = time.monotonic()
    r   = await _run(command, timeout=timeout)
    out = r.get("stdout", "")

    sk = f"diff_baseline:{baseline_key}" if baseline_key else f"diff_baseline:{uuid.uuid4().hex[:8]}"
    found, baseline = scratch.get(sk)

    if not found:
        await scratch.set(sk, out)
        return R.ok({
            "mode": "stored_baseline", "key": sk,
            "output": out[:3000],
        }, tool="diff_exec", t0=t0,
        hint=f"Baseline stored as '{sk}'. Run diff_exec again with same baseline_key to compare.")

    # Compute diff
    old_lines = baseline.splitlines(keepends=True) if isinstance(baseline, str) else []
    new_lines = out.splitlines(keepends=True)
    diff = list(difflib.unified_diff(old_lines, new_lines, fromfile="baseline", tofile="current", n=3))

    return R.ok({
        "mode": "diff",
        "baseline_key": sk,
        "changed": len(diff) > 0,
        "additions": len([l for l in diff if l.startswith("+") and not l.startswith("+++")]),
        "removals":  len([l for l in diff if l.startswith("-") and not l.startswith("---")]),
        "diff": "".join(diff[:200]),   # First 200 diff lines
    }, tool="diff_exec", t0=t0,
    hint="changed=False means output is identical to baseline." if len(diff) == 0
    else f"{len(diff)} diff lines found. Review additions/removals.")


@mcp.tool()
@shield
async def watch_until(condition_cmd: str, success_pattern: str,
                      poll_interval_s: float = 2.0, timeout_s: int = 60,
                      shell: str = "") -> str:
    """
    ★ Poll a command until output matches a pattern (or timeout).
    Use for: wait for service to start, port to open, file to appear, process to die.
    - condition_cmd: command to run repeatedly
    - success_pattern: regex — when matched in stdout, watch succeeds
    - poll_interval_s: seconds between polls (default 2)
    - timeout_s: max total wait (default 60)
    Example: watch_until('Get-Service nginx | Select Status', 'Running', timeout_s=30)
    """
    t0 = time.monotonic()
    sh = CFG["linux_shell"] if shell == "bash" else SHELL_CMD
    try: rx = re.compile(success_pattern, re.IGNORECASE)
    except re.error as e:
        return R.error(f"Invalid success_pattern regex: {e}", tool="watch_until")

    polls = 0
    out   = ""      # ensure defined even if loop never runs (timeout_s=0)
    while time.monotonic() - t0 < timeout_s:
        r = await _run(condition_cmd, shell=sh, timeout=int(poll_interval_s * 2) + 5)
        out = r.get("stdout", "")
        polls += 1
        if rx.search(out):
            return R.ok({
                "matched": True, "polls": polls,
                "elapsed_s": round(time.monotonic() - t0, 1),
                "matched_output": out[:500],
            }, tool="watch_until", t0=t0,
            hint="Condition met. Proceed with next steps.")
        await asyncio.sleep(poll_interval_s)

    return R.ok({
        "matched": False, "polls": polls,
        "elapsed_s": round(time.monotonic() - t0, 1),
        "last_output": out[:500],
        "timed_out": True,
    }, tool="watch_until", t0=t0,
    hint=f"Timeout after {timeout_s}s ({polls} polls). Condition '{success_pattern}' not matched. "
         "Consider increasing timeout_s or checking if the service/resource is actually starting.")


# ─────────────────────────────────────────────────────────────────────────────
#  GROUP 9 — SERVER META
# ─────────────────────────────────────────────────────────────────────────────

@mcp.tool()
def ping() -> str:
    """Liveness check. Returns version, uptime, platform."""
    return json.dumps({
        "status": "alive", "version": __version__, "codename": __codename__,
        "uptime_s": round(time.time() - START_TIME, 1),
        "platform": platform.system(), "shell": SHELL_CMD, "ts": _ts(),
    })


@mcp.tool()
@shield
async def server_status() -> str:
    """
    Full server health report: resources, jobs, sessions, circuits, ledger stats.
    """
    t0  = time.monotonic()
    mem = psutil.virtual_memory()
    srv = psutil.Process(os.getpid())

    job_counts: Dict[str, int] = {}
    for j in jm.jobs.values():
        job_counts[j.status] = job_counts.get(j.status, 0) + 1

    return R.ok({
        "server": {
            "version": __version__, "codename": __codename__,
            "pid": os.getpid(), "uptime_s": round(time.time() - START_TIME, 1),
            "data_dir": str(DATA_DIR),
        },
        "resources": {
            "server_cpu_pct": srv.cpu_percent(interval=0.1),
            "server_ram_mb": round(srv.memory_info().rss / 1024**2, 1),
            "system_ram_pct": mem.percent,
            "system_ram_free_gb": round(mem.available / 1024**3, 2),
        },
        "jobs":      {"total": len(jm.jobs), "by_status": job_counts},
        "sessions":  {"count": len(sm.sessions), "list": sm.list_all()},
        "ledger":    {"total_entries": len(ledger._mem)},
        "scratchpad":{"keys": len(scratch._data)},
        "circuits":  {n: {"failures": s["f"], "open": s["open"]}
                      for n, s in _CB.items() if s["f"] > 0},
        "security":  {
            "auth_enabled": bool(CFG["api_key"] and CFG["require_auth"]),
            "ip_allowlist": bool(CFG["allowed_ips"]),
        },
    }, tool="server_status", t0=t0)


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def get_local_ip() -> str:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80)); ip = s.getsockname()[0]; s.close(); return ip
    except Exception: return "127.0.0.1"


# ══════════════════════════════════════════════════════════════════════════════
# GRACEFUL SHUTDOWN
# ══════════════════════════════════════════════════════════════════════════════

async def graceful_shutdown():
    logger.info("Graceful shutdown ...")
    for job in [j for j in jm.jobs.values() if not j.is_terminal]:
        try:
            if job.process: job.process.terminate()
            job.status = "stopped"; job.end = _ts()
        except Exception: pass
    for sid in list(sm.sessions.keys()):
        try: await sm.kill(sid)
        except Exception: pass
    await jm._save()
    logger.info("Shutdown complete.")


def _sig(sig, _):
    """Signal handler — schedule graceful shutdown on the running event loop."""
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(graceful_shutdown())
    except RuntimeError:
        logger.info("Shutdown signal received.")
        sys.exit(0)


# ══════════════════════════════════════════════════════════════════════════════
# HEALTH SUPERVISOR
# ══════════════════════════════════════════════════════════════════════════════

def start_supervisor(loop: asyncio.AbstractEventLoop):
    """Background health supervisor — all async work dispatched to main event loop."""
    def _run_coro(coro, timeout_s: int = 15):
        fut = asyncio.run_coroutine_threadsafe(coro, loop)
        try:   return fut.result(timeout=timeout_s)
        except Exception as e: logger.error(f"Supervisor task error: {e}"); return None

    def _loop():
        i = 0
        while True:
            time.sleep(60); i += 1
            try:
                running = sum(1 for j in jm.jobs.values() if not j.is_terminal)
                smry    = scratch.summary()
                mem_st  = mem._stats_sync()
                logger.info(
                    f"💓 #{i} | jobs:{len(jm.jobs)}({running} run) | "
                    f"sess:{len(sm.sessions)} | "
                    f"scratch:{smry['total_keys']}keys/{smry['pct_full']}% | "
                    f"mem:{mem_st['total_records']}recs | "
                    f"uptime:{round((time.time()-START_TIME)/60,1)}m"
                )
                # Scratch GC every 5 min
                if i % 5 == 0:
                    purged = _run_coro(scratch._purge_expired())
                    if purged:
                        logger.info(f"🧹 Scratch GC: {purged} expired keys removed")

                # Session GC: kill dead sessions every 10 min
                if i % 10 == 0:
                    dead = [sid for sid, s in list(sm.sessions.items()) if not s.alive]
                    for sid in dead:
                        _run_coro(sm.kill(sid), timeout_s=5)
                    if dead:
                        logger.info(f"🧹 Session GC: removed {len(dead)} dead sessions")

                # Job TTL purge every 15 min
                if i % 15 == 0:
                    purged_j = _run_coro(jm.purge(older_h=CFG["job_ttl_h"]))
                    if purged_j:
                        logger.info(f"🧹 Job GC: removed {purged_j} old jobs")

            except Exception as e:
                logger.error(f"Supervisor: {e}")
    threading.Thread(target=_loop, daemon=True, name="Supervisor").start()


# ══════════════════════════════════════════════════════════════════════════════
# STARTUP CHECKS
# ══════════════════════════════════════════════════════════════════════════════

def self_test():
    ok = True
    if not shutil.which(SHELL_CMD):
        logger.error(f"❌ Shell '{SHELL_CMD}' not in PATH!"); ok = False
    else:
        logger.info(f"✅ Shell: {SHELL_CMD}")
    try:
        (DATA_DIR / ".wtest").write_text("ok"); (DATA_DIR / ".wtest").unlink()
        logger.info(f"✅ Data dir: {DATA_DIR}")
    except Exception as e:
        logger.error(f"❌ Data dir not writable: {e}"); ok = False
    # Disk space check — warn if < 500MB
    try:
        free_gb = shutil.disk_usage(DATA_DIR).free / 1024**3
        if free_gb < 0.5:
            logger.warning(f"⚠️  Low disk: {free_gb:.2f} GB free — write operations may fail")
        else:
            logger.info(f"✅ Disk: {free_gb:.1f} GB free")
    except Exception: pass
    if not CFG["api_key"]:
        logger.warning("⚠️  No API_KEY — server is UNAUTHENTICATED (OK for localhost dev)")
    # Verify MemStore DB
    try:
        s = mem._stats_sync()
        logger.info(f"✅ MemStore: {s['total_records']} records in {s['namespaces']} namespaces")
    except Exception as e:
        logger.error(f"❌ MemStore init error: {e}"); ok = False
    return ok


# ── Disk guard helper ─────────────────────────────────────────────────────────
def _disk_ok(min_mb: int = 50) -> bool:
    try:
        return shutil.disk_usage(DATA_DIR).free > min_mb * 1024 * 1024
    except Exception:
        return True   # don't block if check fails


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    global START_TIME; START_TIME = time.time()
    self_test()
    signal.signal(signal.SIGTERM, _sig)
    signal.signal(signal.SIGINT,  _sig)

    lip = get_local_ip()

    logger.info("═" * 66)
    logger.info(f"  🤖  GODMODE MCP  v{__version__}  ·  {__codename__}")
    logger.info("═" * 66)
    logger.info(f"  Endpoint : http://{lip}:{CFG['port']}")
    logger.info(f"  Platform : {platform.system()} / {SHELL_CMD}")
    logger.info(f"  Data     : {DATA_DIR}")
    logger.info(f"  MemStore : {MEM_FILE}")
    logger.info(f"  Auth     : {'✅ API_KEY set' if CFG['api_key'] else '⚠️  NONE (localhost only)'}")
    logger.info(f"  Tools    : 70 registered")
    logger.info("─" * 66)
    logger.info("  AI STARTUP SEQUENCE:")
    logger.info("  1. ai_init()         → environment + memory snapshot")
    logger.info("  2. scratch_summary() → working memory state")
    logger.info("  3. mem_namespaces()  → permanent store overview")
    logger.info("  4. tool_guide()      → discipline rules + patterns")
    logger.info("═" * 66)

    async def _run_server():
        loop = asyncio.get_running_loop()

        # Global async exception handler — catches all unhandled task errors
        def _exc_handler(lp, ctx):
            msg = ctx.get("exception", ctx.get("message", "unknown"))
            task = ctx.get("task")
            tname = task.get_name() if task else "?"
            logger.error(f"🔥 Unhandled async exception in task '{tname}': {msg}")

        loop.set_exception_handler(_exc_handler)

        # Start supervisor with main loop reference (fixes BUG-5)
        start_supervisor(loop)

        async def _health(req: Request):
            ms = mem._stats_sync()
            return JSONResponse({
                "status":    "alive",
                "version":   __version__,
                "uptime_s":  round(time.time() - START_TIME, 1),
                "jobs":      sum(1 for j in jm.jobs.values() if not j.is_terminal),
                "sessions":  len(sm.sessions),
                "mem_records": ms["total_records"],
            })

        app = Starlette(
            middleware=[
                Middleware(CORSMiddleware, allow_origins=["*"],
                           allow_methods=["*"], allow_headers=["*"]),
                Middleware(AuthMiddleware),
            ],
            routes=[Route("/health", _health)],
        )
        app.mount("/", mcp.sse_app())

        ssl_kw: Dict = {}
        if CFG["tls_cert"] and CFG["tls_key"]:
            ssl_kw = {"ssl_certfile": CFG["tls_cert"], "ssl_keyfile": CFG["tls_key"]}

        config = uvicorn.Config(app, host=CFG["host"], port=CFG["port"],
                                loop="none", **ssl_kw)
        server = uvicorn.Server(config)
        await server.serve()

    asyncio.run(_run_server())


if __name__ == "__main__":
    main()
