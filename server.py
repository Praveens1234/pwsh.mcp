import os
import sys
import time
import shutil
import asyncio
import logging
import subprocess
import threading
import json
import uuid
import platform
import signal
import socket
import traceback
import functools
import re
import psutil
from datetime import datetime
from typing import Dict, List, Optional, Union, Any
from contextlib import asynccontextmanager
from logging.handlers import RotatingFileHandler

import uvicorn
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware

# CRITICAL: Check for file naming conflict before importing mcp
if os.path.basename(__file__).lower() in ["mcp.py", "mcp.pyw"]:
    print("\n" + "="*60)
    print(" [!] CRITICAL ERROR: FILE NAME CONFLICT DETECTED")
    print("="*60)
    print(" You have named this script 'mcp.py'. This conflicts with the 'mcp'")
    print(" Python library required to run the server.")
    print("\n SOLUTION:")
    print(" 1. Rename this file to 'server.py' or 'god_mode.py'.")
    print(" 2. Run it again.")
    print("="*60 + "\n")
    sys.exit(1)

try:
    from mcp.server.fastmcp import FastMCP
except ImportError as e:
    print(f"\n[!] Failed to import 'mcp': {e}")
    print("    Please ensure 'mcp' is installed: pip install mcp")
    sys.exit(1)

# ==================================================================================
# CONFIGURATION & LOGGING
# ==================================================================================

IS_WINDOWS = platform.system() == "Windows"

# Detect Shell
SHELL_CMD = "pwsh"
if IS_WINDOWS:
    if not shutil.which("pwsh"):
        SHELL_CMD = "powershell" # Fallback to Windows PowerShell
else:
    # Linux/Mac - assume pwsh is installed as per setup
    pass

LOG_FILE = "server.log"
HISTORY_FILE = "history.log"
JOBS_FILE = "jobs.json"
ERROR_FILE = "error.log"
PORT = int(os.environ.get("PORT", 8000))
HOST = os.environ.get("HOST", "0.0.0.0")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        RotatingFileHandler(LOG_FILE, maxBytes=10*1024*1024, backupCount=3, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("GodMode")

# ==================================================================================
# PERSISTENCE LAYER
# ==================================================================================

class StateManager:
    def __init__(self, jobs_file: str = JOBS_FILE, history_file: str = HISTORY_FILE):
        self.jobs_file = jobs_file
        self.history_file = history_file
        self.lock = threading.RLock()
        
        # Ensure files exist
        if not os.path.exists(self.jobs_file):
            self.save_jobs({})
        if not os.path.exists(self.history_file):
            with open(self.history_file, "w", encoding="utf-8") as f:
                f.write(f"[{datetime.now().isoformat()}] History log initialized.\n")

    def load_jobs(self) -> Dict[str, Any]:
        with self.lock:
            if not os.path.exists(self.jobs_file):
                return {}
            try:
                with open(self.jobs_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                # Backup corrupt file
                if os.path.exists(self.jobs_file):
                    shutil.copy(self.jobs_file, f"{self.jobs_file}.corrupt")
                return {}

    def save_jobs(self, jobs: Dict[str, Any]):
        with self.lock:
            temp_file = f"{self.jobs_file}.tmp"
            try:
                with open(temp_file, "w", encoding="utf-8") as f:
                    json.dump(jobs, f, indent=2, ensure_ascii=False)
                os.replace(temp_file, self.jobs_file)
            except Exception as e:
                logger.error(f"Failed to save jobs: {e}")
                if os.path.exists(temp_file):
                    os.remove(temp_file)

    def append_history(self, action: str, details: str):
        timestamp = datetime.now().isoformat()
        entry = f"[{timestamp}] [{action}] {details}\n"
        with self.lock:
            try:
                with open(self.history_file, "a", encoding="utf-8") as f:
                    f.write(entry)
            except Exception:
                pass

state_manager = StateManager()

# ==================================================================================
# UTILITIES
# ==================================================================================

BREAKER_STATE = {} # {func_name: {"failures": 0, "last_fail": timestamp}}
BREAKER_THRESHOLD = 5
BREAKER_COOLDOWN = 60 # seconds

def global_shield(func):
    """Decorator for Crash Protection, Circuit Breaking, and Error Logging."""
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        name = func.__name__
        state = BREAKER_STATE.get(name, {"failures": 0, "last_fail": 0})
        
        # Check breaker
        if state["failures"] >= BREAKER_THRESHOLD:
            if time.time() - state["last_fail"] < BREAKER_COOLDOWN:
                return f"ERROR: Tool '{name}' is temporarily disabled due to repeated failures (Circuit Open)."
            else:
                # Reset after cooldown
                state["failures"] = 0
                
        try:
            res = await func(*args, **kwargs)
            # Success resets failure count
            state["failures"] = 0
            BREAKER_STATE[name] = state
            return res
        except Exception as e:
            state["failures"] += 1
            state["last_fail"] = time.time()
            BREAKER_STATE[name] = state
            
            tb = traceback.format_exc()
            logger.error(f"CRITICAL ERROR in {name}: {e}\n{tb}")
            timestamp = datetime.now().isoformat()
            try:
                with open(ERROR_FILE, "a", encoding="utf-8") as f:
                    f.write(f"[{timestamp}] ERROR in {name}: {e}\n{tb}\n{'-'*40}\n")
            except: pass
            return f"ERROR: Action failed safely.\nException: {str(e)}"
    return wrapper

def get_local_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"

def strip_ansi(text: str) -> str:
    """Remove ANSI escape sequences from string."""
    ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
    return ansi_escape.sub('', text)

def escape_ps(text: str) -> str:
    """Escape single quotes for PowerShell string interpolation."""
    return text.replace("'", "''")

# ==================================================================================
# CORE LOGIC: JOBS & SESSIONS
# ==================================================================================

class Job:
    def __init__(self, job_id: str, command: str, pid: Optional[int], 
                 log_file: str, status: str = "running", 
                 start_time: str = None, exit_code: Optional[int] = None):
        self.id = job_id
        self.command = command
        self.pid = pid
        self.log_file = log_file
        self.status = status
        self.start_time = start_time or datetime.now().isoformat()
        self.exit_code = exit_code
        self.process = None

    def to_dict(self):
        return {
            "id": self.id,
            "command": self.command,
            "pid": self.pid,
            "log_file": self.log_file,
            "status": self.status,
            "start_time": self.start_time,
            "exit_code": self.exit_code
        }

    @classmethod
    def from_dict(cls, data):
        return cls(
            job_id=data["id"],
            command=data["command"],
            pid=data.get("pid"),
            log_file=data["log_file"],
            status=data["status"],
            start_time=data["start_time"],
            exit_code=data.get("exit_code")
        )

class AsyncJobManager:
    def __init__(self, mgr: StateManager):
        self.jobs: Dict[str, Job] = {}
        self.state_manager = mgr
        self.concurrency_limit = asyncio.Semaphore(10) # Throttling
        self._load_from_state()

    def _load_from_state(self):
        data = self.state_manager.load_jobs()
        for j_id, j_data in data.items():
            self.jobs[j_id] = Job.from_dict(j_data)
        self.reconcile()

    def reconcile(self):
        logger.info("Reconciling jobs...")
        dirty = False
        # Create a thread-safe copy for iteration
        current_jobs = list(self.jobs.values())
        for job in current_jobs:
            if job.status == "running":
                # Check if PID exists and is valid
                if job.pid and psutil.pid_exists(job.pid):
                    try:
                        proc = psutil.Process(job.pid)
                        if proc.status() in [psutil.STATUS_ZOMBIE, psutil.STATUS_DEAD]:
                             job.status = "failed"
                             job.exit_code = -1
                             dirty = True
                        else:
                            # Verify it might be our pwsh process? 
                            # Hard to be 100% sure without PID namespaces, but this is best effort.
                            logger.info(f"Job {job.id} (PID {job.pid}) recovered.")
                    except psutil.NoSuchProcess:
                        job.status = "lost"
                        dirty = True
                else:
                    job.status = "lost"
                    dirty = True
        if dirty:
            self.save()

    def save(self):
        self.state_manager.save_jobs({j.id: j.to_dict() for j in self.jobs.values()})

    async def start_job(self, command: str, timeout: int = 0) -> str:
        async with self.concurrency_limit:
            job_id = str(uuid.uuid4())[:8]
            log_file = f"job_{job_id}.log"
            
            # Wrapping in a script block for safety/encoding
            cmd = [SHELL_CMD, "-Command", command]
            
            f = open(log_file, "w", encoding="utf-8")
            try:
                process = await asyncio.create_subprocess_exec(
                    *cmd,
                    stdout=f,
                    stderr=asyncio.subprocess.STDOUT,
                    start_new_session=True # Detach
                )
                
                job = Job(job_id, command, process.pid, log_file)
                job.process = process
                self.jobs[job_id] = job
                await asyncio.to_thread(self.save)
                
                await asyncio.to_thread(state_manager.append_history, "JOB_START", f"ID: {job_id} CMD: {command}")
                
                asyncio.create_task(self._monitor_job(job, f, timeout))
                return job_id
            except Exception as e:
                f.close()
                raise e

    async def _monitor_job(self, job: Job, file_handle, timeout: int):
        try:
            if timeout > 0:
                await asyncio.wait_for(job.process.wait(), timeout=timeout)
            else:
                await job.process.wait()
            job.exit_code = job.process.returncode
            job.status = "completed" if job.exit_code == 0 else "failed"
        except asyncio.TimeoutError:
            logger.warning(f"Job {job.id} timed out. Killing.")
            try:
                job.process.kill()
            except: pass
            job.status = "timed_out"
        except Exception as e:
            logger.error(f"Error monitoring job {job.id}: {e}")
            job.status = "error"
        finally:
            file_handle.close()
            await asyncio.to_thread(self.save)

    async def stop_job(self, job_id: str) -> str:
        job = self.jobs.get(job_id)
        if not job: return "Job not found."
        
        if job.status not in ["running"]:
            return f"Job {job_id} is {job.status}."

        killed = False
        if job.process:
            try:
                job.process.terminate()
                # Give it 2 seconds
                try:
                    await asyncio.wait_for(job.process.wait(), timeout=2.0)
                except asyncio.TimeoutError:
                    job.process.kill()
                killed = True
            except ProcessLookupError:
                pass
        elif job.pid:
            # Reconciled job
            try:
                os.kill(job.pid, signal.SIGTERM)
                killed = True
            except OSError:
                pass

        job.status = "stopped"
        self.save()
        state_manager.append_history("JOB_STOP", f"ID: {job_id}")
        return f"Job {job_id} stopped."

    def list_jobs(self) -> List[Dict]:
        return [j.to_dict() for j in self.jobs.values()]

    def get_job_log(self, job_id: str, tail: int = 0, keyword: str = "") -> str:
        job = self.jobs.get(job_id)
        if not job: return "Job not found."
        if not os.path.exists(job.log_file): return "Log file not found."
        
        try:
            # If standard read
            if tail == 0 and not keyword:
                with open(job.log_file, "r", encoding="utf-8", errors="replace") as f:
                    return f.read()
            
            # Smart read using PowerShell for filtering
            ps_cmd = f"Get-Content -Path '{job.log_file}'"
            if tail > 0:
                ps_cmd += f" -Tail {tail}"
            if keyword:
                ps_cmd += f" | Select-String -Pattern '{keyword}'"
            
            # Run sync (wrapped in shell)
            out = subprocess.check_output([SHELL_CMD, "-Command", ps_cmd], timeout=5).decode("utf-8", errors="replace")
            return out
        except Exception as e:
            return f"Error reading log: {e}"

    async def run_smart(self, command: str, wait: int = 3, timeout: int = 0) -> Dict[str, Any]:
        """Run a job, waiting briefly for completion."""
        job_id = await self.start_job(command, timeout=timeout)
        job = self.jobs[job_id]
        
        # Poll
        steps = wait * 5
        for _ in range(steps):
            if job.status in ["completed", "failed", "stopped", "timed_out"]:
                break
            await asyncio.sleep(0.2)
            
        if job.status in ["completed", "failed", "stopped", "timed_out"]:
            return {
                "mode": "immediate",
                "job_id": job_id,
                "status": job.status,
                "exit_code": job.exit_code,
                "output": strip_ansi(self.get_job_log(job_id))
            }
        else:
            return {
                "mode": "background",
                "job_id": job_id,
                "status": "running",
                "message": "Command is taking time. Promoted to background job."
            }

job_manager = AsyncJobManager(state_manager)

# ==================================================================================
# INTERACTIVE SESSIONS
# ==================================================================================

class PowerShellSession:
    def __init__(self, session_id: str):
        self.id = session_id
        self.process = None
        self.stdout_queue = asyncio.Queue()
        self.stderr_queue = asyncio.Queue()
        self.active = True

    async def start(self):
        cmd = [SHELL_CMD, "-NoExit", "-Command", "-"]
        # Force UTF-8 environment
        env = os.environ.copy()
        env["LANG"] = "en_US.UTF-8"
        
        self.process = await asyncio.create_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env
        )
        
        asyncio.create_task(self._read_stream(self.process.stdout, self.stdout_queue, "STDOUT"))
        asyncio.create_task(self._read_stream(self.process.stderr, self.stderr_queue, "STDERR"))

        # Initialize encoding
        await self.write("$OutputEncoding = [Console]::OutputEncoding = [System.Text.Encoding]::UTF8")
        # Clear initial noise
        await asyncio.sleep(0.5)
        await self.read()

    async def _read_stream(self, stream, queue, label):
        while True:
            try:
                line = await stream.readline()
                if not line:
                    break
                decoded = line.decode("utf-8", errors="replace")
                await queue.put(decoded)
            except Exception:
                break
        self.active = False

    async def write(self, command: str):
        if not self.process or self.process.returncode is not None:
            raise Exception("Session is closed.")
        
        input_data = command + "\n"
        self.process.stdin.write(input_data.encode("utf-8"))
        await self.process.stdin.drain()

    async def read(self) -> str:
        lines = []
        while not self.stdout_queue.empty():
            lines.append(await self.stdout_queue.get())
        while not self.stderr_queue.empty():
            lines.append(await self.stderr_queue.get())
        return "".join(lines)

    async def kill(self):
        self.active = False
        if self.process:
            try:
                self.process.terminate()
                await self.process.wait()
            except ProcessLookupError:
                pass

class SessionManager:
    def __init__(self):
        self.sessions: Dict[str, PowerShellSession] = {}
        self.lock = threading.RLock()

    async def create_session(self) -> str:
        sid = str(uuid.uuid4())[:8]
        session = PowerShellSession(sid)
        await session.start()
        with self.lock:
            self.sessions[sid] = session
        state_manager.append_history("SESSION_CREATE", f"ID: {sid}")
        return sid

    def get_session(self, sid: str) -> Optional[PowerShellSession]:
        with self.lock:
            return self.sessions.get(sid)

    async def kill_session(self, sid: str) -> bool:
        session = self.get_session(sid)
        if session:
            await session.kill()
            with self.lock:
                del self.sessions[sid]
            state_manager.append_history("SESSION_KILL", f"ID: {sid}")
            return True
        return False

    def list_sessions(self) -> List[str]:
        with self.lock:
            return list(self.sessions.keys())

session_manager = SessionManager()

# ==================================================================================
# MCP APP
# ==================================================================================

# Initialize FastMCP with host="0.0.0.0" to disable strict Host header validation (allow LAN access)
mcp = FastMCP("GodMode", host="0.0.0.0")

@mcp.tool()
def ping() -> str:
    """Health check."""
    return "Pong"

# --- 1. Shell Execution ---

@mcp.tool()
@global_shield
async def shell_exec(command: str, timeout: int = 60, json_out: bool = False) -> str:
    """Execute a single PowerShell command asynchronously.
    Wraps 'pwsh -Command'.
    - json_out: If True, appends '| ConvertTo-Json' to the command.
    """
    if json_out and "ConvertTo-Json" not in command:
        command += " | ConvertTo-Json -Depth 2"

    state_manager.append_history("SHELL_EXEC", command[:50])
    
    # We force UTF-8 encoding in the command wrapper
    wrapped_cmd = f"$OutputEncoding = [Console]::OutputEncoding = [System.Text.Encoding]::UTF8; {command}"
    
    try:
        proc = await asyncio.create_subprocess_exec(
            SHELL_CMD, "-NonInteractive", "-Command", wrapped_cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        stdout_data = bytearray()
        stderr_data = bytearray()
        MAX_BYTES = 2 * 1024 * 1024 # 2MB Limit

        async def _read_stream(stream, buffer):
            while True:
                chunk = await stream.read(4096)
                if not chunk: break
                buffer.extend(chunk)
                if len(buffer) > MAX_BYTES:
                    buffer.extend(b"\n[TRUNCATED - Output exceeded 2MB]")
                    try: proc.kill()
                    except: pass
                    break

        try:
            await asyncio.wait_for(asyncio.gather(
                _read_stream(proc.stdout, stdout_data),
                _read_stream(proc.stderr, stderr_data)
            ), timeout=timeout)
            await proc.wait()
        except asyncio.TimeoutError:
            try: proc.kill()
            except: pass
            return "Error: Command timed out."

        output = stdout_data.decode('utf-8', errors='replace')
        error = stderr_data.decode('utf-8', errors='replace')

        # Normalize output
        output = strip_ansi(output)
        error = strip_ansi(error)

        if proc.returncode != 0:
            return f"EXIT CODE: {proc.returncode}\nERROR:\n{error}\nOUTPUT:\n{output}"
        
        # If stderr has content but exit code is 0, it might be warnings or non-fatal
        if error.strip():
            return f"{output}\n[STDERR]: {error}"
            
        return output
    except FileNotFoundError:
        return f"Error: {SHELL_CMD} not found in PATH."

# --- 2. Process Management ---

@mcp.tool()
@global_shield
async def job_start(script: str, timeout: int = 0) -> str:
    """Start a detached background PowerShell job. Returns Job ID.
    - timeout: Auto-kill after N seconds (0 = disabled).
    Job persists across server restarts.
    """
    return await job_manager.start_job(script, timeout=timeout)

@mcp.tool()
@global_shield
async def job_list() -> str:
    """List all background jobs and their status."""
    return json.dumps(job_manager.list_jobs(), indent=2)

@mcp.tool()
@global_shield
async def job_stop(job_id: str) -> str:
    """Stop a running background job (Hard Kill)."""
    return await job_manager.stop_job(job_id)

@mcp.tool()
@global_shield
async def job_logs(job_id: str, tail: int = 0, keyword: str = "") -> str:
    """Read the logs of a job.
    - tail: Last N lines.
    - keyword: Filter by text (grep).
    """
    return await asyncio.to_thread(job_manager.get_job_log, job_id, tail, keyword)

@mcp.tool()
@global_shield
async def smart_exec(command: str, wait_time: int = 3, timeout: int = 0) -> str:
    """Execute command smartly. 
    If it finishes within 'wait_time' (seconds), returns output.
    Else, returns Job ID for background tracking.
    - timeout: Auto-kill after N seconds (0=disabled).
    Recommended for unknown/long processes (e.g. installs).
    """
    res = await job_manager.run_smart(command, wait=wait_time, timeout=timeout)
    return json.dumps(res, indent=2)

# --- 3. Virtual Terminal ---

@mcp.tool()
@global_shield
async def session_create() -> str:
    """Create a persistent interactive PowerShell session."""
    return await session_manager.create_session()

@mcp.tool()
@global_shield
async def session_send(session_id: str, command: str) -> str:
    """Send a command to an active session."""
    s = session_manager.get_session(session_id)
    if not s: return "Error: Session not found."
    await s.write(command)
    return "Command sent."

@mcp.tool()
@global_shield
async def session_read(session_id: str) -> str:
    """Read buffered output from a session."""
    s = session_manager.get_session(session_id)
    if not s: return "Error: Session not found."
    return await s.read()

@mcp.tool()
@global_shield
async def session_kill(session_id: str) -> str:
    """Kill an interactive session."""
    if await session_manager.kill_session(session_id):
        return "Session killed."
    return "Session not found."

# --- 4. File System (Async Wrappers) ---

@mcp.tool()
@global_shield
async def fs_read(path: str, offset: int = 0, length: int = 10000, tail: int = 0) -> str:
    """Read file content. 
    - offset/length: Pagination.
    - tail: If > 0, reads last N lines (efficiently).
    """
    if not os.path.exists(path): return "Error: File not found."
    
    def _read():
        file_size = os.path.getsize(path)
        content = ""
        
        if tail > 0:
            # Use OS tail for efficiency on large files
            if IS_WINDOWS:
                try:
                    out = subprocess.check_output([SHELL_CMD, "-Command", f"Get-Content -Path '{escape_ps(path)}' -Tail {tail}"], timeout=5)
                    content = out.decode("utf-8", errors="replace")
                except Exception as e:
                    content = f"Error tailing file: {e}"
            else:
                try:
                    out = subprocess.check_output(["tail", "-n", str(tail), path], timeout=5)
                    content = out.decode("utf-8", errors="replace")
                except Exception as e:
                    content = f"Error tailing file: {e}"
        else:
            with open(path, "r", encoding="utf-8", errors="replace") as f:
                f.seek(offset)
                content = f.read(length)
        
        return json.dumps({
            "path": path,
            "total_size": file_size,
            "tail_mode": tail > 0,
            "offset": offset if tail == 0 else -1,
            "length_read": len(content),
            "content": content
        })
    return await asyncio.to_thread(_read)

@mcp.tool()
@global_shield
async def fs_write(path: str, content: str, mode: str = "w") -> str:
    """Write or Append content to file."""
    state_manager.append_history("FS_WRITE", f"{mode} {path}")
    
    def _write():
        with open(path, mode, encoding="utf-8") as f:
            f.write(content)
        return "Write successful."
    
    return await asyncio.to_thread(_write)

@mcp.tool()
@global_shield
async def fs_list(path: str) -> str:
    """List directory contents."""
    if not os.path.exists(path): return "Error: Path not found."
    return await asyncio.to_thread(lambda: json.dumps(os.listdir(path), indent=2))

@mcp.tool()
@global_shield
async def fs_delete(path: str) -> str:
    """Delete a file or directory recursively."""
    state_manager.append_history("FS_DELETE", path)
    
    def _delete():
        if os.path.isfile(path):
            os.remove(path)
        else:
            shutil.rmtree(path)
        return "Deleted."
    
    return await asyncio.to_thread(_delete)

@mcp.tool()
@global_shield
async def fs_search(path: str, pattern: str) -> str:
    """Search for text patterns in files recursively (grep equivalent)."""
    # PowerShell Select-String
    cmd = f"Get-ChildItem -Path '{escape_ps(path)}' -Recurse -File -ErrorAction SilentlyContinue | Select-String -Pattern '{escape_ps(pattern)}' | Select-Object Path, LineNumber, Line | ConvertTo-Json -Depth 1"
    return await shell_exec(cmd)

# --- 5. System Management ---

@mcp.tool()
@global_shield
async def service_manage(action: str, name: str) -> str:
    """Manage system services. action: status, start, stop, restart."""
    if action not in ["status", "start", "stop", "restart"]:
        return "Invalid action. Use: status, start, stop, restart."
    
    cmd = f"{action.capitalize()}-Service -Name '{escape_ps(name)}'"
    if action != "status":
        cmd += " -PassThru"
        
    cmd += " | Select-Object Name, Status, DisplayName | ConvertTo-Json"
    return await shell_exec(cmd)

@mcp.tool()
@global_shield
async def process_manage(action: str, target: str = "all") -> str:
    """Manage processes. 
    action: 'list' (target='all' or name), 'kill' (target=ID).
    """
    if action == "list":
        if target == "all":
            # Top 20 CPU consumers
            cmd = "Get-Process | Sort-Object CPU -Descending | Select-Object -First 20 Id, ProcessName, CPU, WorkingSet | ConvertTo-Json"
        else:
            cmd = f"Get-Process -Name '{escape_ps(target)}' -ErrorAction SilentlyContinue | Select-Object Id, ProcessName, CPU, WorkingSet | ConvertTo-Json"
            
    elif action == "kill":
        cmd = f"Stop-Process -Id {target} -Force -PassThru | Select-Object Id, ProcessName | ConvertTo-Json"
    else:
        return "Invalid action."
        
    return await shell_exec(cmd)

@mcp.tool()
@global_shield
async def registry_read(path: str, name: str) -> str:
    """Read Windows Registry key (Windows Only)."""
    if not IS_WINDOWS:
        return "Error: Registry tools are Windows-only."
    cmd = f"Get-ItemProperty -Path '{escape_ps(path)}' -Name '{escape_ps(name)}' | ConvertTo-Json"
    return await shell_exec(cmd)

# --- 6. Network & System (God Mode) ---

@mcp.tool()
@global_shield
async def net_inspect(target: str = "all") -> str:
    """Inspect network configuration using native tools via PowerShell."""
    cmd = ""
    if IS_WINDOWS:
        if target == "interfaces" or target == "all":
            cmd += "Write-Output '--- INTERFACES ---'; Get-NetAdapter | Select-Object Name,Status,MacAddress,LinkSpeed | Format-Table -AutoSize; ipconfig /all; "
        if target == "routes" or target == "all":
            cmd += "Write-Output '--- ROUTES ---'; route print; "
        if target == "sockets" or target == "all":
            cmd += "Write-Output '--- SOCKETS ---'; Get-NetTCPConnection | Select-Object LocalAddress,LocalPort,RemoteAddress,RemotePort,State,OwningProcess | Format-Table -AutoSize; "
    else:
        if target == "interfaces" or target == "all":
            cmd += "Write-Output '--- INTERFACES ---'; ip -c addr; "
        if target == "routes" or target == "all":
            cmd += "Write-Output '--- ROUTES ---'; ip -c route; "
        if target == "sockets" or target == "all":
            cmd += "Write-Output '--- SOCKETS ---'; ss -tuln; "
        
    return await shell_exec(cmd)

@mcp.tool()
@global_shield
async def net_dns(hostname: str) -> str:
    """Resolve DNS name."""
    if IS_WINDOWS:
        cmd = f"Resolve-DnsName -Name '{escape_ps(hostname)}' -ErrorAction SilentlyContinue | Select-Object Name, Type, IPAddress, Section | ConvertTo-Json"
    else:
        # Linux fallback using .NET directly
        cmd = f"[System.Net.Dns]::GetHostEntry('{escape_ps(hostname)}') | Select-Object HostName, AddressList | ConvertTo-Json"
    return await shell_exec(cmd)

@mcp.tool()
@global_shield
async def net_connect(target: str, port: int) -> str:
    """Test TCP connection to a target."""
    if IS_WINDOWS:
        cmd = f"Test-NetConnection -ComputerName '{escape_ps(target)}' -Port {port} | Select-Object ComputerName, RemotePort, TcpTestSucceeded, PingSucceeded | ConvertTo-Json"
    else:
        # Linux fallback using .NET TcpClient
        cmd = f"""
        try {{
            $tcp = New-Object System.Net.Sockets.TcpClient
            $tcp.Connect('{escape_ps(target)}', {port})
            [PSCustomObject]@{{ComputerName='{escape_ps(target)}'; RemotePort={port}; TcpTestSucceeded=$true}} | ConvertTo-Json
            $tcp.Close()
        }} catch {{
            [PSCustomObject]@{{ComputerName='{escape_ps(target)}'; RemotePort={port}; TcpTestSucceeded=$false; Error=$_.Exception.Message}} | ConvertTo-Json
        }}
        """
    return await shell_exec(cmd)

@mcp.tool()
@global_shield
async def sys_info() -> str:
    """Get system resource usage (CPU/RAM/Disk)."""
    def _get_info():
        mem = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        
        if hasattr(os, "getloadavg"):
            load = os.getloadavg()
        else:
            # Windows doesn't support getloadavg, use current CPU percent
            p = psutil.cpu_percent(interval=0.1)
            load = (p, p, p)

        return json.dumps({
            "os": platform.system(),
            "shell": SHELL_CMD,
            "cpu_load_1m_5m_15m": load,
            "memory_total_gb": round(mem.total / (1024**3), 2),
            "memory_available_gb": round(mem.available / (1024**3), 2),
            "disk_total_gb": round(disk.total / (1024**3), 2),
            "disk_free_gb": round(disk.free / (1024**3), 2),
            "boot_time": datetime.fromtimestamp(psutil.boot_time()).isoformat()
        }, indent=2)
    return await asyncio.to_thread(_get_info)

@mcp.tool()
@global_shield
async def log_read(log_path: str = "", lines: int = 50) -> str:
    """Read tail of a log file.
    On Linux defaults to /var/log/syslog.
    On Windows defaults to checking 'System' Event Log if path empty.
    """
    if not log_path:
        if IS_WINDOWS:
            cmd = f"Get-EventLog -LogName System -Newest {lines} | Format-Table -AutoSize"
            return await shell_exec(cmd)
        else:
            log_path = "/var/log/syslog"

    if IS_WINDOWS:
        cmd = f"Get-Content -Path '{log_path}' -Tail {lines}"
    else:
        cmd = f"tail -n {lines} {log_path}"
    return await shell_exec(cmd)

# ==================================================================================
# MAIN ENTRY
# ==================================================================================

def start_health_supervisor():
    def loop():
        while True:
            try:
                time.sleep(60)
                # Heartbeat
                logger.info(f"HEARTBEAT | Jobs: {len(job_manager.jobs)} | Sessions: {len(session_manager.sessions)}")
                
                # Active Reconciliation (Self Healing)
                job_manager.reconcile()
                
                # Log Rotation for server.log is handled by RotatingFileHandler.
                            
            except Exception as e:
                logger.error(f"Supervisor Error: {e}")
                
    t = threading.Thread(target=loop, daemon=True)
    t.start()

start_health_supervisor()

def main():
    local_ip = get_local_ip()
    logger.info("="*50)
    logger.info(" UNIVERSAL GOD MODE SERVER (MCP)")
    logger.info("="*50)
    logger.info(f"HOST: {local_ip}:{PORT}")
    
    app = Starlette(middleware=[
        Middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
    ])
    
    app.mount("/", mcp.sse_app())
    
    uvicorn.run(app, host=HOST, port=PORT)

if __name__ == "__main__":
    main()
