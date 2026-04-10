import sys
import csv
import time
import inspect
import os
from datetime import datetime
from contextlib import contextmanager
from functools import wraps


# ---- Config ----

CONFIG = {
    "log_dir": os.path.join(os.getcwd(), "logs"),
    "enable_console": False
}

FIXED_COLS = ["run_at", "method", "label"]
LINE_FIXED_COLS = ["run_id", "run_at", "label", "line_no", "source", "time_sec"]

def configure(log_folder=None, enable_console_print=None):
    if log_folder is not None:
        CONFIG["log_dir"] = log_folder
    if enable_console_print is not None:
        CONFIG["enable_console"] = enable_console_print

def _get_log_file():
    return os.path.join(CONFIG["log_dir"], "timer_log.csv")

def _get_line_log_file():
    return os.path.join(CONFIG["log_dir"], "line_timer_log.csv")

def _ensure_dir(filepath):
    d = os.path.dirname(filepath)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)

# ---- CSV Helpers ----

def _read_csv():
    log_file = _get_log_file()
    if not os.path.exists(log_file):
        return [], []
    with open(log_file, "r", newline="") as f:
        reader = csv.DictReader(f)
        cols   = list(reader.fieldnames or [])
        rows   = list(reader)
    return rows, cols


def _write_csv(rows, cols):
    log_file = _get_log_file()
    _ensure_dir(log_file)
    with open(log_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _time_cols(cols):
    return sorted(
        [c for c in cols if c.startswith("time_sec_")],
        key=lambda x: int(x.split("_")[-1])
    )


# ---- Session State (fixed for entire script run) ----

def _get_next_col_for(rows, method, label):
    # Find the maximum N where time_sec_N is NOT empty or "-" for THIS label.
    max_idx = 0
    for r in rows:
        if r.get("method", "").strip() == method and r.get("label", "").strip() == label:
            for k, v in r.items():
                if k.startswith("time_sec_") and v not in ("", "-", None):
                    try:
                        idx = int(k.split("_")[-1])
                        max_idx = max(max_idx, idx)
                    except ValueError:
                        pass
    return f"time_sec_{max_idx + 1}"


# ---- Core Update Logic ----

def _update_csv(entries):
    rows, cols = _read_csv()
    
    if not entries:
        return
        
    method = entries[0]["method"]
    label  = entries[0]["label"]
    
    new_col = _get_next_col_for(rows, method, label)
    run_at  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Build column list — add new_col if missing
    tc       = _time_cols(cols)
    all_cols = FIXED_COLS + tc
    if new_col not in all_cols:
        all_cols.append(new_col)

    # Init new_col on existing rows
    for row in rows:
        if new_col not in row or row[new_col] == "":
            row[new_col] = "-"

    # Find the specific row for this method and label
    target_row = next((r for r in rows if r.get("method", "").strip() == method and r.get("label", "").strip() == label), None)
    
    if target_row:
        target_row["run_at"] = run_at
        target_row[new_col]  = entries[0]["time_sec"]
    else:
        new_row = {c: "-" for c in all_cols}
        new_row.update({
            "run_at" : run_at,
            "method" : f"{method:<20}",
            "label"  : f"{label:<12}",
            new_col  : entries[0]["time_sec"],
        })
        rows.append(new_row)

    _write_csv(rows, all_cols)


# ---- Method 1: Specific Part of Code ----

@contextmanager
def block_timer(label):
    start = time.perf_counter()
    yield
    sec = time.perf_counter() - start
    if CONFIG["enable_console"]:
        print(f"{label}\t : {sec:.4f}s")
    _update_csv([{
        "method"  : "Block Timer",
        "label"   : label,
        "time_sec": f"{sec:.5f}",
    }])


# ---- Method 2: Function ----

def func_timer(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start  = time.perf_counter()
        result = func(*args, **kwargs)
        sec    = time.perf_counter() - start
        if CONFIG["enable_console"]:
            print(f"{func.__name__}\t : {sec:.4f}s")
        _update_csv([{
            "method"  : "Function Timer",
            "label"   : func.__name__,
            "time_sec": f"{sec:.5f}",
        }])
        return result
    return wrapper


def _update_line_csv(entries):
    log_file = _get_line_log_file()
    if not os.path.exists(log_file):
        run_id = 1
    else:
        with open(log_file, "r", newline="", encoding="utf-8") as f:
            run_ids = [int(r.get("run_id", 0)) for r in csv.DictReader(f) if str(r.get("run_id", "")).strip().isdigit()]
            run_id = max(run_ids) + 1 if run_ids else 1
            
    run_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    file_exists = os.path.exists(log_file)
    _ensure_dir(log_file)
    
    with open(log_file, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=LINE_FIXED_COLS)
        if not file_exists:
            writer.writeheader()
        
        for entry in entries:
            writer.writerow({
                "run_id": f"{run_id:<4}",
                "run_at": run_at,
                "label": f"{entry['label']:<11}",
                "line_no": f"{entry['line_no']:<4}",
                "source": f"{entry['source'][:50]:<50}",
                "time_sec": f"{entry['time_sec']:>8}"
            })

def analyze_line_runs(label):
    log_file = _get_line_log_file()
    if not os.path.exists(log_file):
        print(f"No log data available in {log_file}.")
        return
        
    with open(log_file, "r", newline="", encoding="utf-8") as f:
        rows = [r for r in csv.DictReader(f) if r["label"] == label]
        
    if not rows:
        print(f"No log data for label: {label}")
        return
        
    runs = {}
    for r in rows:
        rid = int(r["run_id"])
        if rid not in runs:
            runs[rid] = {"rows": [], "run_at": r["run_at"], "total": 0.0}
        runs[rid]["rows"].append(r)
        try:
            runs[rid]["total"] += float(r["time_sec"])
        except ValueError:
            pass
            
    print(f"\n{'='*70}")
    print(f"HISTORY ANALYSIS FOR: {label}")
    print(f"{'='*70}")
    
    for rid, data in sorted(runs.items()):
        print(f"\n[ RUN ID: {rid:<3} | Timestamp: {data['run_at']} | Total Time: {data['total']:.5f}s ]")
        print("-" * 70)
        for r in data["rows"]:
            print(f"Line {int(r['line_no']):<3} | {r['time_sec']:>9}s | {r['source']}")

# ---- Method 3: Line by Line ----

def line_timer(func, *args, **kwargs):
    line_times = {}
    frame_state = {}

    def _tracer(frame, event, _arg):
        if frame.f_code is not func.__code__:
            return _tracer
            
        now = time.perf_counter()
        
        if event in ("line", "exception", "return"):
            if frame in frame_state:
                ln, t = frame_state[frame]
                line_times[ln] = line_times.get(ln, 0.0) + (now - t)
            
            if event == "return":
                if frame in frame_state:
                    del frame_state[frame]
            else:
                frame_state[frame] = (frame.f_lineno, now)
                
        return _tracer

    old_trace = sys.gettrace()
    sys.settrace(_tracer)
    try:
        result = func(*args, **kwargs)
    finally:
        sys.settrace(old_trace)

    try:
        src_lines = inspect.getsource(func).splitlines()
        start_no  = func.__code__.co_firstlineno
    except (TypeError, OSError):
        src_lines = []
        start_no = 1

    entries   = []

    if CONFIG["enable_console"]:
        print(f"\n{'Line':<6} {'Source':<40} {'Time':>10}")
        print("-" * 58)

    for i, src in enumerate(src_lines):
        lineno  = start_no + i
        display = i + 1
        sec     = line_times.get(lineno, 0.0)
        marker  = " <--" if sec > 0 else ""
        if CONFIG["enable_console"]:
            print(f"{display:<6} {src[:38]:<40} {sec:>9.4f}s{marker}")
        entries.append({
            "method"  : "Line Timer",
            "label"   : func.__name__,
            "line_no" : display,
            "source"  : src.rstrip()[:200],
            "time_sec": f"{sec:.5f}",
        })

    _update_line_csv(entries)
    return result


# ---- Usage Examples ----

r"""
import time_management as tm
import time

# tm.configure(enable_console_print=False)
# tm.configure(log_folder=r"C:\Users\ojhas\OneDrive\Desktop\Trade_with_ai\Data")

# Method 1 -----
# Specific Part of Code
with tm.block_timer("loop"): # add
    for i in range(2):
        time.sleep(0.1)

# Method 2 ----- 
# Function
@tm.func_timer               # add
def my_function():
    for _ in range(2):
        time.sleep(0.1)

my_function()

# Method 3 -----
# Line by Line
def track_lines():           # add
    for _ in range(2):
        time.sleep(0.1)

tm.line_timer(track_lines)   # add
"""