import sys
import time
import inspect
from contextlib import contextmanager
from functools import wraps


# ---- Method 1: Specific Part of Code ----

@contextmanager
def block_timer(label):
    start = time.perf_counter()
    yield
    print(f"{label}\t : {time.perf_counter()-start:.4f}s")


# ---- Method 2: Function ----

def func_timer(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        result = func(*args, **kwargs)
        print(f"{func.__name__}\t : {time.perf_counter()-start:.4f}s")
        return result
    return wrapper


# ---- Method 3: Line by Line ----

def line_timer(func):
    line_times = {}
    last = [None]

    def _tracer(frame, event, _):
        if frame.f_code is not func.__code__:
            return _tracer
        now = time.perf_counter()
        if event == "line":
            if last[0] is not None:
                ln, t = last[0]
                line_times[ln] = line_times.get(ln, 0) + (now - t)
            last[0] = (frame.f_lineno, now)
        elif event == "return":
            if last[0] is not None:
                ln, t = last[0]
                line_times[ln] = line_times.get(ln, 0) + (now - t)
            last[0] = None
        return _tracer

    sys.settrace(_tracer)
    func()
    sys.settrace(None)

    src_lines = inspect.getsource(func).splitlines()
    start_no  = func.__code__.co_firstlineno

    print(f"\n{'Line':<6} {'Source':<40} {'Time':>10}")
    print("-" * 58)
    for i, src in enumerate(src_lines):
        lineno  = start_no + i
        display = i + 1
        sec     = line_times.get(lineno, 0)
        marker  = " <--" if sec > 0 else ""
        print(f"{display:<6} {src:<40} {sec:>9.4f}s{marker}")

"""
import time_management as tm
import time

# Method 1  
# Specific Part of Code
with tm.block_timer("loop"): # add
    for i in range(2):
        time.sleep(1)

# Method 2  
# Function
@tm.func_timer # add
def my_function():
    for _ in range(2):
        time.sleep(1)

my_function()

# Method 3  
# Line by Line
def track_lines(): # add
    for _ in range(2):
        time.sleep(1)

tm.line_timer(track_lines)

"""