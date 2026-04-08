from __future__ import annotations

import csv
import io
import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import requests
from zoneinfo import ZoneInfo


INSTRUMENT_CSV_COMPACT_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"
INSTRUMENT_CSV_FALLBACK_URL = "https://images.dhan.co/api-data/api-scrip-master-detailed.csv"
INTRADAY_URL = "https://api.dhan.co/v2/charts/intraday"
OPTIONCHAIN_URL = "https://api.dhan.co/v2/optionchain"
OPTIONCHAIN_EXPIRYLIST_URL = "https://api.dhan.co/v2/optionchain/expirylist"
NSE_HOME_URL = "https://www.nseindia.com"
NSE_ALL_INDICES_URL = "https://www.nseindia.com/api/allIndices"
CACHE_DIR = Path(__file__).resolve().parent / ".cache"
INSTRUMENT_ROWS_CACHE_TTL_SECONDS = 6 * 60 * 60
RESOLVED_INSTRUMENT_CACHE_TTL_SECONDS = 6 * 60 * 60
MAX_INSTRUMENT_ROWS_CACHE_BYTES = 80 * 1024 * 1024
_IN_MEMORY_INSTRUMENT_ROWS: Dict[str, Dict[str, Any]] = {}
_IN_MEMORY_NSE_ALL_INDICES: Dict[str, Any] = {
    "cached_at": 0,
    "rows": [],
}
NSE_ALL_INDICES_CACHE_TTL_SECONDS = 20


MONTH_MAP = {
    "JANUARY": "JAN",
    "FEBRUARY": "FEB",
    "MARCH": "MAR",
    "APRIL": "APR",
    "MAY": "MAY",
    "JUNE": "JUN",
    "JULY": "JUL",
    "AUGUST": "AUG",
    "SEPTEMBER": "SEP",
    "OCTOBER": "OCT",
    "NOVEMBER": "NOV",
    "DECEMBER": "DEC",
}

UNDERLYING_CHAIN_MAP: Dict[str, Tuple[int, str]] = {
    "NIFTY": (13, "IDX_I"),
}


@dataclass
class InstrumentRef:
    security_id: str
    exchange_segment: str
    instrument: str
    display_name: str


def _ensure_cache_dir() -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)


def _safe_read_json(path: Path) -> Optional[Dict[str, Any]]:
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            return data
    except (OSError, json.JSONDecodeError):
        return None
    return None


def _safe_write_json(path: Path, data: Dict[str, Any]) -> None:
    try:
        _ensure_cache_dir()
        with path.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=True, separators=(",", ":"))
    except OSError:
        # Cache should never break main flow.
        return


def _instrument_rows_cache_path(exchange_segment: str) -> Path:
    return CACHE_DIR / f"instrument_rows_{exchange_segment.lower()}.json"


def _resolved_cache_path() -> Path:
    return CACHE_DIR / "resolved_instruments.json"


def _now_epoch() -> int:
    return int(datetime.now(timezone.utc).timestamp())


def _extract_exchange_segment(row: Dict[str, Any]) -> str:
    return _pick(
        row,
        (
            "EXCHANGE_SEGMENT",
            "EXCHANGE",
            "EXCH_ID",
            "SEM_EXM_EXCH_ID",
            "exchangeSegment",
            "exchange_segment",
        ),
    ).upper()


def _filter_rows_by_exchange(rows: List[Dict[str, Any]], exchange_segment: str) -> List[Dict[str, Any]]:
    segment = exchange_segment.upper().strip()
    filtered = [row for row in rows if _extract_exchange_segment(row) == segment]
    return filtered if filtered else rows


def _compact_instrument_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    keys = (
        "DISPLAY_NAME",
        "SEM_CUSTOM_SYMBOL",
        "SYMBOL_NAME",
        "SM_SYMBOL_NAME",
        "UNDERLYING_SYMBOL",
        "UNDERLYING",
        "SEM_TRADING_SYMBOL",
        "SECURITY_ID",
        "SEM_SMST_SECURITY_ID",
        "INSTRUMENT",
        "SEM_INSTRUMENT_NAME",
        "EXCHANGE_SEGMENT",
        "EXCHANGE",
        "EXCH_ID",
        "SEM_EXM_EXCH_ID",
    )
    compact: List[Dict[str, Any]] = []
    for row in rows:
        item = {k: row.get(k) for k in keys if k in row and row.get(k) is not None}
        if item:
            compact.append(item)
    return compact if compact else rows


def _pick(row: Dict[str, Any], keys: Sequence[str], default: str = "") -> str:
    for key in keys:
        value = row.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return default


def _normalize_text(text: str) -> str:
    value = text.upper().strip()
    value = value.replace(" CE ", " CALL ").replace(" PE ", " PUT ")
    value = re.sub(r"\bCE\b", "CALL", value)
    value = re.sub(r"\bPE\b", "PUT", value)
    for full, short in MONTH_MAP.items():
        value = re.sub(rf"\b{full}\b", short, value)
    value = re.sub(r"[^A-Z0-9]+", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def _extract_underlying(instrument_name: str) -> str:
    tokens = _normalize_text(instrument_name).split()
    return tokens[0] if tokens else ""


def _filter_rows_by_underlying(rows: List[Dict[str, Any]], underlying: str) -> List[Dict[str, Any]]:
    if not underlying:
        return rows

    filtered: List[Dict[str, Any]] = []
    for row in rows:
        display = _normalize_text(
            _pick(row, ("DISPLAY_NAME", "SEM_CUSTOM_SYMBOL", "SYMBOL_NAME", "SM_SYMBOL_NAME"))
        )
        base = _normalize_text(
            _pick(row, ("UNDERLYING_SYMBOL", "UNDERLYING", "SM_SYMBOL_NAME", "SYMBOL_NAME"))
        )
        if underlying in set(display.split()) or underlying in set(base.split()):
            filtered.append(row)
    return filtered if filtered else rows


def _build_headers(access_token: str, client_id: str) -> Dict[str, str]:
    return {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "access-token": access_token,
        "client-id": str(client_id),
    }


def _parse_named_option(instrument_name: str) -> Optional[Dict[str, Any]]:
    norm = _normalize_text(instrument_name)
    tokens = norm.split()
    if len(tokens) < 4:
        return None

    option_type: Optional[str] = None
    if "CALL" in tokens:
        option_type = "CALL"
    elif "PUT" in tokens:
        option_type = "PUT"
    if option_type is None:
        return None

    short_months = set(MONTH_MAP.values())
    month: Optional[str] = None
    month_idx = -1
    for idx, token in enumerate(tokens):
        if token in short_months:
            month = token
            month_idx = idx
            break
    if month is None or month_idx <= 0:
        return None

    date_token_raw = tokens[month_idx - 1]
    if not date_token_raw.isdigit():
        return None
    date_token = int(date_token_raw)
    if 2000 <= date_token <= 2100:
        date_token = date_token % 100
    if not (1 <= date_token <= 99):
        return None

    strike: Optional[float] = None
    for token in reversed(tokens):
        if token.replace(".", "", 1).isdigit():
            value = float(token)
            if value >= 1:
                strike = value
                break
    if strike is None:
        return None

    underlying = tokens[0]
    return {
        "normalized": norm,
        "underlying": underlying,
        "date_token": date_token,
        "month": month,
        "strike": strike,
        "option_type": option_type,
    }


def _select_expiry(expiries: List[str], date_token: int, month_abbr: str, ref_dt: datetime) -> Optional[str]:
    month_num = datetime.strptime(month_abbr, "%b").month
    month_candidates: List[datetime] = []
    for expiry in expiries:
        try:
            d = datetime.strptime(expiry, "%Y-%m-%d").date()
        except ValueError:
            continue
        if d.month == month_num:
            month_candidates.append(datetime(d.year, d.month, d.day, tzinfo=ref_dt.tzinfo))

    if not month_candidates:
        return None

    year_candidates = [d for d in month_candidates if (d.year % 100) == date_token]
    day_candidates = [d for d in month_candidates if d.day == date_token]
    current_yy = ref_dt.year % 100
    try_year_first = abs(date_token - current_yy) <= 5

    candidates: List[datetime] = []
    if try_year_first and year_candidates:
        candidates = year_candidates
    elif day_candidates:
        candidates = day_candidates
    elif year_candidates:
        candidates = year_candidates
    else:
        candidates = month_candidates

    # Prefer upcoming expiry for same day/month, otherwise closest previous.
    candidates.sort(key=lambda d: (0 if d >= ref_dt else 1, abs((d - ref_dt).days)))
    return candidates[0].strftime("%Y-%m-%d")


def _resolve_from_option_chain(
    instrument_name: str,
    access_token: str,
    client_id: str,
    request_timeout: int,
    reference_dt: datetime,
) -> Optional[InstrumentRef]:
    parsed = _parse_named_option(instrument_name)
    if not parsed:
        return None

    # Confirmed underlying ids from Dhan option chain docs/examples.
    underlying_map = {
        "NIFTY": (13, "IDX_I"),
    }
    underlying_info = underlying_map.get(parsed["underlying"])
    if not underlying_info:
        return None

    underlying_scrip, underlying_seg = underlying_info
    headers = _build_headers(access_token, client_id)

    try:
        expiry_resp = requests.post(
            OPTIONCHAIN_EXPIRYLIST_URL,
            json={"UnderlyingScrip": underlying_scrip, "UnderlyingSeg": underlying_seg},
            headers=headers,
            timeout=request_timeout,
        )
        expiry_resp.raise_for_status()
        expiry_data = expiry_resp.json()
        expiries = expiry_data.get("data", [])
        if not isinstance(expiries, list) or not expiries:
            return None
        expiry = _select_expiry(expiries, parsed["date_token"], parsed["month"], reference_dt)
        if not expiry:
            return None

        chain_resp = requests.post(
            OPTIONCHAIN_URL,
            json={
                "UnderlyingScrip": underlying_scrip,
                "UnderlyingSeg": underlying_seg,
                "Expiry": expiry,
            },
            headers=headers,
            timeout=request_timeout,
        )
        chain_resp.raise_for_status()
        chain = chain_resp.json().get("data", {}).get("oc", {})
        if not isinstance(chain, dict) or not chain:
            return None

        strike_target = float(parsed["strike"])
        best_key: Optional[str] = None
        best_diff = float("inf")
        for strike_key in chain.keys():
            try:
                strike_val = float(strike_key)
            except (TypeError, ValueError):
                continue
            diff = abs(strike_val - strike_target)
            if diff < best_diff:
                best_diff = diff
                best_key = strike_key
            if diff == 0:
                break
        if not best_key:
            return None

        side = "ce" if parsed["option_type"] == "CALL" else "pe"
        option_leg = chain.get(best_key, {}).get(side, {})
        security_id = str(option_leg.get("security_id", "")).strip()
        if not security_id:
            return None

        return InstrumentRef(
            security_id=security_id,
            exchange_segment="NSE_FNO",
            instrument="OPTIDX",
            display_name=instrument_name.upper(),
        )
    except (requests.RequestException, ValueError):
        return None


def _score_match(query_norm: str, row: Dict[str, Any]) -> int:
    display = _normalize_text(
        _pick(row, ("DISPLAY_NAME", "SEM_CUSTOM_SYMBOL", "SYMBOL_NAME", "SM_SYMBOL_NAME"))
    )
    symbol = _normalize_text(_pick(row, ("SYMBOL_NAME", "SM_SYMBOL_NAME", "SEM_TRADING_SYMBOL")))
    query_tokens = set(query_norm.split())
    display_tokens = set(display.split())
    symbol_tokens = set(symbol.split())

    if query_norm and (query_norm == display or query_norm == symbol):
        return 1000

    score = 0
    if query_norm and query_norm in display:
        score += 650
    if query_norm and query_norm in symbol:
        score += 500
    if query_tokens:
        score += int(300 * (len(query_tokens & display_tokens) / len(query_tokens)))
        score += int(200 * (len(query_tokens & symbol_tokens) / len(query_tokens)))
    return score


def _extract_rows(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]

    if isinstance(payload, dict):
        if isinstance(payload.get("data"), list):
            return [x for x in payload["data"] if isinstance(x, dict)]
        if isinstance(payload.get("data"), dict):
            rows: List[Dict[str, Any]] = []
            for value in payload["data"].values():
                if isinstance(value, list):
                    rows.extend([x for x in value if isinstance(x, dict)])
            if rows:
                return rows

        # Some APIs return dict with numeric keys.
        maybe_rows = [x for x in payload.values() if isinstance(x, dict)]
        if maybe_rows:
            return maybe_rows

    return []


def _download_instrument_rows(
    exchange_segment: str,
    timeout: int = 20,
    force_refresh: bool = False,
) -> List[Dict[str, Any]]:
    segment_key = exchange_segment.upper().strip()
    if not force_refresh:
        mem_obj = _IN_MEMORY_INSTRUMENT_ROWS.get(segment_key, {})
        if isinstance(mem_obj, dict):
            cached_at = int(mem_obj.get("cached_at", 0))
            rows = mem_obj.get("rows")
            if isinstance(rows, list) and (_now_epoch() - cached_at) <= INSTRUMENT_ROWS_CACHE_TTL_SECONDS:
                return rows

    cache_file = _instrument_rows_cache_path(exchange_segment)
    if cache_file.exists():
        try:
            if cache_file.stat().st_size > MAX_INSTRUMENT_ROWS_CACHE_BYTES:
                cache_file.unlink(missing_ok=True)
        except OSError:
            pass

    if not force_refresh:
        cache_obj = _safe_read_json(cache_file)
        if cache_obj:
            cached_at = int(cache_obj.get("cached_at", 0))
            rows = cache_obj.get("rows")
            if isinstance(rows, list) and (_now_epoch() - cached_at) <= INSTRUMENT_ROWS_CACHE_TTL_SECONDS:
                clean = [x for x in rows if isinstance(x, dict)]
                compact = _compact_instrument_rows(clean)
                _IN_MEMORY_INSTRUMENT_ROWS[segment_key] = {
                    "cached_at": _now_epoch(),
                    "rows": compact,
                }
                if len(compact) == len(clean) and clean and len(clean[0]) > len(compact[0]):
                    _safe_write_json(
                        cache_file,
                        {"cached_at": _now_epoch(), "exchange_segment": exchange_segment, "rows": compact},
                    )
                return compact

    with requests.Session() as session:
        # Compact CSV is significantly smaller and faster than full segment payloads.
        try:
            csv_resp = session.get(INSTRUMENT_CSV_COMPACT_URL, timeout=timeout)
            csv_resp.raise_for_status()
            reader = csv.DictReader(io.StringIO(csv_resp.text))
            rows = _compact_instrument_rows(
                _filter_rows_by_exchange(list(reader), exchange_segment)
            )
            _IN_MEMORY_INSTRUMENT_ROWS[segment_key] = {
                "cached_at": _now_epoch(),
                "rows": rows,
            }
            _safe_write_json(
                cache_file,
                {"cached_at": _now_epoch(), "exchange_segment": exchange_segment, "rows": rows},
            )
            return rows
        except requests.RequestException:
            pass

        # Final fallback to detailed CSV.
        csv_resp = session.get(INSTRUMENT_CSV_FALLBACK_URL, timeout=timeout)
        csv_resp.raise_for_status()
        reader = csv.DictReader(io.StringIO(csv_resp.text))
        rows = _compact_instrument_rows(_filter_rows_by_exchange(list(reader), exchange_segment))
        _IN_MEMORY_INSTRUMENT_ROWS[segment_key] = {
            "cached_at": _now_epoch(),
            "rows": rows,
        }
        _safe_write_json(
            cache_file,
            {"cached_at": _now_epoch(), "exchange_segment": exchange_segment, "rows": rows},
        )
        return rows


def refresh_instruments(exchange_segment: str = "NSE_FNO", request_timeout: int = 20) -> int:
    rows = _download_instrument_rows(
        exchange_segment=exchange_segment,
        timeout=request_timeout,
        force_refresh=True,
    )
    return len(rows)


def _resolve_instrument(
    instrument_name: str,
    exchange_segment: str = "NSE_FNO",
    access_token: Optional[str] = None,
    client_id: Optional[str] = None,
    request_timeout: int = 20,
    reference_dt: Optional[datetime] = None,
) -> InstrumentRef:
    query_norm = _normalize_text(instrument_name)
    cache_key = f"{exchange_segment.upper().strip()}|{query_norm}"
    resolved_cache = _safe_read_json(_resolved_cache_path()) or {}
    items = resolved_cache.get("items", {})
    if isinstance(items, dict):
        entry = items.get(cache_key, {})
        if isinstance(entry, dict):
            cached_at = int(entry.get("cached_at", 0))
            if (_now_epoch() - cached_at) <= RESOLVED_INSTRUMENT_CACHE_TTL_SECONDS:
                security_id = str(entry.get("security_id", "")).strip()
                instrument = str(entry.get("instrument", "")).strip()
                display_name = str(entry.get("display_name", "")).strip()
                if security_id and instrument and display_name:
                    return InstrumentRef(
                        security_id=security_id,
                        exchange_segment=exchange_segment,
                        instrument=instrument,
                        display_name=display_name,
                    )

    if access_token and client_id:
        ref_dt = reference_dt or datetime.now(timezone.utc)
        fast_ref = _resolve_from_option_chain(
            instrument_name=instrument_name,
            access_token=access_token,
            client_id=client_id,
            request_timeout=request_timeout,
            reference_dt=ref_dt,
        )
        if fast_ref:
            cache_data = _safe_read_json(_resolved_cache_path()) or {"items": {}}
            cache_items = cache_data.setdefault("items", {})
            cache_items[cache_key] = {
                "cached_at": _now_epoch(),
                "security_id": fast_ref.security_id,
                "instrument": fast_ref.instrument,
                "display_name": fast_ref.display_name,
            }
            _safe_write_json(_resolved_cache_path(), cache_data)
            return fast_ref

    rows = _download_instrument_rows(exchange_segment=exchange_segment)
    if not rows:
        raise RuntimeError("Could not fetch instrument list from Dhan.")
    rows = _filter_rows_by_underlying(rows, _extract_underlying(instrument_name))

    # Fast path: exact normalized display/symbol match.
    for row in rows:
        display = _normalize_text(
            _pick(row, ("DISPLAY_NAME", "SEM_CUSTOM_SYMBOL", "SYMBOL_NAME", "SM_SYMBOL_NAME"))
        )
        symbol = _normalize_text(
            _pick(row, ("SYMBOL_NAME", "SM_SYMBOL_NAME", "SEM_TRADING_SYMBOL"))
        )
        if query_norm and (query_norm == display or query_norm == symbol):
            security_id = _pick(row, ("SECURITY_ID", "SEM_SMST_SECURITY_ID"))
            instrument = _pick(row, ("INSTRUMENT", "SEM_INSTRUMENT_NAME")) or "OPTIDX"
            display_name = _pick(
                row,
                ("DISPLAY_NAME", "SEM_CUSTOM_SYMBOL", "SYMBOL_NAME", "SM_SYMBOL_NAME"),
                default=instrument_name.upper(),
            )
            result = InstrumentRef(
                security_id=security_id,
                exchange_segment=exchange_segment,
                instrument=instrument,
                display_name=display_name,
            )
            if security_id:
                cache_data = _safe_read_json(_resolved_cache_path()) or {"items": {}}
                cache_items = cache_data.setdefault("items", {})
                cache_items[cache_key] = {
                    "cached_at": _now_epoch(),
                    "security_id": result.security_id,
                    "instrument": result.instrument,
                    "display_name": result.display_name,
                }
                _safe_write_json(_resolved_cache_path(), cache_data)
            return result

    best_row: Optional[Dict[str, Any]] = None
    best_score = -1
    best_names: List[str] = []
    for row in rows:
        score = _score_match(query_norm, row)
        if score > best_score:
            best_row = row
            best_score = score
        name = _pick(row, ("DISPLAY_NAME", "SEM_CUSTOM_SYMBOL", "SYMBOL_NAME", "SM_SYMBOL_NAME"))
        if name and len(best_names) < 5:
            best_names.append(name)

    if not best_row:
        raise RuntimeError("Could not match instrument from Dhan list.")

    if best_score < 200:
        raise ValueError(
            "Instrument not found. Try exact Dhan display name. "
            f"Examples: {', '.join(best_names)}"
        )

    security_id = _pick(best_row, ("SECURITY_ID", "SEM_SMST_SECURITY_ID"))
    if not security_id:
        raise RuntimeError("Matched row does not contain security id.")

    instrument = _pick(best_row, ("INSTRUMENT", "SEM_INSTRUMENT_NAME"))
    if not instrument:
        # Safe fallback for index options.
        instrument = "OPTIDX"

    display_name = _pick(
        best_row,
        ("DISPLAY_NAME", "SEM_CUSTOM_SYMBOL", "SYMBOL_NAME", "SM_SYMBOL_NAME"),
        default=instrument_name.upper(),
    )

    result = InstrumentRef(
        security_id=security_id,
        exchange_segment=exchange_segment,
        instrument=instrument,
        display_name=display_name,
    )
    cache_data = _safe_read_json(_resolved_cache_path()) or {"items": {}}
    cache_items = cache_data.setdefault("items", {})
    cache_items[cache_key] = {
        "cached_at": _now_epoch(),
        "security_id": result.security_id,
        "instrument": result.instrument,
        "display_name": result.display_name,
    }
    _safe_write_json(_resolved_cache_path(), cache_data)
    return result


def _ensure_dt(value: Optional[Any], tz: ZoneInfo) -> datetime:
    if value is None:
        raise ValueError("Datetime value cannot be None here.")
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=tz)
    if isinstance(value, str):
        txt = value.strip()
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
            try:
                parsed = datetime.strptime(txt, fmt)
                return parsed.replace(tzinfo=tz)
            except ValueError:
                continue
    raise ValueError(f"Invalid datetime format: {value!r}")


def _classify_zone(price_change: float, oi_change: float) -> str:
    if price_change > 0 and oi_change > 0:
        return "Long Buildup"
    if price_change < 0 and oi_change > 0:
        return "Short Buildup"
    if price_change > 0 and oi_change < 0:
        return "Short Covering"
    if price_change < 0 and oi_change < 0:
        return "Long Unwinding"
    return "Neutral"


def _format_indian_number(value: float, decimals: int = 0) -> str:
    if decimals > 0:
        text = f"{value:.{decimals}f}"
        int_part, frac_part = text.split(".")
    else:
        int_part = str(int(round(value)))
        frac_part = ""

    sign = "-" if int_part.startswith("-") else ""
    digits = int_part.lstrip("-")
    if len(digits) <= 3:
        out = digits
    else:
        out = digits[-3:]
        digits = digits[:-3]
        while digits:
            out = digits[-2:] + "," + out
            digits = digits[:-2]

    return f"{sign}{out}" if not frac_part else f"{sign}{out}.{frac_part}"


def _nse_headers() -> Dict[str, str]:
    return {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json,text/plain,*/*",
        "Referer": NSE_HOME_URL + "/",
    }


def _fetch_nse_all_indices(request_timeout: int = 8) -> List[Dict[str, Any]]:
    cache_rows = _IN_MEMORY_NSE_ALL_INDICES.get("rows", [])
    cache_ts = int(_IN_MEMORY_NSE_ALL_INDICES.get("cached_at", 0))
    if (
        isinstance(cache_rows, list)
        and cache_rows
        and (_now_epoch() - cache_ts) <= NSE_ALL_INDICES_CACHE_TTL_SECONDS
    ):
        return [row for row in cache_rows if isinstance(row, dict)]

    with requests.Session() as session:
        headers = _nse_headers()
        # NSE APIs require a priming request for cookies.
        session.get(NSE_HOME_URL, headers=headers, timeout=request_timeout)
        resp = session.get(NSE_ALL_INDICES_URL, headers=headers, timeout=request_timeout)
        resp.raise_for_status()
        payload = resp.json()
        rows = payload.get("data", []) if isinstance(payload, dict) else []
        if not isinstance(rows, list):
            rows = []
        clean_rows = [row for row in rows if isinstance(row, dict)]
        _IN_MEMORY_NSE_ALL_INDICES["cached_at"] = _now_epoch()
        _IN_MEMORY_NSE_ALL_INDICES["rows"] = clean_rows
        return clean_rows


def _find_index_row(rows: Sequence[Dict[str, Any]], names: Sequence[str]) -> Optional[Dict[str, Any]]:
    targets = {_normalize_text(name) for name in names}
    for row in rows:
        index_name = str(row.get("index", "")).strip()
        symbol_name = str(row.get("indexSymbol", "")).strip()
        if _normalize_text(index_name) in targets or _normalize_text(symbol_name) in targets:
            return row
    return None


def _derive_direction(
    last_price: Optional[float],
    open_price: Optional[float],
    change_pct: Optional[float],
    side_threshold_pct: float = 0.08,
) -> str:
    pct = change_pct
    if pct is None and last_price is not None and open_price not in (None, 0):
        pct = ((last_price - open_price) / open_price) * 100.0
    if pct is None:
        return "NA"
    if pct > side_threshold_pct:
        return "UP"
    if pct < -side_threshold_pct:
        return "DOWN"
    return "SIDE"


def _build_nse_market_context(
    timezone_name: str = "Asia/Kolkata",
    request_timeout: int = 8,
) -> Optional[Dict[str, Any]]:
    try:
        rows = _fetch_nse_all_indices(request_timeout=request_timeout)
    except Exception:
        return None
    if not rows:
        return None

    tz = ZoneInfo(timezone_name)
    now = datetime.now(tz)

    nifty_row = _find_index_row(rows, ("NIFTY 50", "NIFTY"))
    vix_row = _find_index_row(rows, ("INDIA VIX", "INDIAVIX"))
    bank_row = _find_index_row(rows, ("NIFTY BANK", "BANKNIFTY", "NIFTYBANK"))

    def row_num(row: Optional[Dict[str, Any]], key: str) -> Optional[float]:
        if not row:
            return None
        return _maybe_float(row.get(key))

    nifty_last = row_num(nifty_row, "last")
    nifty_open = row_num(nifty_row, "open")
    nifty_high = row_num(nifty_row, "high")
    nifty_low = row_num(nifty_row, "low")
    nifty_prev_close = row_num(nifty_row, "previousClose")
    nifty_change = row_num(nifty_row, "variation")
    nifty_change_pct = row_num(nifty_row, "percentChange")

    bank_last = row_num(bank_row, "last")
    bank_open = row_num(bank_row, "open")
    bank_change_pct = row_num(bank_row, "percentChange")

    return {
        "as_of": f"{now:%Y-%m-%d %H:%M}",
        "source": NSE_ALL_INDICES_URL,
        "nifty": {
            "last": nifty_last,
            "open": nifty_open,
            "high": nifty_high,
            "low": nifty_low,
            "previous_close": nifty_prev_close,
            "change": nifty_change,
            "change_pct": nifty_change_pct,
            "time": f"{now:%Y-%m-%d %H:%M}",
        },
        "india_vix": {
            "last": row_num(vix_row, "last"),
            "change": row_num(vix_row, "variation"),
            "change_pct": row_num(vix_row, "percentChange"),
            "time": f"{now:%Y-%m-%d %H:%M}",
        },
        "breadth": {
            "advances": int(row_num(nifty_row, "advances") or 0),
            "declines": int(row_num(nifty_row, "declines") or 0),
            "unchanged": int(row_num(nifty_row, "unchanged") or 0),
        },
        "banknifty": {
            "last": bank_last,
            "open": bank_open,
            "change_pct": bank_change_pct,
            "direction": _derive_direction(
                last_price=bank_last,
                open_price=bank_open,
                change_pct=bank_change_pct,
            ),
            "time": f"{now:%Y-%m-%d %H:%M}",
        },
    }


def _build_chain_leg_context(leg_payload: Dict[str, Any]) -> Dict[str, Any]:
    oi = _maybe_float(leg_payload.get("oi"))
    previous_oi = _maybe_float(leg_payload.get("previous_oi"))
    oi_change = (oi - previous_oi) if oi is not None and previous_oi is not None else None
    oi_change_pct = (
        ((oi_change / previous_oi) * 100.0) if oi_change is not None and previous_oi not in (None, 0) else None
    )

    volume = _maybe_float(leg_payload.get("volume"))
    previous_volume = _maybe_float(leg_payload.get("previous_volume"))
    volume_change = (
        (volume - previous_volume)
        if volume is not None and previous_volume is not None
        else None
    )
    volume_change_pct = (
        ((volume_change / previous_volume) * 100.0)
        if volume_change is not None and previous_volume not in (None, 0)
        else None
    )

    bid_price = _maybe_float(leg_payload.get("top_bid_price"))
    ask_price = _maybe_float(leg_payload.get("top_ask_price"))
    spread = (ask_price - bid_price) if ask_price is not None and bid_price is not None else None

    return {
        "last_price": _extract_leg_ltp(leg_payload),
        "average_price": _maybe_float(leg_payload.get("average_price")),
        "previous_close_price": _maybe_float(leg_payload.get("previous_close_price")),
        "implied_volatility": _maybe_float(leg_payload.get("implied_volatility")),
        "oi": int(round(oi)) if oi is not None else None,
        "previous_oi": int(round(previous_oi)) if previous_oi is not None else None,
        "oi_change": int(round(oi_change)) if oi_change is not None else None,
        "oi_change_pct": oi_change_pct,
        "volume": int(round(volume)) if volume is not None else None,
        "previous_volume": int(round(previous_volume)) if previous_volume is not None else None,
        "volume_change": int(round(volume_change)) if volume_change is not None else None,
        "volume_change_pct": volume_change_pct,
        "top_bid_price": bid_price,
        "top_bid_quantity": _maybe_float(leg_payload.get("top_bid_quantity")),
        "top_ask_price": ask_price,
        "top_ask_quantity": _maybe_float(leg_payload.get("top_ask_quantity")),
        "bid_ask_spread": spread,
        "greeks": leg_payload.get("greeks", {}) if isinstance(leg_payload.get("greeks"), dict) else {},
    }


def _aggregate_minute_candles(
    candles_1m: Sequence[Dict[str, Any]],
    interval_minutes: int = 5,
    max_candles: int = 6,
) -> List[Dict[str, Any]]:
    if interval_minutes <= 1:
        out = list(candles_1m)
        return out[-max_candles:] if max_candles > 0 else out
    buckets: Dict[datetime, Dict[str, Any]] = {}
    for candle in candles_1m:
        ts = str(candle.get("timestamp", "")).strip()
        if not ts:
            continue
        try:
            dt = datetime.strptime(ts, "%Y-%m-%d %H:%M")
        except ValueError:
            continue
        bucket_dt = dt.replace(minute=(dt.minute // interval_minutes) * interval_minutes, second=0, microsecond=0)
        bucket = buckets.setdefault(
            bucket_dt,
            {
                "timestamp": f"{bucket_dt:%Y-%m-%d %H:%M}",
                "open": float(candle.get("open", 0.0)),
                "high": float(candle.get("high", 0.0)),
                "low": float(candle.get("low", 0.0)),
                "close": float(candle.get("close", 0.0)),
                "volume": 0,
            },
        )
        bucket["high"] = max(bucket["high"], float(candle.get("high", bucket["high"])))
        bucket["low"] = min(bucket["low"], float(candle.get("low", bucket["low"])))
        bucket["close"] = float(candle.get("close", bucket["close"]))
        bucket["volume"] += int(round(float(candle.get("volume", 0.0))))

    out = [buckets[key] for key in sorted(buckets.keys())]
    if max_candles > 0:
        out = out[-max_candles:]
    return out


def _fetch_intraday_payload(
    access_token: str,
    client_id: str,
    instrument_ref: InstrumentRef,
    from_dt: datetime,
    to_dt: datetime,
    request_timeout: int,
    with_oi: bool = True,
) -> Dict[str, Any]:
    payload = {
        "securityId": str(instrument_ref.security_id),
        "exchangeSegment": instrument_ref.exchange_segment,
        "instrument": instrument_ref.instrument,
        "interval": "1",
        "oi": bool(with_oi),
        "fromDate": from_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "toDate": to_dt.strftime("%Y-%m-%d %H:%M:%S"),
    }
    headers = _build_headers(access_token, client_id)
    response = requests.post(
        INTRADAY_URL,
        json=payload,
        headers=headers,
        timeout=request_timeout,
    )
    if not response.ok:
        raise RuntimeError(
            f"Intraday API failed ({response.status_code}): {response.text[:400]}"
        )
    return response.json()


def _build_buildup_from_intraday(
    data: Dict[str, Any],
    tz: ZoneInfo,
    max_rows: int,
    include_oi: bool = True,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    timestamps = data.get("timestamp", []) or []
    opens = data.get("open", []) or []
    highs = data.get("high", []) or []
    lows = data.get("low", []) or []
    closes = data.get("close", []) or []
    volumes = data.get("volume", []) or []

    ois_raw = data.get("open_interest", []) or []
    if include_oi:
        ois = ois_raw if len(ois_raw) == len(timestamps) else [0.0] * len(timestamps)
    else:
        ois = ois_raw if len(ois_raw) == len(timestamps) else [0.0] * len(timestamps)

    lengths = {len(timestamps), len(opens), len(highs), len(lows), len(closes), len(volumes), len(ois)}
    if len(lengths) != 1 or 0 in lengths:
        raise RuntimeError("Unexpected intraday response format from Dhan.")

    buckets: Dict[datetime, Dict[str, Any]] = {}
    minute_candles: List[Dict[str, Any]] = []
    prev_tick_oi: Optional[float] = None
    points = sorted(zip(timestamps, opens, highs, lows, closes, volumes, ois), key=lambda x: x[0])
    for ts, o, h, l, c, v, oi in points:
        tick_dt = datetime.fromtimestamp(int(ts), tz=timezone.utc).astimezone(tz)
        minute_candles.append(
            {
                "timestamp": f"{tick_dt:%Y-%m-%d %H:%M}",
                "open": round(float(o), 2),
                "high": round(float(h), 2),
                "low": round(float(l), 2),
                "close": round(float(c), 2),
                "oi": int(round(float(oi))),
                "volume": int(round(float(v))),
            }
        )

        start_min = (tick_dt.minute // 15) * 15
        interval_start = tick_dt.replace(minute=start_min, second=0, microsecond=0)

        bucket = buckets.setdefault(
            interval_start,
            {
                "interval_start": interval_start,
                "interval_end": interval_start + timedelta(minutes=15),
                "low": float(l),
                "high": float(h),
                "close": float(c),
                "volume": 0.0,
                "oi_end": float(oi),
                "fresh": 0.0,
                "square_off": 0.0,
            },
        )

        bucket["low"] = min(bucket["low"], float(l))
        bucket["high"] = max(bucket["high"], float(h))
        bucket["close"] = float(c)
        bucket["volume"] += float(v)
        bucket["oi_end"] = float(oi)

        if prev_tick_oi is not None:
            oi_delta = float(oi) - prev_tick_oi
            if oi_delta >= 0:
                bucket["fresh"] += oi_delta
            else:
                bucket["square_off"] += abs(oi_delta)
        prev_tick_oi = float(oi)

    rows_sorted = [buckets[key] for key in sorted(buckets.keys())]
    output_rows: List[Dict[str, Any]] = []
    prev_close: Optional[float] = None
    prev_oi_end: Optional[float] = None

    for row in rows_sorted:
        oi_end = row["oi_end"]
        close = row["close"]
        if prev_oi_end is None:
            oi_change = 0.0
            oi_change_pct = 0.0
        else:
            oi_change = oi_end - prev_oi_end
            oi_change_pct = (oi_change / prev_oi_end * 100.0) if prev_oi_end else 0.0
        price_change = 0.0 if prev_close is None else close - prev_close

        output_rows.append(
            {
                "interval": f"{row['interval_start']:%H:%M} - {row['interval_end']:%H:%M}",
                "trading_zone": _classify_zone(price_change, oi_change) if prev_close is not None else "Neutral",
                "price_range": [round(row["low"], 2), round(row["high"], 2)],
                "oi": int(round(oi_end)),
                "oi_change_pct": round(oi_change_pct, 2),
                "fresh": int(round(row["fresh"])),
                "square_off": int(round(row["square_off"])),
                "volume": int(round(row["volume"])),
            }
        )

        prev_close = close
        prev_oi_end = oi_end

    if max_rows > 0:
        output_rows = output_rows[-max_rows:]
    output_rows = list(reversed(output_rows))
    return output_rows, minute_candles


def get_15min_buildup(
    access_token: str,
    client_id: str,
    instrument_name: str,
    exchange_segment: str = "NSE_FNO",
    security_id: Optional[str] = None,
    instrument: str = "OPTIDX",
    resolved_name: Optional[str] = None,
    from_datetime: Optional[Any] = None,
    to_datetime: Optional[Any] = None,
    timezone_name: str = "Asia/Kolkata",
    max_rows: int = 9,
    request_timeout: int = 20,
) -> Dict[str, Any]:
    """
    Build 15-minute buildup data like Dhan Option Build-up table.

    Parameters
    ----------
    access_token : str
        Dhan access token.
    client_id : str
        Dhan client ID.
    instrument_name : str
        Example: "NIFTY 24 MAR 23300 PUT"
    exchange_segment : str
        Default "NSE_FNO" for index/stock options.
    security_id : str|None
        Optional direct security_id. If provided, instrument resolution is skipped.
    instrument : str
        Instrument type used with direct security_id. Default "OPTIDX".
    resolved_name : str|None
        Optional display name when using direct security_id.
    from_datetime, to_datetime : datetime|str|None
        If None, function fetches today's data from 09:15 till now.
    timezone_name : str
        Exchange timezone. Default Asia/Kolkata.
    max_rows : int
        Number of latest 15-min rows in output.
    request_timeout : int
        HTTP timeout in seconds.
    """
    if not access_token or not client_id:
        raise ValueError("Both access_token and client_id are required.")

    tz = ZoneInfo(timezone_name)
    now = datetime.now(tz)
    if to_datetime is None:
        to_dt = now
    else:
        to_dt = _ensure_dt(to_datetime, tz)
    if from_datetime is None:
        from_dt = to_dt.replace(hour=9, minute=15, second=0, microsecond=0)
    else:
        from_dt = _ensure_dt(from_datetime, tz)
    if from_dt >= to_dt:
        raise ValueError("from_datetime must be earlier than to_datetime.")

    if security_id:
        instrument_ref = InstrumentRef(
            security_id=str(security_id).strip(),
            exchange_segment=exchange_segment,
            instrument=instrument,
            display_name=resolved_name or instrument_name.upper(),
        )
    else:
        instrument_ref = _resolve_instrument(
            instrument_name,
            exchange_segment=exchange_segment,
            access_token=access_token,
            client_id=client_id,
            request_timeout=request_timeout,
            reference_dt=to_dt,
        )

    data = _fetch_intraday_payload(
        access_token=access_token,
        client_id=client_id,
        instrument_ref=instrument_ref,
        from_dt=from_dt,
        to_dt=to_dt,
        request_timeout=request_timeout,
        with_oi=True,
    )
    output_rows, minute_candles = _build_buildup_from_intraday(
        data=data,
        tz=tz,
        max_rows=max_rows,
        include_oi=True,
    )

    return {
        "instrument": {
            "name_input": instrument_name,
            "resolved_name": instrument_ref.display_name,
            "security_id": instrument_ref.security_id,
            "exchange_segment": instrument_ref.exchange_segment,
            "instrument": instrument_ref.instrument,
        },
        "range": {
            "from": from_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "to": to_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "timezone": timezone_name,
        },
        "rows": output_rows,
        "candles_1m": minute_candles,
    }


def _maybe_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _extract_spot_price_from_chain(chain_payload: Dict[str, Any]) -> Optional[float]:
    data = chain_payload.get("data", {}) if isinstance(chain_payload, dict) else {}
    containers: List[Dict[str, Any]] = []
    if isinstance(chain_payload, dict):
        containers.append(chain_payload)
    if isinstance(data, dict):
        containers.append(data)
        underlying_data = data.get("underlying")
        if isinstance(underlying_data, dict):
            containers.append(underlying_data)

    keys = (
        "last_price",
        "lastPrice",
        "underlying_price",
        "underlyingValue",
        "underlying_value",
        "spot_price",
        "index_price",
        "ltp",
    )
    for container in containers:
        for key in keys:
            value = _maybe_float(container.get(key))
            if value is not None:
                return value
    return None


def _extract_leg_ltp(leg_payload: Dict[str, Any]) -> Optional[float]:
    for key in ("last_price", "lastPrice", "ltp", "close", "price"):
        value = _maybe_float(leg_payload.get(key))
        if value is not None:
            return value
    return None


def _format_strike_value(strike: float) -> str:
    if float(strike).is_integer():
        return str(int(round(strike)))
    return f"{strike:.2f}".rstrip("0").rstrip(".")


def _build_option_display_name(
    underlying: str,
    expiry_iso: str,
    strike: float,
    option_type: str,
) -> str:
    expiry_dt = datetime.strptime(expiry_iso, "%Y-%m-%d")
    return (
        f"{underlying} {expiry_dt.day} {expiry_dt.strftime('%b').upper()} "
        f"{_format_strike_value(strike)} {option_type}"
    )


def _strike_label(offset: int) -> str:
    if offset == 0:
        return "ATM"
    return f"ATM{offset:+d}"


def _minutes_left_to_market_close(now: datetime) -> int:
    close_dt = now.replace(hour=15, minute=30, second=0, microsecond=0)
    if now >= close_dt:
        return 0
    seconds_left = int((close_dt - now).total_seconds())
    return max(0, (seconds_left + 59) // 60)


def _fetch_symbol_snapshot(
    access_token: str,
    client_id: str,
    symbol_candidates: Sequence[str],
    exchange_candidates: Sequence[str],
    timezone_name: str,
    candles_count: int,
    request_timeout: int,
) -> Optional[Dict[str, Any]]:
    tz = ZoneInfo(timezone_name)
    now = datetime.now(tz)
    from_dt = now - timedelta(minutes=max(60, candles_count * 6))

    for exchange_segment in exchange_candidates:
        for symbol in symbol_candidates:
            try:
                instrument_ref = _resolve_instrument(
                    symbol,
                    exchange_segment=exchange_segment,
                    request_timeout=request_timeout,
                    reference_dt=now,
                )
                data = _fetch_intraday_payload(
                    access_token=access_token,
                    client_id=client_id,
                    instrument_ref=instrument_ref,
                    from_dt=from_dt,
                    to_dt=now,
                    request_timeout=request_timeout,
                    with_oi=False,
                )
                _, minute_candles = _build_buildup_from_intraday(
                    data=data,
                    tz=tz,
                    max_rows=1,
                    include_oi=False,
                )
                if not minute_candles:
                    continue
                candles = minute_candles[-candles_count:]
                latest = candles[-1]
                return {
                    "symbol": symbol,
                    "resolved_name": instrument_ref.display_name,
                    "security_id": instrument_ref.security_id,
                    "time": latest["timestamp"],
                    "price": latest["close"],
                    "candles_1m": candles,
                }
            except Exception:
                continue
    return None


def get_option_market_snapshot(
    access_token: str,
    client_id: str,
    instrument_name: str,
    exchange_segment: str = "NSE_FNO",
    timezone_name: str = "Asia/Kolkata",
    max_rows: int = 9,
    last_spot_candles: int = 10,
    request_timeout: int = 20,
) -> Dict[str, Any]:
    """
    Combined market pack:
    - CE/PE for same expiry at ATM, ATM-1, ATM+1
    - Latest OI change% + volume per CE/PE (from latest 15-min row)
    - NIFTY spot last 1-minute candles
    - Current spot price/time
    - India VIX current
    - Minutes left to market close
    """
    if not access_token or not client_id:
        raise ValueError("Both access_token and client_id are required.")

    parsed = _parse_named_option(instrument_name)
    if not parsed:
        raise ValueError(
            "instrument_name must look like: NIFTY 30 MAR 22700 PUT"
        )

    underlying = parsed["underlying"]
    underlying_info = UNDERLYING_CHAIN_MAP.get(underlying)
    if not underlying_info:
        raise ValueError(
            f"Unsupported underlying '{underlying}'. Supported: {', '.join(sorted(UNDERLYING_CHAIN_MAP.keys()))}"
        )

    tz = ZoneInfo(timezone_name)
    now = datetime.now(tz)
    from_dt = now.replace(hour=9, minute=15, second=0, microsecond=0)

    underlying_scrip, underlying_seg = underlying_info
    headers = _build_headers(access_token, client_id)

    expiry_resp = requests.post(
        OPTIONCHAIN_EXPIRYLIST_URL,
        json={"UnderlyingScrip": underlying_scrip, "UnderlyingSeg": underlying_seg},
        headers=headers,
        timeout=request_timeout,
    )
    if not expiry_resp.ok:
        raise RuntimeError(
            f"Expiry list API failed ({expiry_resp.status_code}): {expiry_resp.text[:300]}"
        )
    expiry_data = expiry_resp.json()
    expiries = expiry_data.get("data", [])
    if not isinstance(expiries, list) or not expiries:
        raise RuntimeError("No expiries returned from option chain expiry list API.")

    expiry = _select_expiry(expiries, parsed["date_token"], parsed["month"], now)
    if not expiry:
        raise RuntimeError("Could not map instrument month/date to an expiry.")

    chain_resp = requests.post(
        OPTIONCHAIN_URL,
        json={
            "UnderlyingScrip": underlying_scrip,
            "UnderlyingSeg": underlying_seg,
            "Expiry": expiry,
        },
        headers=headers,
        timeout=request_timeout,
    )
    if not chain_resp.ok:
        raise RuntimeError(
            f"Option chain API failed ({chain_resp.status_code}): {chain_resp.text[:300]}"
        )
    chain_payload = chain_resp.json()
    chain = chain_payload.get("data", {}).get("oc", {})
    if not isinstance(chain, dict) or not chain:
        raise RuntimeError("Option chain response does not contain strikes.")

    strike_pairs: List[Tuple[float, str]] = []
    for strike_key in chain.keys():
        strike_val = _maybe_float(strike_key)
        if strike_val is not None:
            strike_pairs.append((strike_val, strike_key))
    strike_pairs.sort(key=lambda x: x[0])
    if not strike_pairs:
        raise RuntimeError("No numeric strikes found in option chain response.")

    spot_price_from_chain = _extract_spot_price_from_chain(chain_payload)
    spot_reference = spot_price_from_chain if spot_price_from_chain is not None else float(parsed["strike"])
    atm_idx = min(
        range(len(strike_pairs)),
        key=lambda idx: abs(strike_pairs[idx][0] - spot_reference),
    )
    selected_indices = sorted(
        idx for idx in (atm_idx - 1, atm_idx, atm_idx + 1) if 0 <= idx < len(strike_pairs)
    )

    options: List[Dict[str, Any]] = []
    strike_buckets: List[Dict[str, Any]] = []
    for idx in selected_indices:
        strike_value, strike_key = strike_pairs[idx]
        offset = idx - atm_idx
        label = _strike_label(offset)
        strike_buckets.append(
            {
                "label": label,
                "offset": offset,
                "strike": strike_value,
            }
        )

        strike_node = chain.get(strike_key, {})
        for option_type, side_key in (("CALL", "ce"), ("PUT", "pe")):
            leg_payload = strike_node.get(side_key, {}) if isinstance(strike_node, dict) else {}
            if not isinstance(leg_payload, dict):
                leg_payload = {}
            security_id = str(leg_payload.get("security_id", "")).strip()
            if not security_id:
                continue

            display_name = _pick(
                leg_payload,
                ("display_name", "trading_symbol", "symbol", "name"),
                default=_build_option_display_name(
                    underlying=underlying,
                    expiry_iso=expiry,
                    strike=strike_value,
                    option_type=option_type,
                ),
            )
            leg_ref = InstrumentRef(
                security_id=security_id,
                exchange_segment=exchange_segment,
                instrument="OPTIDX",
                display_name=display_name,
            )

            leg_record: Dict[str, Any] = {
                "label": label,
                "offset": offset,
                "strike": strike_value,
                "option_type": option_type,
                "instrument_name": display_name,
                "security_id": security_id,
                "last_price": _extract_leg_ltp(leg_payload),
                "oi_change_pct": None,
                "volume": None,
                "trading_zone": None,
                "rows": [],
            }
            try:
                intraday_data = _fetch_intraday_payload(
                    access_token=access_token,
                    client_id=client_id,
                    instrument_ref=leg_ref,
                    from_dt=from_dt,
                    to_dt=now,
                    request_timeout=request_timeout,
                    with_oi=True,
                )
                leg_rows, leg_candles = _build_buildup_from_intraday(
                    data=intraday_data,
                    tz=tz,
                    max_rows=max_rows,
                    include_oi=True,
                )
                leg_record["rows"] = leg_rows
                if leg_rows:
                    leg_record["oi_change_pct"] = leg_rows[0]["oi_change_pct"]
                    leg_record["volume"] = leg_rows[0]["volume"]
                    leg_record["trading_zone"] = leg_rows[0]["trading_zone"]
                if leg_record["last_price"] is None and leg_candles:
                    leg_record["last_price"] = leg_candles[-1]["close"]
            except Exception as exc:
                leg_record["error"] = str(exc)
            options.append(leg_record)

    spot_snapshot = _fetch_symbol_snapshot(
        access_token=access_token,
        client_id=client_id,
        symbol_candidates=("NIFTY 50", "NIFTY"),
        exchange_candidates=("IDX_I", "NSE_IDX"),
        timezone_name=timezone_name,
        candles_count=last_spot_candles,
        request_timeout=request_timeout,
    )
    if spot_snapshot is None:
        spot_snapshot = {
            "symbol": "NIFTY 50",
            "resolved_name": "NIFTY 50",
            "security_id": "",
            "time": f"{now:%Y-%m-%d %H:%M}",
            "price": round(spot_price_from_chain, 2) if spot_price_from_chain is not None else None,
            "candles_1m": [],
        }

    vix_snapshot = _fetch_symbol_snapshot(
        access_token=access_token,
        client_id=client_id,
        symbol_candidates=("INDIA VIX", "INDIAVIX"),
        exchange_candidates=("IDX_I", "NSE_IDX"),
        timezone_name=timezone_name,
        candles_count=1,
        request_timeout=request_timeout,
    )

    atm_strike = strike_pairs[atm_idx][0]
    return {
        "as_of": f"{now:%Y-%m-%d %H:%M}",
        "underlying": underlying,
        "expiry": expiry,
        "atm_strike": atm_strike,
        "spot": spot_snapshot,
        "india_vix": vix_snapshot,
        "minutes_to_close": _minutes_left_to_market_close(now),
        "strike_buckets": strike_buckets,
        "options": options,
    }


def get_intraday_trade_context(
    access_token: str,
    client_id: str,
    instrument_name: str,
    exchange_segment: str = "NSE_FNO",
    timezone_name: str = "Asia/Kolkata",
    strikes_each_side: int = 3,
    spot_1m_candles: int = 15,
    spot_5m_candles: int = 6,
    include_nse_context: bool = True,
    request_timeout: int = 12,
) -> Dict[str, Any]:
    """
    Compact intraday context pack for decision-making.

    Includes:
    - NIFTY spot context (LTP/Open/High/Low/Prev Close when available)
    - India VIX, market breadth and BankNifty direction (from NSE allIndices)
    - Option chain window around ATM (ATM ± strikes_each_side)
      with IV, OI, OI delta, volume and bid/ask stats for CE/PE
    - Spot 1-minute and derived 5-minute candles (best effort)
    """
    if not access_token or not client_id:
        raise ValueError("Both access_token and client_id are required.")

    parsed = _parse_named_option(instrument_name)
    if not parsed:
        raise ValueError("instrument_name must look like: NIFTY 30 MAR 22700 PUT")

    underlying = parsed["underlying"]
    underlying_info = UNDERLYING_CHAIN_MAP.get(underlying)
    if not underlying_info:
        raise ValueError(
            f"Unsupported underlying '{underlying}'. Supported: {', '.join(sorted(UNDERLYING_CHAIN_MAP.keys()))}"
        )

    tz = ZoneInfo(timezone_name)
    now = datetime.now(tz)

    nse_context = (
        _build_nse_market_context(
            timezone_name=timezone_name,
            request_timeout=max(5, min(10, request_timeout)),
        )
        if include_nse_context
        else None
    )

    expiry: Optional[str] = None
    atm_strike: Optional[float] = _maybe_float(parsed.get("strike"))
    strike_rows: List[Dict[str, Any]] = []
    instrument_lookup: Dict[str, Dict[str, Any]] = {}
    spot_price_from_chain: Optional[float] = None
    chain_error: Optional[str] = None

    try:
        underlying_scrip, underlying_seg = underlying_info
        headers = _build_headers(access_token, client_id)

        expiry_resp = requests.post(
            OPTIONCHAIN_EXPIRYLIST_URL,
            json={"UnderlyingScrip": underlying_scrip, "UnderlyingSeg": underlying_seg},
            headers=headers,
            timeout=request_timeout,
        )
        if not expiry_resp.ok:
            raise RuntimeError(
                f"Expiry list API failed ({expiry_resp.status_code}): {expiry_resp.text[:300]}"
            )
        expiry_data = expiry_resp.json()
        expiries = expiry_data.get("data", [])
        if not isinstance(expiries, list) or not expiries:
            raise RuntimeError("No expiries returned from option chain expiry list API.")

        expiry = _select_expiry(expiries, parsed["date_token"], parsed["month"], now)
        if not expiry:
            raise RuntimeError("Could not map instrument month/date to an expiry.")

        chain_resp = requests.post(
            OPTIONCHAIN_URL,
            json={
                "UnderlyingScrip": underlying_scrip,
                "UnderlyingSeg": underlying_seg,
                "Expiry": expiry,
            },
            headers=headers,
            timeout=request_timeout,
        )
        if not chain_resp.ok:
            raise RuntimeError(
                f"Option chain API failed ({chain_resp.status_code}): {chain_resp.text[:300]}"
            )
        chain_payload = chain_resp.json()
        chain = chain_payload.get("data", {}).get("oc", {})
        if not isinstance(chain, dict) or not chain:
            raise RuntimeError("Option chain response does not contain strikes.")

        strike_pairs: List[Tuple[float, str]] = []
        for strike_key in chain.keys():
            strike_val = _maybe_float(strike_key)
            if strike_val is not None:
                strike_pairs.append((strike_val, strike_key))
        strike_pairs.sort(key=lambda x: x[0])
        if not strike_pairs:
            raise RuntimeError("No numeric strikes found in option chain response.")

        spot_price_from_chain = _extract_spot_price_from_chain(chain_payload)
        spot_reference = (
            spot_price_from_chain if spot_price_from_chain is not None else float(parsed["strike"])
        )
        atm_idx = min(range(len(strike_pairs)), key=lambda idx: abs(strike_pairs[idx][0] - spot_reference))
        atm_strike = strike_pairs[atm_idx][0]

        wing = max(0, int(strikes_each_side))
        start_idx = max(0, atm_idx - wing)
        end_idx = min(len(strike_pairs), atm_idx + wing + 1)
        selected_indices = list(range(start_idx, end_idx))

        for idx in selected_indices:
            strike_value, strike_key = strike_pairs[idx]
            offset = idx - atm_idx
            label = _strike_label(offset)
            strike_node = chain.get(strike_key, {})
            if not isinstance(strike_node, dict):
                strike_node = {}

            row: Dict[str, Any] = {
                "label": label,
                "offset": offset,
                "strike": strike_value,
                "ce": {},
                "pe": {},
            }
            for option_type, side_key in (("CALL", "ce"), ("PUT", "pe")):
                leg_payload = strike_node.get(side_key, {})
                if not isinstance(leg_payload, dict):
                    leg_payload = {}
                security_id = str(leg_payload.get("security_id", "")).strip()
                display_name = _pick(
                    leg_payload,
                    ("display_name", "trading_symbol", "symbol", "name"),
                    default=_build_option_display_name(
                        underlying=underlying,
                        expiry_iso=expiry,
                        strike=strike_value,
                        option_type=option_type,
                    ),
                )
                leg_context = _build_chain_leg_context(leg_payload)
                leg_context.update(
                    {
                        "option_type": option_type,
                        "instrument_name": display_name,
                        "security_id": security_id or None,
                        "strike": strike_value,
                        "offset": offset,
                        "label": label,
                    }
                )
                row[side_key] = leg_context

                for lookup_key in {
                    str(display_name).strip().upper(),
                    _normalize_text(display_name),
                }:
                    if lookup_key:
                        instrument_lookup[lookup_key] = leg_context
            strike_rows.append(row)
    except Exception as exc:
        chain_error = str(exc)

    spot_snapshot: Optional[Dict[str, Any]] = None
    spot_error: Optional[str] = None
    try:
        spot_snapshot = _fetch_symbol_snapshot(
            access_token=access_token,
            client_id=client_id,
            symbol_candidates=(f"{underlying} 50", underlying),
            exchange_candidates=("IDX_I", "NSE_IDX"),
            timezone_name=timezone_name,
            candles_count=max(spot_1m_candles, 1),
            request_timeout=request_timeout,
        )
    except Exception as exc:
        spot_error = str(exc)

    spot_1m = (spot_snapshot or {}).get("candles_1m", []) or []
    spot_5m = _aggregate_minute_candles(
        candles_1m=spot_1m,
        interval_minutes=5,
        max_candles=max(spot_5m_candles, 0),
    )

    nifty_ctx = (nse_context or {}).get("nifty", {}) if isinstance(nse_context, dict) else {}
    spot_last = _maybe_float(nifty_ctx.get("last"))
    if spot_last is None and spot_snapshot and spot_snapshot.get("price") is not None:
        spot_last = _maybe_float(spot_snapshot.get("price"))
    if spot_last is None:
        spot_last = spot_price_from_chain

    spot_open = _maybe_float(nifty_ctx.get("open"))
    spot_high = _maybe_float(nifty_ctx.get("high"))
    spot_low = _maybe_float(nifty_ctx.get("low"))
    spot_prev_close = _maybe_float(nifty_ctx.get("previous_close"))
    if spot_1m:
        if spot_open is None:
            spot_open = _maybe_float(spot_1m[0].get("open"))
        if spot_high is None:
            highs = [_maybe_float(c.get("high")) for c in spot_1m]
            highs = [h for h in highs if h is not None]
            spot_high = max(highs) if highs else None
        if spot_low is None:
            lows = [_maybe_float(c.get("low")) for c in spot_1m]
            lows = [l for l in lows if l is not None]
            spot_low = min(lows) if lows else None

    spot_change = None
    spot_change_pct = None
    if spot_last is not None and spot_prev_close not in (None, 0):
        spot_change = spot_last - spot_prev_close
        spot_change_pct = (spot_change / spot_prev_close) * 100.0

    return {
        "as_of": f"{now:%Y-%m-%d %H:%M}",
        "underlying": underlying,
        "expiry": expiry,
        "atm_strike": atm_strike,
        "minutes_to_close": _minutes_left_to_market_close(now),
        "spot": {
            "price": spot_last,
            "open": spot_open,
            "high": spot_high,
            "low": spot_low,
            "previous_close": spot_prev_close,
            "change": spot_change,
            "change_pct": spot_change_pct,
            "time": (
                (spot_snapshot or {}).get("time")
                or (nifty_ctx.get("time") if isinstance(nifty_ctx, dict) else None)
                or f"{now:%Y-%m-%d %H:%M}"
            ),
            "candles_1m": spot_1m[-max(spot_1m_candles, 0):] if spot_1m_candles > 0 else [],
            "candles_5m": spot_5m,
            "source": (
                "NSE allIndices + Dhan intraday"
                if nse_context and spot_snapshot
                else "NSE allIndices"
                if nse_context
                else "Dhan intraday/chain"
            ),
        },
        "india_vix": (nse_context or {}).get("india_vix") if isinstance(nse_context, dict) else None,
        "breadth": (nse_context or {}).get("breadth") if isinstance(nse_context, dict) else None,
        "banknifty": (nse_context or {}).get("banknifty") if isinstance(nse_context, dict) else None,
        "chain_spot_price": spot_price_from_chain,
        "strikes": strike_rows,
        "instrument_lookup": instrument_lookup,
        "errors": {
            "chain": chain_error,
            "spot": spot_error,
        },
    }


def get_nse_market_context(
    timezone_name: str = "Asia/Kolkata",
    request_timeout: int = 8,
) -> Dict[str, Any]:
    context = _build_nse_market_context(
        timezone_name=timezone_name,
        request_timeout=request_timeout,
    )
    if context is None:
        tz = ZoneInfo(timezone_name)
        now = datetime.now(tz)
        return {
            "as_of": f"{now:%Y-%m-%d %H:%M}",
            "source": NSE_ALL_INDICES_URL,
            "nifty": None,
            "india_vix": None,
            "breadth": None,
            "banknifty": None,
        }
    return context


def print_option_market_snapshot(
    snapshot: Dict[str, Any],
    last_spot_candles: int = 10,
) -> None:
    spot = snapshot.get("spot", {}) or {}
    vix = snapshot.get("india_vix", {}) or {}

    print(f"As of: {snapshot.get('as_of', '-')}")
    if spot.get("price") is not None:
        print(f"NIFTY Spot: {float(spot['price']):.2f} ({spot.get('time', '-')})")
    else:
        print("NIFTY Spot: NA")
    if vix.get("price") is not None:
        print(f"India VIX: {float(vix['price']):.2f} ({vix.get('time', '-')})")
    else:
        print("India VIX: NA")
    atm_strike_value = _maybe_float(snapshot.get("atm_strike"))
    atm_strike_text = _format_strike_value(atm_strike_value) if atm_strike_value is not None else "-"
    print(
        f"Expiry: {snapshot.get('expiry', '-')} | ATM Strike: {atm_strike_text}"
    )
    print(f"Minutes left to market close: {snapshot.get('minutes_to_close', 0)}")
    print()

    options = snapshot.get("options", []) or []
    by_key: Dict[Tuple[int, str], Dict[str, Any]] = {}
    for item in options:
        by_key[(int(item.get("offset", 0)), str(item.get("option_type", "")))] = item

    offsets = sorted({int(item.get("offset", 0)) for item in options})
    print("CE + PE (ATM, ATM-1, ATM+1)")
    print(
        "Label   Strike       CE OI%     CE Volume   CE Zone          PE OI%     PE Volume   PE Zone"
    )
    print("-" * 98)
    for offset in offsets:
        ce = by_key.get((offset, "CALL"), {})
        pe = by_key.get((offset, "PUT"), {})
        strike = ce.get("strike", pe.get("strike"))
        strike_text = _format_strike_value(float(strike)) if strike is not None else "-"
        ce_oi = ce.get("oi_change_pct")
        pe_oi = pe.get("oi_change_pct")
        ce_vol = ce.get("volume")
        pe_vol = pe.get("volume")
        ce_zone = ce.get("trading_zone", "-") or "-"
        pe_zone = pe.get("trading_zone", "-") or "-"
        print(
            f"{_strike_label(offset):<6} "
            f"{strike_text:>7} "
            f"{(f'{ce_oi:.2f}' if ce_oi is not None else 'NA'):>12} "
            f"{(_format_indian_number(ce_vol) if ce_vol is not None else 'NA'):>11} "
            f"{ce_zone:<15} "
            f"{(f'{pe_oi:.2f}' if pe_oi is not None else 'NA'):>10} "
            f"{(_format_indian_number(pe_vol) if pe_vol is not None else 'NA'):>11} "
            f"{pe_zone:<15}"
        )

    candles = spot.get("candles_1m", []) or []
    if last_spot_candles > 0 and candles:
        candles_to_show = candles[-last_spot_candles:]
        print()
        print(f"NIFTY Spot - Last {len(candles_to_show)} x 1-minute candles")
        print("Timestamp         Open     High      Low    Close      Volume")
        print("-" * 62)
        for candle in candles_to_show:
            print(
                f"{candle['timestamp']:<16} "
                f"{candle['open']:>7.2f} "
                f"{candle['high']:>8.2f} "
                f"{candle['low']:>8.2f} "
                f"{candle['close']:>8.2f} "
                f"{_format_indian_number(candle['volume']):>11}"
            )


def print_buildup_table(result: Dict[str, Any], last_candles: int = 50) -> None:
    instrument = result["instrument"]["resolved_name"]
    print(instrument)
    print(
        "Interval       Trading Zone      Price Range          OI         OI Chg(%)   Fresh      Square-Off   Volume"
    )
    print("-" * 108)
    for row in result["rows"]:
        price_range = f"{row['price_range'][0]:.2f} - {row['price_range'][1]:.2f}"
        print(
            f"{row['interval']:<14} "
            f"{row['trading_zone']:<16} "
            f"{price_range:<20} "
            f"{_format_indian_number(row['oi']):>10} "
            f"{row['oi_change_pct']:>10.2f} "
            f"{_format_indian_number(row['fresh']):>10} "
            f"{_format_indian_number(row['square_off']):>12} "
            f"{_format_indian_number(row['volume']):>10}"
        )

    candles = result.get("candles_1m", []) or []
    if last_candles > 0 and candles:
        candles_to_show = candles[-last_candles:]
        print()
        print(f"Last {len(candles_to_show)} x 1-minute candles")
        print("Timestamp         Open     High      Low    Close         OI      Volume")
        print("-" * 72)
        for candle in candles_to_show:
            print(
                f"{candle['timestamp']:<16} "
                f"{candle['open']:>7.2f} "
                f"{candle['high']:>8.2f} "
                f"{candle['low']:>8.2f} "
                f"{candle['close']:>8.2f} "
                f"{_format_indian_number(candle['oi']):>10} "
                f"{_format_indian_number(candle['volume']):>11}"
            )


def get_buildup_html(
    result: Dict[str, Any],
    spot_price: Optional[float] = None,
) -> str:
    instrument_name = result["instrument"]["resolved_name"].upper()

    # Calculate option last price and change from today's open as fallback
    candles = result.get("candles_1m", [])
    option_last_price = 0.0
    option_change = 0.0
    option_change_pct = 0.0
    if candles:
        today_open = candles[0]["open"]
        option_last_price = candles[-1]["close"]
        option_change = option_last_price - today_open
        if today_open > 0:
            option_change_pct = (option_change / today_open) * 100.0

    # Format changes
    sign_str = "+" if option_change >= 0 else ""
    price_change_str = f"{sign_str}{option_change:.2f}"
    pct_change_str = f"{sign_str}{option_change_pct:.2f}%"
    change_color = "#1ea58d" if option_change >= 0 else "#d85b5b"
    arrow = "&#8599;" if option_change >= 0 else "&#8600;"

    # Spot price formatting
    spot_str = f"{spot_price:,.2f}" if spot_price is not None else "NA"

    html = f"""
    <div style="font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; max-width: 940px; margin: 16px 0; border: 1px solid #d7dce2; overflow: hidden; background: #ffffff;">
      <div style="display: flex; justify-content: space-between; align-items: center; padding: 10px 12px; border-bottom: 1px solid #d7dce2; font-size: 15px;">
        <div style="display: flex; align-items: baseline;">
          <span style="font-weight: 700; font-size: 29px; color: #1f2732;">{instrument_name}</span>
          <span style="font-weight: 600; margin-left: 16px; font-size: 21px; color: #2f3947;">{option_last_price:.2f}</span>
          <span style="margin-left: 10px; color: {change_color}; font-size: 18px; font-weight: 600;">{price_change_str} ({pct_change_str}) {arrow}</span>
        </div>
        <div style="color: #6a737d; font-size: 25px;">
          Spot Price : <span style="font-weight: 600; color: #374250;">{spot_str}</span>
        </div>
      </div>
      <div style="overflow-x: auto;">
      <table style="width: 100%; border-collapse: collapse; text-align: right; font-size: 20px; white-space: nowrap;">
        <thead>
          <tr style="color: #6f8da6; border-bottom: 1px solid #d7dce2; background: #ffffff;">
            <th style="padding: 10px 12px; text-align: left; font-weight: 500;">Interval</th>
            <th style="padding: 10px 12px; text-align: left; font-weight: 500;">Trading Zone</th>
            <th style="padding: 10px 12px; text-align: left; font-weight: 500;">Price Range</th>
            <th style="padding: 10px 12px; text-align: right; font-weight: 500;">OI</th>
            <th style="padding: 10px 12px; text-align: right; font-weight: 500;">OI Chg (%)</th>
            <th style="padding: 10px 12px; text-align: right; font-weight: 500;">Fresh</th>
            <th style="padding: 10px 12px; text-align: right; font-weight: 500;">Square-Off</th>
            <th style="padding: 10px 12px; text-align: right; font-weight: 500;">Volume</th>
          </tr>
        </thead>
        <tbody>
    """

    def get_zone_color(zone: str) -> str:
        if zone in ("Long Buildup", "Short Covering"):
            return "#2ea79a"
        elif zone == "Short Buildup":
            return "#d85b5b"
        elif zone == "Long Unwinding":
            return "#ef9f52"
        return "#848f9a"

    for index, row in enumerate(result["rows"]):
        zone_color = get_zone_color(row["trading_zone"])
        price_range = f"{row['price_range'][0]:.2f} - {row['price_range'][1]:.2f}"

        oi_chg = row["oi_change_pct"]
        oi_chg_color = "#1ea58d" if oi_chg > 0 else "#d85b5b" if oi_chg < 0 else "#848f9a"

        bg_color = "#ffffff" if index % 2 == 0 else "#fdfefe"

        # Omit zero values
        fresh_str = _format_indian_number(row["fresh"]) if row["fresh"] > 0 else "0"
        sq_str = _format_indian_number(row["square_off"]) if row["square_off"] > 0 else "0"

        html += f"""
          <tr style="border-bottom: 1px solid #e2e6ea; background: {bg_color};">
            <td style="padding: 11px 12px; text-align: left; color: #2f3947;">{row['interval']}</td>
            <td style="padding: 11px 12px; text-align: left; font-weight: 500; color: {zone_color};">{row['trading_zone']}</td>
            <td style="padding: 11px 12px; text-align: left; color: #2f3947;">{price_range}</td>
            <td style="padding: 11px 12px; color: #2f3947;">{_format_indian_number(row['oi'])}</td>
            <td style="padding: 11px 12px; color: {oi_chg_color};">{oi_chg:.2f}%</td>
            <td style="padding: 11px 12px; color: #2f3947;">{fresh_str}</td>
            <td style="padding: 11px 12px; color: #2f3947;">{sq_str}</td>
            <td style="padding: 11px 12px; color: #2f3947;">{_format_indian_number(row['volume'])}</td>
          </tr>
        """

    html += """
        </tbody>
      </table>
      </div>
    </div>
    """
    return html


def display_buildup_for_instruments(
    access_token: str,
    client_id: str,
    instrument_names: Any,
    exchange_segment: str = "NSE_FNO",
    max_rows: int = 9,
) -> None:
    """
    Given a list of instrument names or a comma-separated string,
    renders a nice HTML table in Jupyter for each instrument's 15-minute buildup.
    """
    from IPython.display import display, HTML
    
    if isinstance(instrument_names, str):
        names = [x.strip() for x in instrument_names.split(",") if x.strip()]
    else:
        names = instrument_names
        
    for name in names:
        try:
            # First fetch buildup to get instrument ref
            data = get_15min_buildup(
                access_token=access_token,
                client_id=client_id,
                instrument_name=name,
                exchange_segment=exchange_segment,
                max_rows=max_rows,
            )
            
            # Use underlying to grab spot price correctly
            parsed = _parse_named_option(name)
            spot_price = None
            if parsed:
                underlying = parsed["underlying"]
                if underlying in UNDERLYING_CHAIN_MAP:
                    # Let's try to grab spot from last NIFTY candles
                    spot_snap = _fetch_symbol_snapshot(
                        access_token=access_token,
                        client_id=client_id,
                        symbol_candidates=(f"{underlying} 50", underlying),
                        exchange_candidates=("IDX_I", "NSE_IDX"),
                        timezone_name="Asia/Kolkata",
                        candles_count=1,
                        request_timeout=10,
                    )
                    if spot_snap and spot_snap.get("price") is not None:
                        spot_price = spot_snap["price"]

            html_content = get_buildup_html(data, spot_price=spot_price)
            display(HTML(html_content))
            
        except Exception as e:
            display(HTML(f"<div style='color:red; padding:10px;'>Error fetching {name}: {str(e)}</div>"))


if __name__ == "__main__":
    # Replace with your own credentials and instrument.
    ACCESS_TOKEN = "YOUR_ACCESS_TOKEN"
    CLIENT_ID = "YOUR_CLIENT_ID"
    INSTRUMENT = "NIFTY 24 MAR 23300 PUT"

    data = get_15min_buildup(
        access_token=ACCESS_TOKEN,
        client_id=CLIENT_ID,
        instrument_name=INSTRUMENT,
        exchange_segment="NSE_FNO",
    )
    print_buildup_table(data)
