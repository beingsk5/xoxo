"""YuppTV India pure-HTTP fast path (no browser).

Verified flow against prod-api.yupptv.com:
  1. GET /service/api/v1/get/token?tenant_code=yupptv&box_id=<uuid>&product=
     yupptv&device_id=5&display_lang_code=ENG&device_sub_type=Firefox,5,Windows
     &timezone=Asia/Kolkata          -> response.sessionId (server-issued)
  2. headers: session-id, box-id, tenant-code: yupptv, Origin/Referer yupptv
  3. GET /page/content?path=livetv&count=200&sourceFrom=free
     -> response.data[*].section.sectionData contains channel objects
        {channelName, detailPagePath: "channels/<slug>/live", langCode, genre}
     (173 channels total across all languages)
  4. GET /page/stream?path=channels/<slug>/live&sourceFrom=free
     -> response.streams[].url (Akamai/ssai m3u8, ~6h IP-signed hdnts)

Premium channels return no streams / hasAccess=false — skipped. Every URL
still goes through probe_url + the normal Indian/language quality gates in
language.py before being kept.
"""
from __future__ import annotations

import logging
import threading
import uuid
from typing import Dict, List, Optional

import requests

log = logging.getLogger("scraper")

API = "https://prod-api.yupptv.com/service/api/v1"
SITE = "https://www.yupptv.com/"
ORIGIN = "https://www.yupptv.com"
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) "
    "Gecko/20100101 Firefox/128.0"
)

# Our run language -> Yupptv langCode. Urdu has no catalog on Yupptv.
LANG_TO_CODE: Dict[str, str] = {
    "hindi": "HIN", "tamil": "TAM", "telugu": "TEL", "malayalam": "MAL",
    "kannada": "KAN", "bengali": "BEN", "marathi": "MAR", "punjabi": "PUN",
    "gujarati": "GUJ", "odia": "ORI", "oriya": "ORI", "bhojpuri": "BHO",
    "assamese": "ASM", "english": "ENG",
}

LOGO_KEYS = ("channelLogo", "logoUrl", "logo", "image", "imageUrl", "avatar")

_tl = threading.local()


def _new_session() -> requests.Session:
    """Fresh session + auth token (stateless beyond the session cookie/headers)."""
    box_id = str(uuid.uuid4())
    s = requests.Session()
    s.headers.update({
        "User-Agent": UA,
        "Accept": "application/json",
        "Referer": SITE,
        "Origin": ORIGIN,
    })
    resp = s.get(
        f"{API}/get/token",
        params={
            "tenant_code": "yupptv",
            "box_id": box_id,
            "product": "yupptv",
            "device_id": "5",
            "display_lang_code": "ENG",
            "device_sub_type": "Firefox,5,Windows",
            "timezone": "Asia/Kolkata",
        },
        timeout=20,
    )
    resp.raise_for_status()
    session_id = ((resp.json() or {}).get("response") or {}).get("sessionId")
    if not session_id:
        raise RuntimeError("yupptv token: no sessionId in response")
    s.headers.update({
        "session-id": session_id,
        "box-id": box_id,
        "tenant-code": "yupptv",
    })
    return s


def _session() -> requests.Session:
    """Thread-local session (one token per worker thread)."""
    s = getattr(_tl, "session", None)
    if s is None:
        s = _new_session()
        _tl.session = s
    return s


def invalidate() -> None:
    """Drop the thread-local session (next call re-authenticates)."""
    _tl.session = None


def _get(path: str, params: dict) -> dict:
    """GET an API path; rebuild the session once if it went stale."""
    for attempt in (0, 1):
        try:
            resp = _session().get(f"{API}/{path}", params=params, timeout=25)
        except requests.RequestException:
            if attempt == 0:
                invalidate()
                continue
            raise
        if resp.status_code in (401, 403, 419) and attempt == 0:
            invalidate()
            continue
        resp.raise_for_status()
        return resp.json() or {}
    return {}


def _walk_channels(obj) -> List[dict]:
    """Recursively collect channel dicts (detailPagePath + channelName)."""
    out: List[dict] = []
    if isinstance(obj, dict):
        if obj.get("detailPagePath") and obj.get("channelName"):
            out.append(obj)
        for v in obj.values():
            out.extend(_walk_channels(v))
    elif isinstance(obj, list):
        for v in obj:
            out.extend(_walk_channels(v))
    return out


def fetch_catalog(lang_code: str = "") -> List[dict]:
    """Livetv catalog entries, optionally filtered to one langCode.

    Returns [{"name", "path", "lang", "genre", "logo"}] deduped by path.
    """
    data = _get("page/content", {
        "path": "livetv", "count": 200, "sourceFrom": "free",
    })
    seen = set()
    out: List[dict] = []
    for ch in _walk_channels(data):
        path = ch.get("detailPagePath", "")
        lang = (ch.get("langCode") or "").upper()
        if lang_code and lang != lang_code:
            continue
        if path in seen:
            continue
        seen.add(path)
        logo = ""
        for k in LOGO_KEYS:
            if ch.get(k):
                logo = ch[k]
                break
        out.append({
            "name": str(ch.get("channelName", "")).strip(),
            "path": path,
            "lang": lang,
            "genre": str(ch.get("genre", "") or ""),
            "logo": logo,
        })
    return out


def get_stream_urls(path: str) -> List[str]:
    """Stream URLs for one catalog entry (premium -> empty list)."""
    data = _get("page/stream", {"path": path, "sourceFrom": "free"})
    resp = data.get("response") or {}
    if (resp.get("streamStatus") or {}).get("hasAccess") is False:
        return []
    urls = []
    for item in resp.get("streams") or []:
        if isinstance(item, dict):
            u = item.get("url") or ""
            if u.startswith("http"):
                urls.append(u)
    return urls
